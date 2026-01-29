import os
import re
import json
import time
import uuid
import calendar
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests
import pdfplumber
from PIL import ImageDraw
from pypdf import PdfReader, PdfWriter

# ============================================================
# ✅ 실행 설정 (여기만 바꾸면 됨)
# ============================================================
PDF_PATH = r"0129/5-1.경력증명서(변주석).pdf"   # ✅ 원본 스캔 PDF
OUT_JSON = "변주석.json"                               # (파일명 ORC -> OCR)

USER_NO  = "goodbye3372"
AREA_DIV = "건설기술인협회"

SAVE_DEBUG_PNG = False
DEBUG_DIR = "debug_png"
DEBUG_DPI = 200

# ============================================================
# ✅ CLOVA OCR 설정
# ============================================================
# 권장: 환경변수로 설정
#   setx CLOVA_OCR_API_URL "https://...."
#   setx CLOVA_OCR_SECRET  "...."
CLOVA_OCR_API_URL = ""
CLOVA_OCR_SECRET  = ""

# 캐시(비용/시간 절약)
CACHE_OCR_JSON = f"clova_ocr_cache_{Path(PDF_PATH).stem}.json"
USE_CACHE_IF_EXISTS = False

# 큰 PDF 분할 OCR
PAGES_PER_CHUNK = 10
OCR_SLEEP_SEC = 0.3

# OCR bbox 안 텍스트 채택 방식
# - True: 단어 bbox "중심점"이 bbox 안에 들어오면 채택(추천, 칸 섞임/누락 밸런스 좋음)
# - False: 조금이라도 겹치면 채택(1번 코드식, 칸 섞임이 늘 수 있음)
USE_CENTER_POINT_PICK = True

# ============================================================
# ✅ 공용: 텍스트 정리/유틸
# ============================================================
def clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n+", "\n", s)
    return s.strip()

def clean_single_line(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ")
    s = s.replace("\n", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def is_blank(s: str) -> bool:
    if s is None:
        return True
    s = str(s).replace("\u00a0", " ")
    s = re.sub(r"\s+", "", s)
    return s == ""

def parse_amount_to_int(s):
    if s is None:
        return None
    s = str(s).replace("\u00a0", " ").strip()
    if s == "":
        return None
    s = re.sub(r"[^0-9,]", "", s)
    if s == "":
        return None
    try:
        return int(s.replace(",", ""))
    except:
        return None

# ============================================================
# ✅ bbox 유틸
# ============================================================
def bbox_from_ratios(page, x0r, x1r, y0r, y1r):
    w, h = page.width, page.height
    return (w * x0r, h * y0r, w * x1r, h * y1r)

def _clamp_bbox_to_page(page, bbox):
    x0, y0, x1, y1 = bbox
    px0, py0, px1, py1 = page.bbox  # (0,0,w,h)
    x0 = max(px0, min(x0, px1))
    x1 = max(px0, min(x1, px1))
    y0 = max(py0, min(y0, py1))
    y1 = max(py0, min(y1, py1))
    if x1 <= x0: x1 = x0 + 0.1
    if y1 <= y0: y1 = y0 + 0.1
    return (x0, y0, x1, y1)

def bbox_from_bigbox_inner_ratios(page, big_box_ratios, ix0, ix1, iy0, iy1):
    bx0r, bx1r, by0r, by1r = big_box_ratios
    x0r = bx0r + (bx1r - bx0r) * ix0
    x1r = bx0r + (bx1r - bx0r) * ix1
    y0r = by0r + (by1r - by0r) * iy0
    y1r = by0r + (by1r - by0r) * iy1
    return bbox_from_ratios(page, x0r, x1r, y0r, y1r)

# ============================================================
# ✅ 디버그 PNG 공용
# ============================================================
def _draw_rect(draw, bbox, color, width=3):
    x0, y0, x1, y1 = bbox
    for j in range(width):
        draw.rectangle([x0+j, y0+j, x1-j, y1-j], outline=color)

# ============================================================
# ✅ CLOVA OCR 호출 + PDF 분할/병합
# ============================================================
def call_clova_ocr_pdf(pdf_path: str) -> Dict[str, Any]:
    if not CLOVA_OCR_API_URL or not CLOVA_OCR_SECRET:
        raise RuntimeError(
            "CLOVA_OCR_API_URL / CLOVA_OCR_SECRET 설정이 비어있음.\n"
            "- 환경변수 CLOVA_OCR_API_URL, CLOVA_OCR_SECRET 설정하거나\n"
            "- 코드 상단에 직접 값을 넣어라."
        )

    headers = {"X-OCR-SECRET": CLOVA_OCR_SECRET}
    req = {
        "version": "V2",
        "requestId": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "images": [{"format": "pdf", "name": "career_pdf"}],
    }

    with open(pdf_path, "rb") as f:
        files = {"file": f}
        data = {"message": json.dumps(req)}
        r = requests.post(CLOVA_OCR_API_URL, headers=headers, data=data, files=files, timeout=180)

        if r.status_code >= 400:
            body = r.text if hasattr(r, "text") else "<no-body>"
            raise requests.exceptions.HTTPError(f"{r.status_code} Client Error: {body}", response=r)

        return r.json()

def split_pdf_by_pages(pdf_path: str, pages_per_chunk: int = 10) -> list[str]:
    reader = PdfReader(pdf_path)
    total = len(reader.pages)

    out_files = []
    base = Path(pdf_path)
    out_dir = base.parent / f"{base.stem}_chunks"
    out_dir.mkdir(exist_ok=True)

    for i in range(0, total, pages_per_chunk):
        writer = PdfWriter()
        for j in range(i, min(i + pages_per_chunk, total)):
            writer.add_page(reader.pages[j])

        out_path = out_dir / f"{base.stem}_p{i+1:03d}_to_{min(i+pages_per_chunk, total):03d}.pdf"
        with open(out_path, "wb") as f:
            writer.write(f)

        out_files.append(str(out_path))

    return out_files

def _merge_clova_images(chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {"images": []}
    for c in chunks:
        merged["images"].extend(c.get("images") or [])
    return merged

def load_or_run_ocr(pdf_path: str) -> Dict[str, Any]:
    cache_path = Path(CACHE_OCR_JSON)
    if USE_CACHE_IF_EXISTS and cache_path.exists():
        print(f"[OCR] load cache: {cache_path}")
        return json.loads(cache_path.read_text(encoding="utf-8"))

    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    if total_pages <= PAGES_PER_CHUNK:
        print(f"[OCR] single call: pages={total_pages}")
        clova = call_clova_ocr_pdf(pdf_path)
    else:
        chunk_paths = split_pdf_by_pages(pdf_path, pages_per_chunk=PAGES_PER_CHUNK)
        print(f"[OCR] split mode: total_pages={total_pages}, chunks={len(chunk_paths)}, per_chunk={PAGES_PER_CHUNK}")

        chunk_results: List[Dict[str, Any]] = []
        for idx, ch_path in enumerate(chunk_paths, start=1):
            print(f"[OCR] chunk {idx}/{len(chunk_paths)} -> {ch_path}")
            chunk_results.append(call_clova_ocr_pdf(ch_path))
            if OCR_SLEEP_SEC and OCR_SLEEP_SEC > 0:
                time.sleep(OCR_SLEEP_SEC)

        clova = _merge_clova_images(chunk_results)
        print(f"[OCR] merged images={len(clova.get('images') or [])} (expect={total_pages})")

    if USE_CACHE_IF_EXISTS:
        cache_path.write_text(json.dumps(clova, ensure_ascii=False, indent=2), encoding="utf-8")

    return clova

# ============================================================
# ✅ CLOVA bbox / OCR 좌표 변환
# ============================================================
def _get_page_image_wh(img_obj: Dict[str, Any], fallback_w: float, fallback_h: float) -> Tuple[float, float]:
    for key in ("convertedImageInfo", "convertedImage", "imageInfo"):
        if isinstance(img_obj.get(key), dict):
            w = img_obj[key].get("width")
            h = img_obj[key].get("height")
            if w and h:
                return float(w), float(h)

    w = img_obj.get("width")
    h = img_obj.get("height")
    if w and h:
        return float(w), float(h)

    # fallback (정확도 떨어질 수 있음)
    return float(fallback_w), float(fallback_h)

def _field_bbox_in_pdf_coords(field: Dict[str, Any], page_img_w: float, page_img_h: float,
                             pdf_w: float, pdf_h: float) -> Tuple[float, float, float, float]:
    bp = field.get("boundingPoly") or {}
    verts = bp.get("vertices") or []
    if not verts:
        return (0.0, 0.0, 0.0, 0.0)

    xs = [v.get("x", 0) for v in verts]
    ys = [v.get("y", 0) for v in verts]
    x0, x1 = min(xs), max(xs)
    y0, y1 = min(ys), max(ys)

    nx0 = x0 / page_img_w if page_img_w else 0.0
    nx1 = x1 / page_img_w if page_img_w else 0.0
    ny0 = y0 / page_img_h if page_img_h else 0.0
    ny1 = y1 / page_img_h if page_img_h else 0.0

    return (nx0 * pdf_w, ny0 * pdf_h, nx1 * pdf_w, ny1 * pdf_h)

def extract_text_in_bbox_from_clova(
    clova: Dict[str, Any],
    page_index_0: int,
    page_pdf_w: float,
    page_pdf_h: float,
    bbox_pdf: Tuple[float, float, float, float],
) -> str:
    x0, y0, x1, y1 = bbox_pdf

    images = clova.get("images") or []
    if page_index_0 < 0 or page_index_0 >= len(images):
        return ""

    img_obj = images[page_index_0]
    page_img_w, page_img_h = _get_page_image_wh(img_obj, fallback_w=page_pdf_w, fallback_h=page_pdf_h)

    picked = []
    for f in (img_obj.get("fields") or []):
        txt = f.get("inferText", "")
        if not txt:
            continue

        fx0, fy0, fx1, fy1 = _field_bbox_in_pdf_coords(f, page_img_w, page_img_h, page_pdf_w, page_pdf_h)

        # 선택 규칙
        if USE_CENTER_POINT_PICK:
            cx = (fx0 + fx1) / 2.0
            cy = (fy0 + fy1) / 2.0
            if (x0 <= cx <= x1) and (y0 <= cy <= y1):
                picked.append((fy0, fx0, txt))
        else:
            ix0 = max(x0, fx0)
            iy0 = max(y0, fy0)
            ix1 = min(x1, fx1)
            iy1 = min(y1, fy1)
            if (ix1 > ix0) and (iy1 > iy0):
                picked.append((fy0, fx0, txt))

    picked.sort(key=lambda t: (round(t[0], 1), t[1]))
    out = " ".join([t[2] for t in picked])
    return clean_text(out)

# ============================================================
# ✅ 페이지 상단 텍스트(제목 탐색용) - CLOVA 기반
# ============================================================
def normalize_ocr_key(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ").lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9a-z가-힣.]", "", s)
    return s

def get_top_text_clova(clova: Dict[str, Any], pdf, page_index_0: int, top_ratio: float = 0.20) -> str:
    images = clova.get("images") or []
    if page_index_0 < 0 or page_index_0 >= len(images):
        return ""

    page = pdf.pages[page_index_0]
    img_obj = images[page_index_0]
    page_img_w, page_img_h = _get_page_image_wh(img_obj, fallback_w=page.width, fallback_h=page.height)

    top_texts = []
    for f in (img_obj.get("fields") or []):
        txt = f.get("inferText", "")
        if not txt:
            continue
        fx0, fy0, fx1, fy1 = _field_bbox_in_pdf_coords(f, page_img_w, page_img_h, page.width, page.height)
        cy = (fy0 + fy1) / 2.0
        if cy <= page.height * top_ratio:
            top_texts.append(txt)

    return clean_single_line(" ".join(top_texts))

# ============================================================
# ✅ 공용: 페이지 제외(섹션 페이지) - CLOVA 기반
# ============================================================
EXCLUDE_TITLES = [
    "1. 기술경력",
    "2. 건설사업관리 및 감리경력",
]

def is_excluded_page_clova(clova, pdf, page_index_0: int) -> bool:
    top_txt = get_top_text_clova(clova, pdf, page_index_0, top_ratio=0.20)
    tnorm = normalize_ocr_key(top_txt)
    for title in EXCLUDE_TITLES:
        if normalize_ocr_key(title) in tnorm:
            return True
    return False

# ============================================================
# ============================================================
# 1) ✅ 등급 파서 (CLOVA 기반)
# ============================================================
# ============================================================
GRADE_BIG_BOX = (0.0, 1.0, 0.212, 0.281)  # (x0r, x1r, y0r, y1r)

GRADE_INNER_W, GRADE_INNER_H = 990.0, 98.0
def gixr(x): return x / GRADE_INNER_W
def giyr(y): return y / GRADE_INNER_H

GRADE_CELL_LAYOUT = {
    "SC_DUTY_JOB_1":   (gixr(116), gixr(260), giyr(44), giyr(68)),
    "SC_DUTY_LV_1":    (gixr(260), gixr(300), giyr(44), giyr(68)),
    "SC_DUTY_JOB_2":   (gixr(116), gixr(260), giyr(68), giyr(90)),
    "SC_DUTY_LV_2":    (gixr(260), gixr(300), giyr(68), giyr(90)),

    "SC_SPEC_JOB_1":   (gixr(300), gixr(448), giyr(44), giyr(68)),
    "SC_SPEC_LV_1":    (gixr(448), gixr(482), giyr(44), giyr(68)),
    "SC_SPEC_JOB_2":   (gixr(300), gixr(448), giyr(68), giyr(90)),
    "SC_SPEC_LV_2":    (gixr(448), gixr(482), giyr(68), giyr(90)),

    "CM_DUTY_JOB_1":   (gixr(480), gixr(627), giyr(44), giyr(68)),
    "CM_DUTY_LV_1":    (gixr(627), gixr(662), giyr(44), giyr(68)),
    "CM_DUTY_JOB_2":   (gixr(480), gixr(627), giyr(68), giyr(90)),
    "CM_DUTY_LV_2":    (gixr(627), gixr(662), giyr(68), giyr(90)),

    "CM_SPEC_JOB_1":   (gixr(662), gixr(808), giyr(44), giyr(68)),
    "CM_SPEC_LV_1":    (gixr(808), gixr(848), giyr(44), giyr(68)),
    "CM_SPEC_JOB_2":   (gixr(662), gixr(808), giyr(68), giyr(90)),
    "CM_SPEC_LV_2":    (gixr(808), gixr(848), giyr(68), giyr(90)),

    "QA_LV":           (gixr(848), gixr(990), giyr(42), giyr(90)),
}

ALLOWED_LEVELS = ("초급", "중급", "고급", "특급")

def normalize_level(raw: str):
    if is_blank(raw):
        return None
    s = clean_single_line(raw)
    for lv in ALLOWED_LEVELS:
        if lv in s:
            return lv
    return None

def apply_pair_filter(job_raw: str, lv_raw: str):
    job = clean_single_line(job_raw) if not is_blank(job_raw) else ""
    lv  = normalize_level(lv_raw)
    if is_blank(job) or (lv is None):
        return None, None
    return job, lv

def find_grade_target_pages(clova, pdf):
    return [i for i in range(1, len(pdf.pages) + 1) if not is_excluded_page_clova(clova, pdf, i - 1)]

def build_career_grade_items(clova, pdf, page_nos):
    items = []
    PAIRS = [
        ("SC_DUTY_JOB_1", "SC_DUTY_LV_1", "설계시공", "직무"),
        ("SC_DUTY_JOB_2", "SC_DUTY_LV_2", "설계시공", "직무"),

        ("SC_SPEC_JOB_1", "SC_SPEC_LV_1", "설계시공", "전문"),
        ("SC_SPEC_JOB_2", "SC_SPEC_LV_2", "설계시공", "전문"),

        ("CM_DUTY_JOB_1", "CM_DUTY_LV_1", "건설사업관리", "직무"),
        ("CM_DUTY_JOB_2", "CM_DUTY_LV_2", "건설사업관리", "직무"),

        ("CM_SPEC_JOB_1", "CM_SPEC_LV_1", "건설사업관리", "전문"),
        ("CM_SPEC_JOB_2", "CM_SPEC_LV_2", "건설사업관리", "전문"),
    ]

    for pno in page_nos:
        page = pdf.pages[pno - 1]

        cells = {}
        for key, (ix0, ix1, iy0, iy1) in GRADE_CELL_LAYOUT.items():
            bbox = bbox_from_bigbox_inner_ratios(page, GRADE_BIG_BOX, ix0, ix1, iy0, iy1)
            bbox = _clamp_bbox_to_page(page, bbox)
            txt = extract_text_in_bbox_from_clova(
                clova=clova,
                page_index_0=pno - 1,
                page_pdf_w=page.width,
                page_pdf_h=page.height,
                bbox_pdf=bbox,
            )
            cells[key] = clean_single_line(txt)

        made = 0

        for job_key, lv_key, grade_div, field_div in PAIRS:
            job, lv = apply_pair_filter(cells.get(job_key, ""), cells.get(lv_key, ""))
            if job is None:
                continue

            items.append({
                "user_no": USER_NO,
                "area_div": AREA_DIV,
                "grade_div": grade_div,
                "field_div": field_div,
                "field_name": job,
                "grade_name": lv,
                "grade_num": None,

                "_src_page": pno,
                "_src_slot": f"{job_key}/{lv_key}",
            })
            made += 1

        qa_lv = normalize_level(cells.get("QA_LV", ""))
        if qa_lv is not None:
            items.append({
                "user_no": USER_NO,
                "area_div": AREA_DIV,
                "grade_div": "품질관리",
                "field_div": None,
                "field_name": None,
                "grade_name": qa_lv,
                "grade_num": None,

                "_src_page": pno,
                "_src_slot": "QA_LV",
            })
            made += 1

        print(f"[GRADE PAGE {pno}] created_items={made}")

    return items

def save_debug_pngs_grade(pdf_path, page_nos):
    Path(DEBUG_DIR).mkdir(parents=True, exist_ok=True)
    with pdfplumber.open(pdf_path) as pdf:
        for pno in page_nos:
            page = pdf.pages[pno - 1]
            im = page.to_image(resolution=DEBUG_DPI).original.convert("RGB")
            draw = ImageDraw.Draw(im)

            Wp, Hp = page.width, page.height
            Wi, Hi = im.size
            sx = Wi / Wp
            sy = Hi / Hp

            def pdf_to_img_bbox(b):
                x0, y0, x1, y1 = _clamp_bbox_to_page(page, b)
                return (x0*sx, y0*sy, x1*sx, y1*sy)

            big_pdf_bbox = bbox_from_ratios(page, *GRADE_BIG_BOX)
            _draw_rect(draw, pdf_to_img_bbox(big_pdf_bbox), color=(255, 0, 0), width=5)
            draw.text((pdf_to_img_bbox(big_pdf_bbox)[0] + 8, pdf_to_img_bbox(big_pdf_bbox)[1] + 8),
                      f"GRADE P{pno}", fill=(255, 0, 0))

            for _, (ix0, ix1, iy0, iy1) in GRADE_CELL_LAYOUT.items():
                cell_pdf_bbox = bbox_from_bigbox_inner_ratios(page, GRADE_BIG_BOX, ix0, ix1, iy0, iy1)
                _draw_rect(draw, pdf_to_img_bbox(cell_pdf_bbox), color=(0, 80, 255), width=2)

            out_path = Path(DEBUG_DIR) / f"debug_grade_cells_page_{pno:02d}.png"
            im.save(out_path)
            print(f"[DEBUG] saved: {out_path}")

# ============================================================
# ============================================================
# 2) ✅ 경력 파서 (CLOVA 기반)
# ============================================================
# ============================================================
SECTION_TITLES = {
    "1. 기술경력": "기술경력",
    "2. 건설사업관리 및 감리경력": "건설사업관리 및 감리경력",
}

RECORD_ROWS = [
    (0.1820, 0.2850),
    (0.2850, 0.3880),
    (0.3880, 0.4930),
    (0.4930, 0.5980),
    (0.5980, 0.7020),
    (0.7020, 0.8070),
]

W, H = 840.0, 124.0
def xr(a): return a / W
def yr(a): return a / H

SECTION_CELL_LAYOUT = {
    "participation": (xr(0),   xr(110), yr(0),   yr(120)),

    "PJT_NM":      (xr(110), xr(535), yr(-2),   yr(29)),
    "DUTY_FIELD":  (xr(530), xr(630), yr(-2),   yr(29)),
    "DUTY_JOB":    (xr(630), xr(770), yr(-2),   yr(29)),

    "ORDER_NM":    (xr(110), xr(320), yr(27),  yr(55)),
    "CON_TYPE1":   (xr(320), xr(535), yr(27),  yr(55)),
    "PRO_FILED":   (xr(530), xr(630), yr(27),  yr(55)),
    "lev":         (xr(630), xr(770), yr(27),  yr(55)),

    "con_detail":  (xr(110), xr(535), yr(55),  yr(92)),
    "respon":      (xr(535), xr(630), yr(55),  yr(92)),
    "cont_amt":    (xr(630), xr(770), yr(55),  yr(92)),

    "con_method":  (xr(110), xr(320), yr(92),  yr(120)),
    "con_tech":    (xr(320), xr(535), yr(92),  yr(120)),
    "new_tech":    (xr(530), xr(630), yr(92),  yr(120)),
    "facility_div":(xr(630), xr(770), yr(92),  yr(120)),

    "memo":        (xr(770), xr(840), yr(0),   yr(120)),
}

DATE_RE = re.compile(r"\d{4}\.\d{2}\.\d{2}")
YM_RE   = re.compile(r"\d{4}\.\d{2}(?!\.\d{2})")
DAYS_RE = re.compile(r"\(\s*([\d,]+)\s*일\s*\)")

def dotdate_to_iso(s):
    if not s:
        return None
    return s.replace(".", "-")

def _ym_to_full_date(ym: str, is_start: bool):
    try:
        y_str, m_str = ym.split(".")
        y = int(y_str)
        m = int(m_str)
        if not (1 <= m <= 12):
            return None
        dd = 1 if is_start else calendar.monthrange(y, m)[1]
        return f"{y:04d}.{m:02d}.{dd:02d}"
    except Exception:
        return None

def parse_participation(text: str):
    if not text:
        return None, None, None, None, []

    t = text.replace(" ", "")
    dates = DATE_RE.findall(t)
    start = dates[0] if len(dates) >= 1 else None
    end   = dates[1] if len(dates) >= 2 else None

    if start is None and end is None:
        yms = YM_RE.findall(t)
        if len(yms) >= 1:
            start = _ym_to_full_date(yms[0], is_start=True)
        if len(yms) >= 2:
            end = _ym_to_full_date(yms[1], is_start=False)

    days = [int(x.replace(",", "")) for x in DAYS_RE.findall(text)]
    days_total = max(days) if days else None
    days_recognized = min(days) if len(days) >= 2 else (days[0] if days else None)
    return start, end, days_total, days_recognized, days

def find_pages_for_title_clova(clova, pdf, title: str):
    pages = []
    title_norm = normalize_ocr_key(title)
    for i0 in range(len(pdf.pages)):
        top_txt = get_top_text_clova(clova, pdf, i0, top_ratio=0.20)
        if title_norm in normalize_ocr_key(top_txt):
            pages.append(i0 + 1)
    return pages

def extract_section_record_clova(clova, page, page_no, record_index, y0r, y1r):
    rec = {"page": page_no, "record_index": record_index}

    for key, (cx0, cx1, cy0, cy1) in SECTION_CELL_LAYOUT.items():
        abs_y0 = y0r + (y1r - y0r) * cy0
        abs_y1 = y0r + (y1r - y0r) * cy1
        bbox = bbox_from_ratios(page, cx0, cx1, abs_y0, abs_y1)
        bbox = _clamp_bbox_to_page(page, bbox)

        txt = extract_text_in_bbox_from_clova(
            clova=clova,
            page_index_0=page_no - 1,
            page_pdf_w=page.width,
            page_pdf_h=page.height,
            bbox_pdf=bbox,
        )

        if key == "participation":
            rec[key] = txt
        else:
            rec[key] = clean_single_line(txt)

    s, e, days_total, days_recognized, _ = parse_participation(rec.get("participation", ""))
    rec["CAR_S_DATE"] = dotdate_to_iso(s)
    rec["CAR_F_DATE"] = dotdate_to_iso(e)
    rec["CAR_DAYS"]   = days_recognized
    rec["CAR_DAYS2"]  = days_total
    return rec

def extract_section_items_by_div_clova(clova, pdf):
    items_by_div = {}
    info_by_div = {}

    for title, career_div_value in SECTION_TITLES.items():
        pages = find_pages_for_title_clova(clova, pdf, title)
        records = []

        for pno in pages:
            page = pdf.pages[pno - 1]
            for ridx, (y0r, y1r) in enumerate(RECORD_ROWS, start=1):
                rec = extract_section_record_clova(clova, page, pno, ridx, y0r, y1r)

                core_empty = is_blank(rec.get("PJT_NM")) and is_blank(rec.get("ORDER_NM")) and is_blank(rec.get("con_detail"))
                if (rec.get("CAR_S_DATE") is None) and core_empty:
                    continue
                if (rec.get("CAR_S_DATE") is None) and (rec.get("CAR_F_DATE") is None):
                    continue

                rec["career_div"] = career_div_value
                records.append(rec)

        items = []
        seq = 1
        for r in records:
            item = {
                "user_no": USER_NO,
                "area_div": AREA_DIV,
                "career_div": career_div_value,
                "seq": seq,

                "car_s_date": r.get("CAR_S_DATE"),
                "car_f_date": r.get("CAR_F_DATE"),
                "car_days": r.get("CAR_DAYS"),

                "pjt_nm": r.get("PJT_NM") or None,
                "duty_field": r.get("DUTY_FIELD") or None,
                "duty_job": r.get("DUTY_JOB") or None,

                "order_nm": r.get("ORDER_NM") or None,
                "con_type1": r.get("CON_TYPE1") or None,
                "pro_field": r.get("PRO_FILED") or None,
                "lev": r.get("lev") or None,

                "con_detail": r.get("con_detail") or None,
                "respon": r.get("respon") or None,
                "con_amt": parse_amount_to_int(r.get("cont_amt")),

                "con_method": r.get("con_method") or None,
                "con_tech": r.get("con_tech") or None,
                "new_tech": r.get("new_tech") or None,
                "facility_div": r.get("facility_div") or None,

                "memo": r.get("memo") or None,
                "workplace": None,
                "work_div": None,
            }

            has_any = any([
                item["car_s_date"], item["car_f_date"],
                item["pjt_nm"], item["order_nm"], item["con_detail"],
                item["duty_field"], item["duty_job"],
            ])
            if not has_any:
                continue

            items.append(item)
            seq += 1

        items_by_div[career_div_value] = items
        info_by_div[career_div_value] = {
            "pages": pages,
            "count_records": len(records),
            "count_items": len(items),
        }

    return items_by_div, info_by_div

# ============================================================
# (B) 근무처 BIG_BOX - CLOVA 기반
# ============================================================
WORK_BIG_BOX = (0.0, 1.0, 0.695, 0.89)

WORK_INNER_W, WORK_INNER_H = 595.0, 160.0
def wixr(x): return x / WORK_INNER_W
def wiyr(y): return y / WORK_INNER_H

WORK_BIGBOX_CELL_LAYOUT = {
    "WORKPLACE_UNUSED": (wixr(0), wixr(69), wiyr(0), wiyr(160)),

    "PERIOD_01": (wixr(69),  wixr(129), wiyr(15),  wiyr(35)),
    "NAME_01":   (wixr(129), wixr(317), wiyr(15),  wiyr(35)),
    "PERIOD_02": (wixr(317), wixr(379), wiyr(15),  wiyr(35)),
    "NAME_02":   (wixr(379), wixr(570), wiyr(15),  wiyr(35)),

    "PERIOD_03": (wixr(69),  wixr(129), wiyr(34),  wiyr(55)),
    "NAME_03":   (wixr(129), wixr(317), wiyr(34),  wiyr(55)),
    "PERIOD_04": (wixr(317), wixr(379), wiyr(34),  wiyr(55)),
    "NAME_04":   (wixr(379), wixr(570), wiyr(34),  wiyr(55)),

    "PERIOD_05": (wixr(69),  wixr(129), wiyr(54),  wiyr(75)),
    "NAME_05":   (wixr(129), wixr(317), wiyr(54),  wiyr(75)),
    "PERIOD_06": (wixr(317), wixr(379), wiyr(54),  wiyr(75)),
    "NAME_06":   (wixr(379), wixr(570), wiyr(54),  wiyr(75)),

    "PERIOD_07": (wixr(69),  wixr(129), wiyr(74),  wiyr(96)),
    "NAME_07":   (wixr(129), wixr(317), wiyr(74),  wiyr(96)),
    "PERIOD_08": (wixr(317), wixr(379), wiyr(74),  wiyr(96)),
    "NAME_08":   (wixr(379), wixr(570), wiyr(74),  wiyr(96)),

    "PERIOD_09": (wixr(69),  wixr(129), wiyr(94),  wiyr(118)),
    "NAME_09":   (wixr(129), wixr(317), wiyr(94),  wiyr(118)),
    "PERIOD_10": (wixr(317), wixr(379), wiyr(94),  wiyr(118)),
    "NAME_10":   (wixr(379), wixr(570), wiyr(94),  wiyr(118)),

    "PERIOD_11": (wixr(69),  wixr(129), wiyr(118), wiyr(136)),
    "NAME_11":   (wixr(129), wixr(317), wiyr(118), wiyr(136)),
    "PERIOD_12": (wixr(317), wixr(379), wiyr(118), wiyr(136)),
    "NAME_12":   (wixr(379), wixr(570), wiyr(118), wiyr(136)),

    "PERIOD_13": (wixr(69),  wixr(129), wiyr(136), wiyr(158)),
    "NAME_13":   (wixr(129), wixr(317), wiyr(136), wiyr(158)),
    "PERIOD_14": (wixr(317), wixr(379), wiyr(136), wiyr(158)),
    "NAME_14":   (wixr(379), wixr(570), wiyr(136), wiyr(158)),
}

BIGBOX_CAREER_DIV = "근무처"
STRICT_REQUIRE_PERIOD = False  # period 없어도 name 있으면 뽑고 싶으면 False

WB_DATE_RE = re.compile(r"(\d{4})[.\-/](\d{2})[.\-/](\d{2})")
WB_YM_RE   = re.compile(r"(\d{4})[.\-/](\d{2})(?![.\-/]\d{2})")

def _last_day(y: int, m: int) -> int:
    return calendar.monthrange(y, m)[1]

def _to_iso_date(y: int, m: int, d: int) -> str:
    return f"{y:04d}-{m:02d}-{d:02d}"

def parse_work_period(period_text: str):
    if not period_text or is_blank(period_text):
        return None, None

    t = str(period_text).strip()
    t = t.replace(" ", "")
    t = t.replace("∼", "~").replace("～", "~")
    t = t.replace("–", "-").replace("—", "-")

    dates = WB_DATE_RE.findall(t)
    if len(dates) >= 2:
        y1, m1, d1 = map(int, dates[0])
        y2, m2, d2 = map(int, dates[1])
        return _to_iso_date(y1, m1, d1), _to_iso_date(y2, m2, d2)

    yms = WB_YM_RE.findall(t)
    if len(yms) >= 2:
        y1, m1 = map(int, yms[0])
        y2, m2 = map(int, yms[1])
        return _to_iso_date(y1, m1, 1), _to_iso_date(y2, m2, _last_day(y2, m2))

    if len(dates) == 1:
        y, m, d = map(int, dates[0])
        return _to_iso_date(y, m, d), None

    if len(yms) == 1:
        y, m = map(int, yms[0])
        return _to_iso_date(y, m, 1), None

    return None, None

def find_bigbox_pages(clova, pdf):
    return [i for i in range(1, len(pdf.pages) + 1) if not is_excluded_page_clova(clova, pdf, i - 1)]

def extract_bigbox_items_clova(clova, pdf, pages):
    def normalize_company_name(s: str) -> str:
        s = clean_single_line(s)
        if not s:
            return ""

        s = s.replace("：", ":")

        if "現" in s:
            s = s.replace("現", "현")

        s = s.replace(" :", " 현:")
        s = s.replace(": ", "현:")

        if s.startswith(":"):
            s = "현" + s
        return s

    items = []
    seq = 1

    for pno in pages:
        page = pdf.pages[pno - 1]

        cells = {}
        for key, (ix0, ix1, iy0, iy1) in WORK_BIGBOX_CELL_LAYOUT.items():
            if key == "WORKPLACE_UNUSED":
                continue

            bbox = bbox_from_bigbox_inner_ratios(page, WORK_BIG_BOX, ix0, ix1, iy0, iy1)
            bbox = _clamp_bbox_to_page(page, bbox)
            txt = extract_text_in_bbox_from_clova(
                clova=clova,
                page_index_0=pno - 1,
                page_pdf_w=page.width,
                page_pdf_h=page.height,
                bbox_pdf=bbox,
            )
            cells[key] = clean_single_line(txt)

        for i in range(1, 15):
            period_raw = cells.get(f"PERIOD_{i:02d}", "")
            comp_raw = cells.get(f"NAME_{i:02d}", "")
            comp = normalize_company_name(comp_raw)

            if STRICT_REQUIRE_PERIOD and is_blank(period_raw):
                continue
            if is_blank(period_raw) and is_blank(comp):
                continue

            carr_strdate, carr_comdate = parse_work_period(period_raw)

            # ✅ 기존 코드 정책 유지: 종료일 없으면 버림
            # 필요하면 아래 2줄을 주석 처리해서 "시작일만" 있는 것도 남길 수 있음
            if carr_comdate is None:
                continue

            if is_blank(comp) and (carr_strdate is None) and (carr_comdate is None):
                continue

            item = {
                "user_no": USER_NO,
                "area_div": AREA_DIV,
                "career_div": BIGBOX_CAREER_DIV,
                "seq": seq,

                "carr_strdate": carr_strdate,
                "carr_comdate": carr_comdate,
                "carr_comp": comp,

                "_src_page": pno,
                "_src_row": i,
                "_period_raw": period_raw,
                "_carr_comp_raw": comp_raw,
            }
            items.append(item)
            seq += 1

    return items

# ============================================================
# ✅ 디버그 PNG (섹션/근무처)
# ============================================================
def save_debug_pngs_section(pdf_path, pages, prefix):
    Path(DEBUG_DIR).mkdir(parents=True, exist_ok=True)
    with pdfplumber.open(pdf_path) as pdf:
        for pno in pages:
            page = pdf.pages[pno - 1]
            im = page.to_image(resolution=DEBUG_DPI).original.convert("RGB")
            draw = ImageDraw.Draw(im)

            Wp, Hp = page.width, page.height
            Wi, Hi = im.size
            sx = Wi / Wp
            sy = Hi / Hp

            def pdf_to_img_bbox(b):
                x0, y0, x1, y1 = _clamp_bbox_to_page(page, b)
                return (x0*sx, y0*sy, x1*sx, y1*sy)

            for ridx, (y0r, y1r) in enumerate(RECORD_ROWS, start=1):
                rec_pdf_bbox = bbox_from_ratios(page, 0.0, 1.0, y0r, y1r)
                _draw_rect(draw, pdf_to_img_bbox(rec_pdf_bbox), color=(255, 0, 0), width=4)
                draw.text((pdf_to_img_bbox(rec_pdf_bbox)[0] + 6, pdf_to_img_bbox(rec_pdf_bbox)[1] + 6),
                          f"R{ridx}", fill=(255, 0, 0))

                for _, (cx0, cx1, cy0, cy1) in SECTION_CELL_LAYOUT.items():
                    abs_y0 = y0r + (y1r - y0r) * cy0
                    abs_y1 = y0r + (y1r - y0r) * cy1
                    cell_pdf_bbox = bbox_from_ratios(page, cx0, cx1, abs_y0, abs_y1)
                    _draw_rect(draw, pdf_to_img_bbox(cell_pdf_bbox), color=(0, 80, 255), width=2)

            out_path = Path(DEBUG_DIR) / f"{prefix}_section_page_{pno:02d}.png"
            im.save(out_path)
            print(f"[DEBUG] saved: {out_path}")

def save_debug_pngs_bigbox(pdf_path, pages):
    Path(DEBUG_DIR).mkdir(parents=True, exist_ok=True)
    with pdfplumber.open(pdf_path) as pdf:
        for pno in pages:
            page = pdf.pages[pno - 1]
            im = page.to_image(resolution=DEBUG_DPI).original.convert("RGB")
            draw = ImageDraw.Draw(im)

            Wp, Hp = page.width, page.height
            Wi, Hi = im.size
            sx = Wi / Wp
            sy = Hi / Hp

            def pdf_to_img_bbox(b):
                x0, y0, x1, y1 = _clamp_bbox_to_page(page, b)
                return (x0*sx, y0*sy, x1*sx, y1*sy)

            big_pdf_bbox = bbox_from_ratios(page, *WORK_BIG_BOX)
            _draw_rect(draw, pdf_to_img_bbox(big_pdf_bbox), color=(255, 0, 0), width=5)
            draw.text((pdf_to_img_bbox(big_pdf_bbox)[0] + 8, pdf_to_img_bbox(big_pdf_bbox)[1] + 8),
                      f"WORK P{pno}", fill=(255, 0, 0))

            for key, (ix0, ix1, iy0, iy1) in WORK_BIGBOX_CELL_LAYOUT.items():
                if key == "WORKPLACE_UNUSED":
                    continue
                cell_pdf_bbox = bbox_from_bigbox_inner_ratios(page, WORK_BIG_BOX, ix0, ix1, iy0, iy1)
                _draw_rect(draw, pdf_to_img_bbox(cell_pdf_bbox), color=(0, 80, 255), width=2)

            out_path = Path(DEBUG_DIR) / f"debug_work_bigbox_cells_page_{pno:02d}.png"
            im.save(out_path)
            print(f"[DEBUG] saved: {out_path}")

# ============================================================
# ✅ main (CLOVA로 통일)
# ============================================================
def main():
    pdf_path = Path(PDF_PATH)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

    # 0) CLOVA OCR 먼저
    clova = load_or_run_ocr(PDF_PATH)

    with pdfplumber.open(PDF_PATH) as pdf:
        # 1) 등급
        grade_pages = find_grade_target_pages(clova, pdf)
        grade_items = build_career_grade_items(clova, pdf, grade_pages)

        # 2) 근무처
        bigbox_pages = find_bigbox_pages(clova, pdf)
        bigbox_items = extract_bigbox_items_clova(clova, pdf, bigbox_pages)

        # 3) 섹션형(기술경력/감리)
        items_by_div, info_by_div = extract_section_items_by_div_clova(clova, pdf)

    # ✅ 최종 순서: 등급 → 근무처 → 기술경력 → CM
    items_all = []
    items_all.extend(grade_items)
    items_all.extend(bigbox_items)
    items_all.extend(items_by_div.get("기술경력", []))
    items_all.extend(items_by_div.get("건설사업관리 및 감리경력", []))

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(items_all, f, ensure_ascii=False, indent=2)

    print(f"[OK] saved: {OUT_JSON}")
    print(f" - 등급: {len(grade_items)}")
    print(f" - 근무처: {len(bigbox_items)}")
    print(f" - 기술경력: {len(items_by_div.get('기술경력', []))}")
    print(f" - 건설사업관리 및 감리경력: {len(items_by_div.get('건설사업관리 및 감리경력', []))}")
    print(f" - total: {len(items_all)}")

    if SAVE_DEBUG_PNG:
        # 등급 디버그
        if grade_pages:
            save_debug_pngs_grade(PDF_PATH, grade_pages)

        # 섹션 디버그 (CLOVA로 페이지 탐색)
        with pdfplumber.open(PDF_PATH) as pdf:
            for title, career_div_value in SECTION_TITLES.items():
                pages = find_pages_for_title_clova(clova, pdf, title)
                if pages:
                    prefix = career_div_value.replace(" ", "_")
                    save_debug_pngs_section(PDF_PATH, pages, prefix)

        # 근무처 디버그
        if bigbox_pages:
            save_debug_pngs_bigbox(PDF_PATH, bigbox_pages)

if __name__ == "__main__":
    main()

