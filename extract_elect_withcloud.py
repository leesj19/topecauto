import os
import re
import json
import time
import uuid
import calendar
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from pypdf import PdfReader, PdfWriter
import requests
import pdfplumber
from PIL import ImageDraw

# ============================================================
# ✅ 실행 설정 (여기만 바꾸면 됨)
# ============================================================
PDF_PATH = r"C:/file/전기기술인협회/이채용(전기).pdf"
OUT_JSON = "이채용.json"

USER_NO  = "chaeyong.lee"
AREA_DIV = "전기"                       # ✅ 웹 payload 기준
CAREER_DIV_VALUE = "전력기술근무경력(총괄)"  # ✅ 웹 payload 기준

SAVE_DEBUG_PNG = True
DEBUG_DIR = "debug_png"
DEBUG_DPI = 200

# ============================================================
# ✅ CLOVA OCR 설정
# ============================================================
# 1) 환경변수에서 읽기(권장) 또는 아래에 직접 문자열 넣기
CLOVA_OCR_API_URL = "l"
CLOVA_OCR_SECRET  = ""

# OCR 결과 캐시(같은 PDF 반복 테스트 시 비용/시간 절약)
CACHE_OCR_JSON = f"clova_ocr_cache_{Path(PDF_PATH).stem}.json"
USE_CACHE_IF_EXISTS = False

# ✅ (추가) 큰 PDF 분할 OCR 설정
PAGES_PER_CHUNK = 10          # 10페이지 넘으면 쪼개서 OCR
OCR_SLEEP_SEC = 0.3           # 너무 빠르게 연속 호출하면 막히는 경우 방지(필요 없으면 0으로)

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

def remove_all_spaces(s: str) -> str:
    """✅ 모든 공백 제거(스페이스/탭/개행 등)"""
    if not s:
        return ""
    s = str(s).replace("\u00a0", " ")
    return re.sub(r"\s+", "", s).strip()

def is_blank(s: str) -> bool:
    if s is None:
        return True
    s = str(s).replace("\u00a0", " ")
    s = re.sub(r"\s+", "", s)
    return s == ""

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

# ============================================================
# ✅ CLOVA OCR 호출 + 결과 파싱
# ============================================================
def call_clova_ocr_pdf(pdf_path: str) -> Dict[str, Any]:
    """
    PDF를 CLOVA OCR로 보내서 JSON 결과 반환
    """
    if not CLOVA_OCR_API_URL or not CLOVA_OCR_SECRET:
        raise RuntimeError(
            "CLOVA_OCR_API_URL / CLOVA_OCR_SECRET 설정이 비어있음.\n"
            "- 환경변수 CLOVA_OCR_API_URL, CLOVA_OCR_SECRET을 설정하거나\n"
            "- 코드 상단에 직접 값을 넣어라."
        )

    headers = {"X-OCR-SECRET": CLOVA_OCR_SECRET}

    req = {
        "version": "V2",
        "requestId": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "images": [{"format": "pdf", "name": "power_career"}],
    }

    with open(pdf_path, "rb") as f:
        files = {"file": f}
        data = {"message": json.dumps(req)}
        r = requests.post(CLOVA_OCR_API_URL, headers=headers, data=data, files=files, timeout=180)

        # ✅ 400일 때 원인 로그가 response text에 들어오는 경우가 많아서 같이 뿌려줌
        if r.status_code >= 400:
            try:
                body = r.text
            except Exception:
                body = "<no-body>"
            raise requests.exceptions.HTTPError(
                f"{r.status_code} Client Error: {body}",
                response=r
            )

        return r.json()

# ============================================================
# ✅ PDF 분할
# ============================================================
def split_pdf_by_pages(pdf_path: str, pages_per_chunk: int = 10) -> list[str]:
    """
    pdf를 pages_per_chunk 페이지씩 쪼개서
    파일 경로 리스트 반환
    """
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

# ============================================================
# ✅ (핵심 수정) OCR 로더: PDF가 길면 분할해서 OCR 후 images 병합
# ============================================================
def _merge_clova_images(chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    chunk별 clova 응답의 images를 순서대로 합쳐서
    원본 PDF 페이지 순서와 동일하게 만든다.
    """
    merged: Dict[str, Any] = {"images": []}
    for c in chunks:
        merged["images"].extend(c.get("images") or [])
    return merged

def load_or_run_ocr(pdf_path: str) -> Dict[str, Any]:
    """
    ✅ 기존: 원본 PDF를 그대로 OCR 호출
    ✅ 수정: 10페이지 초과면 분할 -> chunk별 OCR -> images 병합
    """
    # 캐시 로직은 사용 안 한다고 했으니 그대로 무시(원하면 여기 다시 살리면 됨)

    # 원본 PDF 페이지 수 확인
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    # 10페이지 이하면 기존처럼 한번에
    if total_pages <= PAGES_PER_CHUNK:
        print(f"[OCR] single call: pages={total_pages}")
        return call_clova_ocr_pdf(pdf_path)

    # 10페이지 초과면 분할
    chunk_paths = split_pdf_by_pages(pdf_path, pages_per_chunk=PAGES_PER_CHUNK)
    print(f"[OCR] split mode: total_pages={total_pages}, chunks={len(chunk_paths)}, per_chunk={PAGES_PER_CHUNK}")

    chunk_results: List[Dict[str, Any]] = []
    for idx, ch_path in enumerate(chunk_paths, start=1):
        print(f"[OCR] chunk {idx}/{len(chunk_paths)} -> {ch_path}")
        chunk_results.append(call_clova_ocr_pdf(ch_path))
        if OCR_SLEEP_SEC and OCR_SLEEP_SEC > 0:
            time.sleep(OCR_SLEEP_SEC)

    merged = _merge_clova_images(chunk_results)
    print(f"[OCR] merged images={len(merged.get('images') or [])} (expect={total_pages})")
    return merged

# ============================================================
# ✅ bbox / OCR 좌표 변환
# ============================================================
def _get_page_image_wh(img_obj: Dict[str, Any], fallback_w: float, fallback_h: float) -> Tuple[float, float]:
    """
    CLOVA 결과에서 페이지 이미지 width/height를 최대한 찾아온다.
    없으면 fallback(PDF page w/h)로 대체(정확도 떨어질 수 있음)
    """
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

    return float(fallback_w), float(fallback_h)

def _field_bbox_in_pdf_coords(field: Dict[str, Any], page_img_w: float, page_img_h: float,
                             pdf_w: float, pdf_h: float) -> Tuple[float, float, float, float]:
    """
    field.boundingPoly(vertices)가 '이미지 픽셀 좌표'인 것을
    -> (0~1) 비율로 정규화
    -> PDF 페이지 좌표로 변환
    """
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
        return ""   # ✅ CLOVA에 이 페이지 없음 → 빈 문자열

    img_obj = images[page_index_0]

    page_img_w, page_img_h = _get_page_image_wh(
        img_obj, fallback_w=page_pdf_w, fallback_h=page_pdf_h
    )

    picked = []
    for f in (img_obj.get("fields") or []):
        txt = f.get("inferText", "")
        if not txt:
            continue

        fx0, fy0, fx1, fy1 = _field_bbox_in_pdf_coords(
            f, page_img_w, page_img_h, page_pdf_w, page_pdf_h
        )

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
# ✅ 전력기술근무경력: 시작~마지막 페이지 범위 찾기 (OCR 잡음 필터)
# ============================================================
def normalize_ocr_key(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ").lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9a-z가-힣]", "", s)
    return s

def find_power_career_range_clova(clova: Dict[str, Any], pdf, keyword="전력기술근무경력", top_ratio=0.30):
    """
    시작: 페이지 상단(top_ratio)에 keyword 최초 등장
    끝  : PDF 마지막 페이지
    """
    key_norm = normalize_ocr_key(keyword)
    start = None

    imgs = clova.get("images") or []
    for i0, page in enumerate(pdf.pages):
        if i0 >= len(imgs):
            break

        img_obj = imgs[i0]
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

        merged = " ".join(top_texts)
        if key_norm in normalize_ocr_key(merged):
            start = i0 + 1  # 1-based
            break

    if start is None:
        return [], None, None

    end = min(len(pdf.pages), len(imgs))
    return list(range(start, end + 1)), start, end

# ============================================================
# ✅ 레코드 행(너가 조정한 값)
# ============================================================
RECORD_ROWS = [
    (0.2230, 0.2890),
    (0.2900, 0.3550),
    (0.3560, 0.4210),
    (0.4220, 0.4830),
    (0.4835, 0.5490),
    (0.5485, 0.6150),
    (0.6140, 0.6790),
    (0.6780, 0.7410),
    (0.7410, 0.8060),
    (0.8045, 0.8690),
]

# ============================================================
# ✅ 내부 좌표계(너가 조정한 레이아웃)
# ============================================================
W, H = 840.0, 124.0
def xr(a): return a / W
def yr(a): return a / H

SECTION_CELL_LAYOUT = {
    "participation": (xr(150), xr(220), yr(15),   yr(105)),
    "WORKPLACE":     (xr(55),  xr(130), yr(15),   yr(100)),

    "PJT_NM":        (xr(250), xr(495), yr(15),   yr(55)),
    "ORDER_NM":      (xr(250), xr(495), yr(75),  yr(120)),

    "DUTY_FIELD":    (xr(510), xr(560), yr(15),   yr(55)),
    "CON_TYPE1":     (xr(510), xr(560), yr(75),  yr(120)),

    "respon":        (xr(600), xr(650), yr(15),   yr(55)),
    "DUTY_JOB":      (xr(600), xr(650), yr(75),  yr(120)),

    "WORK_DIV":      (xr(665), xr(750), yr(15),   yr(55)),
    "lev":           (xr(665), xr(750), yr(75),  yr(120)),

    "memo":          (xr(750), xr(800), yr(15),   yr(120)),
}

# ============================================================
# ✅ participation 파싱
# ============================================================
DATE_RE = re.compile(r"(\d{4})[.\-/](\d{2})[.\-/](\d{2})")
YM_RE   = re.compile(r"(\d{4})[.\-/](\d{2})(?![.\-/]\d{2})")
PAIR_DAYS_RE = re.compile(r"\(\s*([\d,]+)\s*(?:일)?\s*[/|]\s*([\d,]+)\s*(?:일)?\s*\)")

def _to_dotdate(y: int, m: int, d: int) -> str:
    return f"{y:04d}.{m:02d}.{d:02d}"

def dotdate_to_iso(s: str):
    if not s:
        return None
    return s.replace(".", "-")

def parse_participation(text: str):
    """
    return: start_dot, end_dot, car_days(first in (a/b))
    """
    if not text:
        return None, None, None

    t = str(text)
    t_nospace = re.sub(r"\s+", "", t)

    dates = DATE_RE.findall(t_nospace)
    start = end = None
    if len(dates) >= 1:
        y, m, d = map(int, dates[0])
        start = _to_dotdate(y, m, d)
    if len(dates) >= 2:
        y, m, d = map(int, dates[1])
        end = _to_dotdate(y, m, d)

    if start is None and end is None:
        yms = YM_RE.findall(t_nospace)
        if len(yms) >= 1:
            y, m = map(int, yms[0])
            start = _to_dotdate(y, m, 1)
        if len(yms) >= 2:
            y, m = map(int, yms[1])
            end = _to_dotdate(y, m, calendar.monthrange(y, m)[1])

    car_days = None
    m = PAIR_DAYS_RE.search(t)
    if m:
        a = m.group(1).replace(",", "")
        try:
            car_days = int(a)
        except:
            car_days = None

    return start, end, car_days

# ============================================================
# ✅ 레코드 추출 (CLOVA 기반)
# ============================================================
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
            rec[key] = remove_all_spaces(clean_single_line(txt))

    s_dot, e_dot, car_days = parse_participation(rec.get("participation", ""))
    rec["CAR_S_DATE"] = dotdate_to_iso(s_dot)
    rec["CAR_F_DATE"] = dotdate_to_iso(e_dot)
    rec["CAR_DAYS"]   = car_days
    return rec

# ============================================================
# ✅ 아이템 생성: 웹 payload 스키마로 맞춤
# ============================================================
def extract_power_career_items_clova(pdf, clova, keyword="전력기술근무경력", top_ratio=0.30):
    pages, start_p, end_p = find_power_career_range_clova(clova, pdf, keyword=keyword, top_ratio=top_ratio)

    records = []
    for pno in pages:
        page = pdf.pages[pno - 1]
        for ridx, (y0r, y1r) in enumerate(RECORD_ROWS, start=1):
            rec = extract_section_record_clova(clova, page, pno, ridx, y0r, y1r)

            # ✅ 시작일 없는 행은 그냥 버림 (너가 원한 버리는 기준)
            if rec.get("CAR_S_DATE") is None:
                continue

            records.append(rec)

    items = []
    for r in records:
        def v(key):
            return remove_all_spaces(r.get(key, "")) or None

        item = {
            "user_no": USER_NO,
            "area_div": AREA_DIV,
            "career_div": CAREER_DIV_VALUE,

            "car_s_date": r.get("CAR_S_DATE"),
            "car_f_date": r.get("CAR_F_DATE"),
            "car_days": r.get("CAR_DAYS"),

            "pjt_nm": v("PJT_NM"),
            "duty_field": v("DUTY_FIELD"),
            "duty_job": v("DUTY_JOB"),

            "order_nm": v("ORDER_NM"),
            "con_type1": v("CON_TYPE1"),
            "respon": v("respon"),
            "lev": v("lev"),
            "memo": v("memo"),

            "workplace": v("WORKPLACE"),
            "work_div": v("WORK_DIV"),
        }

        has_any = any([
            item["car_s_date"], item["car_f_date"], item["car_days"],
            item["workplace"], item["work_div"],
            item["pjt_nm"], item["order_nm"],
            item["duty_field"], item["duty_job"],
            item["con_type1"], item["respon"], item["lev"], item["memo"],
        ])
        if not has_any:
            continue

        items.append(item)

    info = {
        "pages": pages,
        "start": start_p,
        "end": end_p,
        "count_records": len(records),
        "count_items": len(items),
    }
    return items, info

# ============================================================
# ✅ 디버그 PNG (기존 유지: bbox 시각화용)
# ============================================================
def _draw_rect(draw, bbox, color, width=3):
    x0, y0, x1, y1 = bbox
    for j in range(width):
        draw.rectangle([x0+j, y0+j, x1-j, y1-j], outline=color)

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
                draw.text(
                    (pdf_to_img_bbox(rec_pdf_bbox)[0] + 6, pdf_to_img_bbox(rec_pdf_bbox)[1] + 6),
                    f"R{ridx}",
                    fill=(255, 0, 0)
                )

                for _, (cx0, cx1, cy0, cy1) in SECTION_CELL_LAYOUT.items():
                    abs_y0 = y0r + (y1r - y0r) * cy0
                    abs_y1 = y0r + (y1r - y0r) * cy1
                    cell_pdf_bbox = bbox_from_ratios(page, cx0, cx1, abs_y0, abs_y1)
                    _draw_rect(draw, pdf_to_img_bbox(cell_pdf_bbox), color=(0, 80, 255), width=2)

            out_path = Path(DEBUG_DIR) / f"{prefix}_section_page_{pno:02d}.png"
            im.save(out_path)
            print(f"[DEBUG] saved: {out_path}")

# ============================================================
# ✅ main
# ============================================================
def main():
    pdf_path = Path(PDF_PATH)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

    # 1) ✅ OCR 실행 (10페이지 초과면 자동 분할 OCR + 병합)
    clova = load_or_run_ocr(PDF_PATH)

    with pdfplumber.open(PDF_PATH) as pdf:
        # 2) CLOVA 기반 추출
        items, info = extract_power_career_items_clova(
            pdf=pdf,
            clova=clova,
            keyword="전력기술근무경력",
            top_ratio=0.30
        )

    # 3) JSON 저장
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    print(f"[OK] saved: {OUT_JSON}")
    print(f" - {CAREER_DIV_VALUE}: {len(items)}")
    print(f" - range: start={info.get('start')} end={info.get('end')} pages={len(info.get('pages', []))}")

    # 4) 디버그 bbox PNG(선택)
    if SAVE_DEBUG_PNG:
        pages = info.get("pages", [])
        if pages:
            save_debug_pngs_section(PDF_PATH, pages, prefix=CAREER_DIV_VALUE)
        else:
            print("[WARN] keyword start page not found; debug png skipped.")

if __name__ == "__main__":
    main()

