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
from pypdf import PdfReader, PdfWriter

# ============================================================
# ✅ 실행 설정 (여기만 바꾸면 됨)
# ============================================================
PDF_PATH = r"C:/file/소방기술인협회/현준식(소방).pdf"

# ✅ 최종 매핑 결과(items) 저장
OUT_JSON = "현준식.json"

USER_NO  = "hjs"
AREA_DIV = "소방"
CAREER_DIV_VALUE = "주요기술경력"   # 웹 payload에 맞게 필요시 변경

# "주요기술경력" 페이지 필터 키워드
KEYWORD = "주요기술경력"
TOP_RATIO = 0.30   # 페이지 상단 30%에서만 키워드 탐색

# ============================================================
# ✅ CLOVA OCR 설정
# ============================================================
CLOVA_OCR_API_URL = os.environ.get(
    "CLOVA_OCR_API_URL",
    ""
)
CLOVA_OCR_SECRET = os.environ.get(
    "CLOVA_OCR_SECRET",
    ""
)

# OCR 결과 캐시
CACHE_OCR_JSON = f"clova_ocr_cache_{Path(PDF_PATH).stem}.json"
USE_CACHE_IF_EXISTS = False

# 큰 PDF 분할 OCR
PAGES_PER_CHUNK = 10
OCR_SLEEP_SEC = 0.3

# ============================================================
# ✅ 공용 유틸
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
    if not s:
        return ""
    s = str(s).replace("\u00a0", " ")
    return re.sub(r"\s+", "", s).strip()

def normalize_ocr_key(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ").lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9a-z가-힣]", "", s)
    return s

# ============================================================
# ✅ 참여기간 파싱: (시작일, 종료일, 총일수)
# ============================================================
DATE_RE = re.compile(r"(\d{4})[.\-/](\d{2})[.\-/](\d{2})")
YM_RE   = re.compile(r"(\d{4})[.\-/](\d{2})(?![.\-/]\d{2})")
DAYS_RE = re.compile(r"\(\s*([\d,]+)\s*일?\s*\)")

def _to_dotdate(y: int, m: int, d: int) -> str:
    return f"{y:04d}.{m:02d}.{d:02d}"

def dotdate_to_iso(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return s.replace(".", "-")

def parse_participation_period(text: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    if not text:
        return None, None, None

    t = clean_single_line(str(text))
    t_nospace = re.sub(r"\s+", "", t)

    dates = DATE_RE.findall(t_nospace)
    start_dot = end_dot = None
    if len(dates) >= 1:
        y, m, d = map(int, dates[0])
        start_dot = _to_dotdate(y, m, d)
    if len(dates) >= 2:
        y, m, d = map(int, dates[1])
        end_dot = _to_dotdate(y, m, d)

    if start_dot is None and end_dot is None:
        yms = YM_RE.findall(t_nospace)
        if len(yms) >= 1:
            y, m = map(int, yms[0])
            start_dot = _to_dotdate(y, m, 1)
        if len(yms) >= 2:
            y, m = map(int, yms[1])
            end_dot = _to_dotdate(y, m, calendar.monthrange(y, m)[1])

    car_days = None
    m = DAYS_RE.search(t)
    if m:
        try:
            car_days = int(m.group(1).replace(",", ""))
        except:
            car_days = None

    return dotdate_to_iso(start_dot), dotdate_to_iso(end_dot), car_days

# ============================================================
# ✅ CLOVA OCR 호출
# ============================================================
def call_clova_ocr_pdf(pdf_path: str) -> Dict[str, Any]:
    """
    PDF를 CLOVA OCR로 보내서 JSON 결과 반환
    ✅ tables를 받기 위해 enableTableDetection True
    """
    if not CLOVA_OCR_API_URL or not CLOVA_OCR_SECRET:
        raise RuntimeError("CLOVA_OCR_API_URL / CLOVA_OCR_SECRET 설정이 비어있음.")

    headers = {"X-OCR-SECRET": CLOVA_OCR_SECRET}
    req = {
        "version": "V2",
        "requestId": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),

        # ✅ 표 추출
        "enableTableDetection": True,

        "images": [{"format": "pdf", "name": "major_career"}],
    }

    with open(pdf_path, "rb") as f:
        files = {"file": f}
        data = {"message": json.dumps(req)}
        r = requests.post(CLOVA_OCR_API_URL, headers=headers, data=data, files=files, timeout=180)

    if r.status_code >= 400:
        body = r.text if hasattr(r, "text") else "<no-body>"
        raise requests.exceptions.HTTPError(f"{r.status_code} Client Error: {body}", response=r)

    return r.json()

# ============================================================
# ✅ PDF 분할 + images 병합
# ============================================================
def split_pdf_by_pages(pdf_path: str, pages_per_chunk: int = 10) -> List[str]:
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
    if USE_CACHE_IF_EXISTS and Path(CACHE_OCR_JSON).exists():
        print(f"[OCR] load cache: {CACHE_OCR_JSON}")
        return json.loads(Path(CACHE_OCR_JSON).read_text(encoding="utf-8"))

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

    # cache save
    try:
        Path(CACHE_OCR_JSON).write_text(json.dumps(clova, ensure_ascii=False), encoding="utf-8")
        print(f"[OCR] saved cache: {CACHE_OCR_JSON}")
    except Exception as e:
        print(f"[WARN] cache save failed: {e}")

    return clova

# ============================================================
# ✅ 1) "주요기술경력" 페이지 찾기 (상단 TOP_RATIO만)
# ============================================================
def _get_page_image_h(img_obj: Dict[str, Any], fallback_h: float) -> float:
    info = img_obj.get("convertedImageInfo") or {}
    h = info.get("height")
    if h:
        return float(h)
    h2 = img_obj.get("height")
    return float(h2) if h2 else float(fallback_h)

def find_major_pages_top(clova: Dict[str, Any], pdf, keyword: str, top_ratio: float) -> List[int]:
    key_norm = normalize_ocr_key(keyword)
    pages: List[int] = []
    images = clova.get("images") or []

    for i0, page in enumerate(pdf.pages):
        if i0 >= len(images):
            break
        img = images[i0]
        img_h = _get_page_image_h(img, fallback_h=page.height)

        top_texts: List[str] = []
        for f in (img.get("fields") or []):
            txt = f.get("inferText", "")
            if not txt:
                continue
            bp = f.get("boundingPoly") or {}
            verts = bp.get("vertices") or []
            if not verts:
                continue
            cy = sum([v.get("y", 0) for v in verts]) / max(len(verts), 1)
            if cy <= img_h * top_ratio:
                top_texts.append(txt)

        merged = " ".join(top_texts)
        if key_norm in normalize_ocr_key(merged):
            pages.append(i0 + 1)

    return pages

# ============================================================
# ✅ 2) tables/cells -> 매핑(items)
# ============================================================
def _join_cell_text(cell: Dict[str, Any]) -> str:
    # 원본 CLOVA 응답은 cellTextLines/cellWords 구조일 수 있어서 둘 다 지원
    if "text" in cell and isinstance(cell["text"], str):
        return clean_text(cell["text"])

    lines = cell.get("cellTextLines") or []
    words: List[str] = []
    for ln in lines:
        for w in (ln.get("cellWords") or []):
            t = w.get("inferText")
            if t:
                words.append(t)
    return clean_text(" ".join(words))

def normalize_cells_for_mapping(raw_cells: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    tables.cells가 CLOVA 원본 형태든, 우리가 저장한 major_tables_raw 형태든 둘 다 처리
    """
    norm: List[Dict[str, Any]] = []
    for c in raw_cells:
        norm.append({
            "rowIndex": int(c.get("rowIndex", 0)),
            "columnIndex": int(c.get("columnIndex", 0)),
            "rowSpan": int(c.get("rowSpan", 1) or 1),
            "columnSpan": int(c.get("columnSpan", 1) or 1),
            "text": _join_cell_text(c),
        })
    return norm

def build_cell_map(cells: List[Dict[str, Any]]) -> Dict[Tuple[int, int], Dict[str, Any]]:
    m: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for c in cells:
        m[(c["rowIndex"], c["columnIndex"])] = c
    return m

def is_empty_table(cells: List[Dict[str, Any]]) -> bool:
    """
    ✅ 비어있는 표 필터:
    - 텍스트가 너무 적거나
    - 연번(col=0)에 숫자 연번이 하나도 없으면 빈 표
    """
    texts = [remove_all_spaces(c.get("text", "")) for c in cells]
    texts = [t for t in texts if t]
    if len(texts) < 10:
        return True

    for c in cells:
        if c["columnIndex"] != 0:
            continue
        t = remove_all_spaces(c.get("text", ""))
        if t.isdigit() and c["rowIndex"] >= 2:
            return False

    return True

def parse_major_table_to_items(
    cells: List[Dict[str, Any]],
    user_no: str,
    area_div: str,
    career_div: str,
) -> List[Dict[str, Any]]:
    """
    ✅ 너가 지정한 매핑:
    참여기간 -> car_s_date, car_f_date, car_days(총일수)
    사업명 -> pjt_nm
    발주자 -> order_nm
    대상물규모 -> con_detail
    주요용도 -> con_type1
    직위 -> respon
    담당업무 -> duty_job
    업무분야 -> duty_field
    구분 -> fire_div
    (연번은 업로드 X)
    """
    cell_map = build_cell_map(cells)

    # 연번 숫자 셀(col=0, rowIndex>=2)
    serials = []
    for c in cells:
        if c["columnIndex"] != 0:
            continue
        t = remove_all_spaces(c.get("text", ""))
        if t.isdigit() and c["rowIndex"] >= 2:
            serials.append(c)

    serials.sort(key=lambda x: x["rowIndex"])

    items: List[Dict[str, Any]] = []

    for sc in serials:
        r0 = sc["rowIndex"]
        span = sc.get("rowSpan", 1)
        r_last = r0 + span - 1

        def cell_txt(r: int, col: int) -> str:
            c = cell_map.get((r, col))
            return clean_single_line(c["text"]) if c else ""

        # ✅ 참여기간: rowSpan 범위(r0 ~ r_last)의 col=1 텍스트를 전부 합쳐서 파싱
        participation = " ".join(
            [cell_txt(r, 1) for r in range(r0, r_last + 1)]
        )

        car_s_date, car_f_date, car_days = parse_participation_period(participation)


        # col=2는 (사업명/발주자/대상물규모) 3줄로 들어오는 타입을 우선 지원
        pjt_nm     = cell_txt(r0, 2)
        order_nm   = cell_txt(r0 + 1, 2) if r0 + 1 <= r_last else ""
        con_detail = cell_txt(r0 + 2, 2) if r0 + 2 <= r_last else ""

        con_type1  = cell_txt(r0, 3)  # 주요용도
        respon     = cell_txt(r0, 4)  # 직위
        duty_job   = cell_txt(r0, 5)  # 담당업무
        duty_field = cell_txt(r0, 6)  # 업무분야
        fire_div   = cell_txt(r0, 7)  # 구분

        def v(s: str) -> Optional[str]:
            s = remove_all_spaces(clean_single_line(s))
            return s if s else None


        item = {
            "user_no": user_no,
            "area_div": area_div,
            "career_div": career_div,

            "car_s_date": car_s_date,
            "car_f_date": car_f_date,
            "car_days": car_days,

            "pjt_nm": v(pjt_nm),
            "order_nm": v(order_nm),
            "con_detail": v(con_detail),

            "con_type1": v(con_type1),
            "respon": v(respon),
            "duty_job": v(duty_job),
            "duty_field": v(duty_field),
            "fire_div": v(fire_div),
        }

        # 시작일 없는 건 버림(너 규칙)
        if item["car_s_date"] is None:
            continue

        # 완전 빈 항목 방지
        has_any = any([
            item["car_s_date"], item["car_f_date"], item["car_days"],
            item["pjt_nm"], item["order_nm"], item["con_detail"],
            item["con_type1"], item["respon"], item["duty_job"],
            item["duty_field"], item["fire_div"]
        ])
        if not has_any:
            continue

        items.append(item)

    return items

# ============================================================
# ✅ main
# ============================================================
def main():
    pdf_path = Path(PDF_PATH)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

    # 1) OCR 실행
    clova = load_or_run_ocr(PDF_PATH)

    # 2) 주요기술경력 페이지 찾기 (상단 기준)
    with pdfplumber.open(PDF_PATH) as pdf:
        major_pages = find_major_pages_top(
            clova=clova,
            pdf=pdf,
            keyword=KEYWORD,
            top_ratio=TOP_RATIO
        )

    print(f"[MAJOR] keyword='{KEYWORD}' pages={major_pages}")

    if not major_pages:
        print("[WARN] No major pages found.")
        Path(OUT_JSON).write_text("[]", encoding="utf-8")
        return

    # 3) pages -> tables -> 빈표 필터 -> 매핑
    images = clova.get("images") or []
    all_items: List[Dict[str, Any]] = []

    for pno in major_pages:
        if pno - 1 >= len(images):
            continue
        img = images[pno - 1]
        tables = img.get("tables") or []
        print(f"[PAGE] {pno} tables={len(tables)}")

        for ti, t in enumerate(tables):
            raw_cells = t.get("cells") or []
            if not raw_cells:
                print(f"  - table[{ti}] skip: no cells")
                continue

            cells = normalize_cells_for_mapping(raw_cells)

            if is_empty_table(cells):
                print(f"  - table[{ti}] skip: empty table")
                continue

            items = parse_major_table_to_items(
                cells=cells,
                user_no=USER_NO,
                area_div=AREA_DIV,
                career_div=CAREER_DIV_VALUE,
            )

            print(f"  - table[{ti}] mapped items={len(items)}")
            all_items.extend(items)

    # 4) 저장
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_items, f, ensure_ascii=False, indent=2)

    print(f"[OK] saved: {OUT_JSON}  items={len(all_items)}")


if __name__ == "__main__":
    main()

