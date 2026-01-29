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
PDF_PATH = r"C:/file/정보통신공사협회/최문철(통신).pdf"

# ✅ 최종 매핑 결과(items) 저장
OUT_JSON = "최문철.json"

USER_NO  = "mcchoi12"
AREA_DIV = "통신"

# ✅ "경력사항" 페이지 필터 키워드
KEYWORD = "경력사항"
TOP_RATIO = 0.30

# ============================================================
# ✅ CLOVA OCR 설정
# ============================================================
CLOVA_OCR_API_URL = os.environ.get(
    "CLOVA_OCR_API_URL",
    ""
)
CLOVA_OCR_SECRET = os.environ.get(
    "CLOVA_OCR_SECRET",
    "

CACHE_OCR_JSON = f"clova_ocr_cache_{Path(PDF_PATH).stem}.json"
USE_CACHE_IF_EXISTS = False

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
    if not CLOVA_OCR_API_URL or not CLOVA_OCR_SECRET:
        raise RuntimeError("CLOVA_OCR_API_URL / CLOVA_OCR_SECRET 설정이 비어있음.")

    headers = {"X-OCR-SECRET": CLOVA_OCR_SECRET}
    req = {
        "version": "V2",
        "requestId": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "enableTableDetection": True,
        "images": [{"format": "pdf", "name": "career"}],
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

    try:
        Path(CACHE_OCR_JSON).write_text(json.dumps(clova, ensure_ascii=False), encoding="utf-8")
        print(f"[OCR] saved cache: {CACHE_OCR_JSON}")
    except Exception as e:
        print(f"[WARN] cache save failed: {e}")

    return clova

# ============================================================
# ✅ 1) "경력사항" 페이지 찾기 (상단 TOP_RATIO만)
# ============================================================
def _get_page_image_h(img_obj: Dict[str, Any], fallback_h: float) -> float:
    info = img_obj.get("convertedImageInfo") or {}
    h = info.get("height")
    if h:
        return float(h)
    h2 = img_obj.get("height")
    return float(h2) if h2 else float(fallback_h)

def find_pages_top_by_keyword(clova: Dict[str, Any], pdf, keyword: str, top_ratio: float) -> List[int]:
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
# ✅ 2) tables/cells normalize
# ============================================================
def _join_cell_text(cell: Dict[str, Any]) -> str:
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

# ============================================================
# ✅ 3) "경력사항" 테이블 필터/파싱 (한 행 = 한 건)
# ============================================================
def find_header_row_and_cols(cells: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[Dict[str, int]]]:
    """
    헤더 행을 찾아서 컬럼 인덱스 매핑을 반환.
    기대 헤더(좌->우): 기간, 근무처명, 직위(또는 직위또는직급), 담당업무, 참여사업명, 발주자

    ✅ 중요: OCR에서 "기 간"처럼 떨어져 나올 수 있어서
    - row 전체 merged를 remove_all_spaces 기준으로 판정
    - 개별 셀도 remove_all_spaces 기준으로 컬럼 매핑
    """
    all_rows = sorted({c["rowIndex"] for c in cells})

    header_tokens = [
        "기간", "근무처", "근무처명", "직위", "직급", "직위또는직급",
        "담당업무", "참여사업", "참여사업명", "발주자"
    ]

    for r in all_rows:
        row_cells = [(c["columnIndex"], clean_single_line(c.get("text", ""))) for c in cells if c["rowIndex"] == r]
        if not row_cells:
            continue

        merged = " ".join([t for _, t in sorted(row_cells, key=lambda x: x[0])])
        merged_ns = remove_all_spaces(merged)  # ✅ "기 간" -> "기간"

        # '기간' 포함 + 다른 헤더 토큰 2개 이상 포함이면 헤더로 간주
        hit = sum(1 for tok in header_tokens if tok in merged_ns)
        if ("기간" in merged_ns) and (hit >= 2):
            cols: Dict[str, int] = {}

            for col, txt in row_cells:
                tns = remove_all_spaces(txt)

                if not tns:
                    continue

                # ✅ 컬럼명 매핑도 tns 기준
                if tns == "기간":
                    cols["기간"] = col
                elif "근무처" in tns:
                    cols["근무처명"] = col
                elif ("직위" in tns) or ("직급" in tns):
                    cols["직위"] = col
                elif "담당업무" in tns:
                    cols["담당업무"] = col
                elif "참여사업" in tns:
                    cols["참여사업명"] = col
                elif "발주자" in tns:
                    cols["발주자"] = col

            # 최소 조건: 기간 + 근무처명
            if ("기간" in cols) and ("근무처명" in cols):
                return r, cols

    return None, None

def is_empty_table_career(cells: List[Dict[str, Any]]) -> bool:
    # 텍스트가 거의 없으면 제외
    texts = [remove_all_spaces(c.get("text", "")) for c in cells]
    texts = [t for t in texts if t]
    if len(texts) < 8:
        return True

    header_row, cols = find_header_row_and_cols(cells)
    if header_row is None or not cols:
        return True

    # 헤더 아래 실제 데이터 row가 1개도 없으면 empty
    data_rows = sorted({c["rowIndex"] for c in cells if c["rowIndex"] > header_row})
    if not data_rows:
        return True

    # ✅ 데이터 row 중 "기간" 컬럼이 비어있지 않은 row가 1개도 없으면 empty
    cell_map = build_cell_map(cells)
    period_col = cols.get("기간")
    if period_col is None:
        return True

    def cell_txt(r: int, col: int) -> str:
        c = cell_map.get((r, col))
        return clean_single_line(c["text"]) if c else ""

    has_any_period = False
    for r in data_rows:
        period = remove_all_spaces(cell_txt(r, period_col))
        if period:
            has_any_period = True
            break

    return not has_any_period

def parse_career_table_to_items(
    cells: List[Dict[str, Any]],
    user_no: str,
    area_div: str,
) -> List[Dict[str, Any]]:
    """
    ✅ 경력사항(단순형) 매핑:
    기간 -> car_s_date, car_f_date, car_days
    근무처명 -> work_nm
    직위 -> respon
    담당업무 -> duty_job
    참여사업명 -> pjt_nm
    발주자 -> order_nm

    ✅ 규칙 변경:
    - 연번 없음
    - 기간 셀이 비어있으면 "레코드 없음"으로 보고 skip
    - career_div는 "아무것도 안 채움" -> item에 아예 넣지 않음
    """
    cell_map = build_cell_map(cells)

    header_row, cols = find_header_row_and_cols(cells)
    if header_row is None or not cols:
        return []

    def cell_txt(r: int, col: int) -> str:
        c = cell_map.get((r, col))
        return clean_single_line(c["text"]) if c else ""

    all_rows = sorted({c["rowIndex"] for c in cells})
    data_rows = [r for r in all_rows if r > header_row]

    def v(s: str) -> Optional[str]:
        s2 = clean_single_line(s)
        s2 = re.sub(r"\s+", "", s2)   # ✅ 모든 공백 제거
        return s2 if s2 else None


    items: List[Dict[str, Any]] = []

    for r in data_rows:
        period   = cell_txt(r, cols.get("기간", -999))
        period_ns = remove_all_spaces(period)

        # ✅ 기간이 비어있으면 레코드 자체가 없다고 보고 skip (너 요구사항)
        if period_ns == "":
            continue

        work_nm  = cell_txt(r, cols.get("근무처명", -999))
        respon   = cell_txt(r, cols.get("직위", -999))
        duty_job = cell_txt(r, cols.get("담당업무", -999))
        pjt_nm   = cell_txt(r, cols.get("참여사업명", -999))
        order_nm = cell_txt(r, cols.get("발주자", -999))

        car_s_date, car_f_date, car_days = parse_participation_period(period)

        item = {
            "user_no": user_no,
            "area_div": area_div,

            "car_s_date": car_s_date,
            "car_f_date": car_f_date,
            "car_days": car_days,

            "work_nm": v(work_nm),
            "respon": v(respon),
            "duty_job": v(duty_job),
            "pjt_nm": v(pjt_nm),
            "order_nm": v(order_nm),
        }

        # 기간은 있었는데 날짜 파싱이 안 됐다 -> 업로드 품질 위해 버림
        if item["car_s_date"] is None:
            continue

        has_any = any([
            item["car_s_date"], item["car_f_date"], item["car_days"],
            item["work_nm"], item["respon"], item["duty_job"], item["pjt_nm"], item["order_nm"]
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

    clova = load_or_run_ocr(PDF_PATH)

    # ✅ "경력사항" 페이지 찾기
    with pdfplumber.open(PDF_PATH) as pdf:
        target_pages = find_pages_top_by_keyword(
            clova=clova,
            pdf=pdf,
            keyword=KEYWORD,
            top_ratio=TOP_RATIO
        )

    print(f"[TARGET] keyword='{KEYWORD}' pages={target_pages}")

    if not target_pages:
        print("[WARN] No target pages found.")
        Path(OUT_JSON).write_text("[]", encoding="utf-8")
        return

    images = clova.get("images") or []
    all_items: List[Dict[str, Any]] = []

    for pno in target_pages:
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

            if is_empty_table_career(cells):
                print(f"  - table[{ti}] skip: not career/empty table")
                continue

            items = parse_career_table_to_items(
                cells=cells,
                user_no=USER_NO,
                area_div=AREA_DIV,
            )

            print(f"  - table[{ti}] mapped items={len(items)}")
            all_items.extend(items)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_items, f, ensure_ascii=False, indent=2)

    print(f"[OK] saved: {OUT_JSON}  items={len(all_items)}")

if __name__ == "__main__":
    main()

