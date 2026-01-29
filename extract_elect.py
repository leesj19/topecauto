import re
import json
import calendar
from pathlib import Path

import pdfplumber
from PIL import ImageDraw

# ============================================================
# ✅ 실행 설정 (여기만 바꾸면 됨)
# ============================================================
PDF_PATH = r"C:/file/전기기술인협회/강대용(전기).pdf"
OUT_JSON = "강대용.json"

USER_NO  = "kdy"
AREA_DIV = "전기"  # ✅ 웹 payload 기준
CAREER_DIV_VALUE = "전력기술근무경력(총괄)"  # ✅ 웹 payload 기준

SAVE_DEBUG_PNG = True
DEBUG_DIR = "debug_png"
DEBUG_DPI = 200

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
    """
    ✅ 모든 공백 제거(스페이스/탭/개행 등)
    """
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
# ✅ 공용: bbox 유틸 + strict 추출 (clamp 포함)
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

def extract_text_in_bbox_strict(page, bbox):
    """
    ✅ bbox 밖 글자 섞임 방지(완전 포함만) + crop 에러 방지를 위한 clamp
    """
    x0, y0, x1, y1 = _clamp_bbox_to_page(page, bbox)

    words = page.extract_words(
        keep_blank_chars=False,
        use_text_flow=False
    ) or []

    picked = []
    for w in words:
        wx0, wx1 = w["x0"], w["x1"]
        wy0, wy1 = w["top"], w["bottom"]
        if (wx0 >= x0 and wx1 <= x1 and wy0 >= y0 and wy1 <= y1):
            picked.append(w)

    picked.sort(key=lambda d: (round(d["top"], 1), d["x0"]))
    txt = " ".join([p["text"] for p in picked])
    return clean_text(txt)

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

def find_power_career_range(pdf, keyword="전력기술근무경력", top_ratio=0.30):
    """
    시작: 페이지 상단(top_ratio)에서 keyword가 최초 등장하는 페이지
    끝  : PDF 마지막 페이지
    """
    key_norm = normalize_ocr_key(keyword)

    start = None
    for i, p in enumerate(pdf.pages, start=1):
        top = p.crop((0, 0, p.width, p.height * top_ratio))
        raw = top.extract_text() or ""
        if key_norm in normalize_ocr_key(raw):
            start = i
            break

    if start is None:
        return [], None, None

    end = len(pdf.pages)
    return list(range(start, end + 1)), start, end

# ============================================================
# ✅ 레코드 행(너가 조정한 값)
# ============================================================
RECORD_ROWS = [
    (0.2100, 0.2800),
    (0.2800, 0.3400),
    (0.3400, 0.4050),
    (0.4050, 0.4650),
    (0.4650, 0.5320),
    (0.5320, 0.5960),
    (0.5960, 0.6620),
    (0.6620, 0.7280),
    (0.7280, 0.7900),
    (0.7900, 0.8540),
]

# ============================================================
# ✅ 내부 좌표계(너가 조정한 레이아웃)
# ============================================================
W, H = 840.0, 124.0
def xr(a): return a / W
def yr(a): return a / H

SECTION_CELL_LAYOUT = {
    "participation": (xr(150), xr(240), yr(0),   yr(120)),
    "WORKPLACE":     (xr(55),  xr(150), yr(0),   yr(120)),

    "PJT_NM":        (xr(240), xr(495), yr(0),   yr(60)),
    "ORDER_NM":      (xr(240), xr(495), yr(60),  yr(120)),

    "DUTY_FIELD":    (xr(495), xr(580), yr(0),   yr(60)),
    "CON_TYPE1":     (xr(495), xr(580), yr(60),  yr(120)),

    "respon":        (xr(580), xr(665), yr(0),   yr(60)),
    "DUTY_JOB":      (xr(580), xr(665), yr(60),  yr(120)),

    "WORK_DIV":      (xr(665), xr(750), yr(0),   yr(60)),
    "lev":           (xr(665), xr(750), yr(60),  yr(120)),

    "memo":          (xr(750), xr(800), yr(0),   yr(120)),
}

# ============================================================
# ✅ participation 파싱
#   - 날짜: 2024.01.02 / 2024-01-02 / 2024/01/02
#   - car_days: (일수/일수) 중 "앞 일수"
# ============================================================
DATE_RE = re.compile(r"(\d{4})[.\-/](\d{2})[.\-/](\d{2})")
YM_RE   = re.compile(r"(\d{4})[.\-/](\d{2})(?![.\-/]\d{2})")

# (123/456) 또는 (123 / 456) 또는 (123일/456일) 등 대응
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

    # 1) 날짜 2개(우선)
    t_nospace = t.replace(" ", "")
    dates = DATE_RE.findall(t_nospace)
    start = None
    end = None
    if len(dates) >= 1:
        y, m, d = map(int, dates[0])
        start = _to_dotdate(y, m, d)
    if len(dates) >= 2:
        y, m, d = map(int, dates[1])
        end = _to_dotdate(y, m, d)

    # 2) fallback: YYYY.MM 2개면 월의 첫날/말일로 보정
    if start is None and end is None:
        yms = YM_RE.findall(t_nospace)
        if len(yms) >= 1:
            y, m = map(int, yms[0])
            start = _to_dotdate(y, m, 1)
        if len(yms) >= 2:
            y, m = map(int, yms[1])
            end = _to_dotdate(y, m, calendar.monthrange(y, m)[1])

    # 3) (일수/일수)에서 앞 일수만
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
# ✅ 레코드 추출
# ============================================================
def extract_section_record(page, page_no, record_index, y0r, y1r):
    rec = {"page": page_no, "record_index": record_index}

    for key, (cx0, cx1, cy0, cy1) in SECTION_CELL_LAYOUT.items():
        abs_y0 = y0r + (y1r - y0r) * cy0
        abs_y1 = y0r + (y1r - y0r) * cy1
        bbox = bbox_from_ratios(page, cx0, cx1, abs_y0, abs_y1)

        txt = extract_text_in_bbox_strict(page, bbox)
        if key == "participation":
            rec[key] = txt
        else:
            # ✅ 모든 텍스트 필드: 공백 제거 버전으로 저장
            rec[key] = remove_all_spaces(clean_single_line(txt))

    s_dot, e_dot, car_days = parse_participation(rec.get("participation", ""))
    rec["CAR_S_DATE"] = dotdate_to_iso(s_dot)
    rec["CAR_F_DATE"] = dotdate_to_iso(e_dot)
    rec["CAR_DAYS"]   = car_days
    return rec

# ============================================================
# ✅ 아이템 생성: 웹 payload 스키마로 맞춤
#   - 모든 내용 공백 제거
# ============================================================
def extract_power_career_items(pdf, keyword="전력기술근무경력", top_ratio=0.30):
    pages, start_p, end_p = find_power_career_range(pdf, keyword=keyword, top_ratio=top_ratio)

    records = []
    for pno in pages:
        page = pdf.pages[pno - 1]
        for ridx, (y0r, y1r) in enumerate(RECORD_ROWS, start=1):
            rec = extract_section_record(page, pno, ridx, y0r, y1r)

            core_empty = (
                is_blank(rec.get("WORKPLACE")) and
                is_blank(rec.get("PJT_NM")) and
                is_blank(rec.get("ORDER_NM")) and
                is_blank(rec.get("DUTY_FIELD")) and
                is_blank(rec.get("DUTY_JOB"))
            )
            if (rec.get("CAR_S_DATE") is None) and core_empty:
                continue
            if (rec.get("CAR_S_DATE") is None) and (rec.get("CAR_F_DATE") is None):
                continue

            records.append(rec)

    items = []
    for r in records:
        # ✅ payload에 들어갈 값: 전부 공백 제거
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
# ✅ 디버그 PNG (섹션형만)
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

    with pdfplumber.open(PDF_PATH) as pdf:
        items, info = extract_power_career_items(
            pdf,
            keyword="전력기술근무경력",
            top_ratio=0.30
        )

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    print(f"[OK] saved: {OUT_JSON}")
    print(f" - {CAREER_DIV_VALUE}: {len(items)}")
    print(f" - range: start={info.get('start')} end={info.get('end')} pages={len(info.get('pages', []))}")

    if SAVE_DEBUG_PNG:
        pages = info.get("pages", [])
        if pages:
            save_debug_pngs_section(PDF_PATH, pages, prefix=CAREER_DIV_VALUE)
        else:
            print("[WARN] keyword start page not found; debug png skipped.")

if __name__ == "__main__":
    main()
