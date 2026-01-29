import json
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple, Optional

import requests

# ============================================================
# ✅ 실행 설정 (여기만 바꾸면 됨)
# ============================================================
JSON_PATH = r"현준식.json"

BASE_URL   = "http://172.20.60.71:8080/api"
HR_USER_NO = "hjs"
HR_API_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlbXAiLCJ1c2VyX2lkIjoxNCwicm9sZSI6IkVNUExPWUVFIiwiZXhwIjoxNzY5NDk5MDU5fQ.SPAoi7XtkccQr0EgyqburucBSo0ntc8tM5AQonqaKKI"

DRY_RUN = False
LIMIT = None
TIMEOUT_SEC = 15

STRICT_USER_MATCH = True
VERIFY_AFTER_UPLOAD = True

# ============================================================
# ✅ 엔드포인트
# ============================================================
URL_COMPANY = f"{BASE_URL}/career-company/"
URL_PJT     = f"{BASE_URL}/career-pjt/"
URL_GRADE   = f"{BASE_URL}/career-grade/"

URL_PJT_LIST     = f"{BASE_URL}/career-pjt/"
URL_COMPANY_LIST = f"{BASE_URL}/career-company/"
URL_GRADE_LIST   = f"{BASE_URL}/career-grade/"

# ============================================================
# ✅ 공용: 헤더
# ============================================================
def build_headers() -> Dict[str, str]:
    token = HR_API_TOKEN.strip()
    auth = token if token.lower().startswith("bearer ") else f"Bearer {token}"
    return {
        "Authorization": auth,
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Origin": BASE_URL.replace("/api", ""),
    }

# ============================================================
# ✅ 아이템 타입 판별
# ============================================================
def is_grade_item(item: dict) -> bool:
    return any(k in item for k in ("grade_div", "grade_name", "field_name", "field_div"))

def is_company_item(item: dict) -> bool:
    return any(k in item for k in ("carr_strdate", "carr_comdate", "carr_comp"))

def is_pjt_item(item: dict) -> bool:
    # ✅ 너희 '주요기술경력' 매핑 스키마 기준
    keys = {
        "car_s_date", "car_f_date", "car_days",
        "pjt_nm", "order_nm", "con_detail",
        "con_type1", "respon", "duty_job",
        "duty_field", "fire_div", "fire_office",
        "career_div",
    }
    return any(k in item for k in keys)

# ============================================================
# ✅ 라우팅 + payload 정규화 (user_no 강제 덮기)
#   - ✅ 요구사항: 필터링 X, career_div 강제 X
# ============================================================
def route_and_build_payload(it: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]], str]:
    # grade
    if is_grade_item(it) and (not is_company_item(it)) and (not is_pjt_item(it)):
        payload = {
            "user_no": HR_USER_NO,
            "area_div": it.get("area_div"),
            "grade_div": it.get("grade_div"),
            "field_div": it.get("field_div"),
            "field_name": it.get("field_name"),
            "grade_name": it.get("grade_name"),
            "grade_num": it.get("grade_num"),
        }
        return URL_GRADE, payload, "grade"

    # company
    if is_company_item(it):
        payload = {
            "user_no": HR_USER_NO,
            "area_div": it.get("area_div"),
            "carr_strdate": it.get("carr_strdate"),
            "carr_comdate": it.get("carr_comdate"),
            "carr_comp": it.get("carr_comp"),
        }
        return URL_COMPANY, payload, "company"

    # pjt (주요기술경력)
    if is_pjt_item(it):
        payload = {
            "user_no": HR_USER_NO,
            "area_div": it.get("area_div"),
            "career_div": it.get("career_div"),  # ✅ null이면 null 그대로 감

            "car_s_date": it.get("car_s_date"),
            "car_f_date": it.get("car_f_date"),
            "car_days": it.get("car_days"),

            "pjt_nm": it.get("pjt_nm"),
            "order_nm": it.get("order_nm"),
            "con_detail": it.get("con_detail"),

            "con_type1": it.get("con_type1"),     # 주요용도
            "respon": it.get("respon"),           # 직위
            "duty_job": it.get("duty_job"),       # 담당업무
            "duty_field": it.get("duty_field"),   # 업무분야
            "fire_div": it.get("fire_div"),       # 구분
            "fire_office": it.get("fire_office"), # 처리관서
        }
        return URL_PJT, payload, "pjt"

    return None, None, "unknown"

# ============================================================
# ✅ GET helper
# ============================================================
def safe_get(url, headers, params=None):
    try:
        r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SEC)
        ct = (r.headers.get("Content-Type") or "").lower()
        if "application/json" in ct:
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, r.text
        return r.status_code, r.text
    except Exception as e:
        return None, str(e)

# ============================================================
# ✅ POST with retry + idempotency
# ============================================================
def post_with_retry(url, payload, headers, max_retries=6):
    idem_key = str(uuid.uuid4())
    h = dict(headers)
    h["Idempotency-Key"] = idem_key

    backoff = 0.7
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, headers=h, json=payload, timeout=TIMEOUT_SEC)
            status = r.status_code

            if 200 <= status < 300:
                try:
                    return True, status, r.json()
                except Exception:
                    return True, status, r.text

            if status == 429 or (500 <= status < 600):
                wait = backoff * (2 ** (attempt - 1))
                print(f"[RETRY] {status} attempt={attempt}/{max_retries} wait={wait:.2f}s url={url}")
                time.sleep(wait)
                continue

            try:
                return False, status, r.json()
            except Exception:
                return False, status, r.text

        except (requests.Timeout, requests.ConnectionError) as e:
            wait = backoff * (2 ** (attempt - 1))
            print(f"[RETRY] network error attempt={attempt}/{max_retries} wait={wait:.2f}s err={e}")
            time.sleep(wait)

    return False, None, None

# ============================================================
# ✅ (2) 서버 응답 user_no 검증
# ============================================================
def enforce_user_match_or_die(kind: str, payload: dict, resp):
    if not STRICT_USER_MATCH:
        return

    sent_user_no = payload.get("user_no")
    if isinstance(resp, dict):
        saved_user_no = resp.get("user_no")
        if saved_user_no is not None and saved_user_no != sent_user_no:
            raise RuntimeError(
                f"[FATAL] USER_NO MISMATCH kind={kind} sent={sent_user_no} resp={saved_user_no}\n"
                f"-> 서버가 payload user_no를 무시하고 토큰 기준으로 저장할 가능성이 큼. 즉시 중단"
            )

# ============================================================
# ✅ (3) 업로드 후 GET 검증
# ============================================================
def verify_lists(headers, uploaded_items):
    print("==========[VERIFY AFTER UPLOAD]==========")

    code, body = safe_get(URL_GRADE_LIST, headers, params={"user_no": HR_USER_NO})
    cnt = len(body) if isinstance(body, list) else None
    print(f"[VERIFY] grade list user_no={HR_USER_NO} -> {code} count={cnt}")

    code, body = safe_get(URL_COMPANY_LIST, headers, params={"user_no": HR_USER_NO, "limit": 1000})
    cnt = len(body) if isinstance(body, list) else None
    print(f"[VERIFY] company list user_no={HR_USER_NO} -> {code} count={cnt}")

    # 업로드 JSON에서 (area_div, career_div) 조합 추출 (career_div가 None이면 제외)
    combos = []
    seen = set()
    for it in uploaded_items:
        if is_pjt_item(it) and not is_company_item(it):
            area_div = it.get("area_div")
            career_div = it.get("career_div")
            if area_div and career_div:
                key = (area_div, career_div)
                if key not in seen:
                    seen.add(key)
                    combos.append(key)

    if not combos:
        print("[VERIFY] pjt list: no (area_div, career_div) combo found (career_div is null?)")
    else:
        for area_div, career_div in combos:
            code, body = safe_get(
                URL_PJT_LIST,
                headers,
                params={
                    "user_no": HR_USER_NO,
                    "area_div": area_div,
                    "career_div": career_div,
                    "limit": 1000,
                },
            )
            cnt = len(body) if isinstance(body, list) else None
            print(f"[VERIFY] pjt list user_no={HR_USER_NO} area_div={area_div} career_div={career_div} -> {code} count={cnt}")

    print("========================================")

# ============================================================
# ✅ main
# ============================================================
def main():
    p = Path(JSON_PATH)
    if not p.exists():
        raise FileNotFoundError(f"JSON not found: {JSON_PATH}")

    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("JSON root must be a list (items array).")

    if LIMIT is not None:
        data = data[: int(LIMIT)]

    headers = build_headers()

    failures = []
    cnt_grade = cnt_company = cnt_pjt = cnt_unknown = 0

    for idx, raw in enumerate(data, start=1):
        url, payload, kind = route_and_build_payload(raw)

        if kind == "unknown" or url is None or payload is None:
            cnt_unknown += 1
            failures.append({
                "_reason": "unknown_item_shape",
                "_index": idx,
                "raw": raw,
            })
            print(f"[SKIP] idx={idx} unknown item shape")
            continue

        if kind == "grade":   cnt_grade += 1
        if kind == "company": cnt_company += 1
        if kind == "pjt":     cnt_pjt += 1

        if DRY_RUN:
            print(f"[DRY_RUN] idx={idx} kind={kind} url={url}")
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            continue

        ok, status, resp = post_with_retry(url, payload, headers=headers)

        if not ok:
            failures.append({
                "_reason": "post_failed",
                "_index": idx,
                "_kind": kind,
                "_status": status,
                "_resp": resp,
                "payload": payload,
                "raw": raw,
            })
            print(f"[FAIL] idx={idx} kind={kind} status={status}")
            continue

        enforce_user_match_or_die(kind, payload, resp)

        resp_user = resp.get("user_no") if isinstance(resp, dict) else None
        resp_id   = resp.get("id") if isinstance(resp, dict) else None
        resp_seq  = resp.get("seq") if isinstance(resp, dict) else None

        print(f"[OK] idx={idx} kind={kind} status={status} sent_user={payload.get('user_no')} resp_user={resp_user} resp_id={resp_id} resp_seq={resp_seq}")

    if failures:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = Path(f"upload_failures_{ts}.json")
        out.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[WARN] failures saved: {out} (count={len(failures)})")

    print("====================================================")
    print(f"[DONE] total={len(data)} grade={cnt_grade} company={cnt_company} pjt={cnt_pjt} unknown={cnt_unknown}")
    print("====================================================")

    if (not DRY_RUN) and VERIFY_AFTER_UPLOAD:
        verify_lists(headers, uploaded_items=data)

if __name__ == "__main__":
    main()
