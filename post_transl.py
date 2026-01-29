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
JSON_PATH = r"최문철.json"

BASE_URL   = "http://172.20.60.71:8080/api"
HR_USER_NO = "mcchoi12"
HR_API_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlbXBsb3llZSIsInVzZXJfaWQiOjMsInJvbGUiOiJFTVBMT1lFRSIsImV4cCI6MTc2OTUwNTg5NX0.Bea97m5-8u_ss127fT2WM_Kib2SO62CYnfktfLKG44o"

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
    """
    ✅ '주요기술경력' / '경력사항' 둘 다 pjt 엔드포인트로 들어갈 수 있으니
    두 스키마 키들을 모두 허용해서 pjt로 라우팅
    """
    major_keys = {
        "car_s_date", "car_f_date", "car_days",
        "pjt_nm", "order_nm", "con_detail",
        "con_type1", "respon", "duty_job",
        "duty_field", "fire_div", "fire_office",
        "career_div",
    }
    career_simple_keys = {
        "car_s_date", "car_f_date", "car_days",
        "work_nm", "workplace",   # 근무처명
        "respon", "lev",          # 직위
        "duty_job",
        "pjt_nm",
        "order_nm",
        "career_div",
    }
    return any(k in item for k in (major_keys | career_simple_keys))

# ============================================================
# ✅ payload cleanup
#   - None 값은 제거(서버가 null 싫어할 수 있음)
#   - 단, career_div는 "빈 문자열"로 보내고 싶으면 예외처리
# ============================================================
def cleanup_payload(payload: Dict[str, Any], keep_empty_keys: Optional[set] = None) -> Dict[str, Any]:
    keep_empty_keys = keep_empty_keys or set()
    out = {}
    for k, v in payload.items():
        if k in keep_empty_keys:
            # 빈 문자열 유지 허용
            out[k] = v
            continue
        if v is None:
            continue
        out[k] = v
    return out

# ============================================================
# ✅ 라우팅 + payload 정규화 (user_no 강제 덮기)
#   - ✅ 요구사항: career_div 강제 채우지 않음(=빈 문자열로 전송)
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
        payload = cleanup_payload(payload)
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
        payload = cleanup_payload(payload)
        return URL_COMPANY, payload, "company"

    # pjt (주요기술경력 / 경력사항 공용)
    if is_pjt_item(it):
        # ✅ 경력사항(단순형) 호환: work_nm/respon -> workplace/lev로도 전송
        workplace = it.get("workplace") or it.get("work_nm")
        lev = it.get("lev") or it.get("respon")

        # ✅ career_div: "아무것도 안채움" => 빈 문자열로 보냄
        career_div_value = ""

        payload = {
            "user_no": HR_USER_NO,
            "area_div": it.get("area_div"),
            "career_div": career_div_value,

            "car_s_date": it.get("car_s_date"),
            "car_f_date": it.get("car_f_date"),
            "car_days": it.get("car_days"),

            # ✅ 경력사항 payload 키(스크린샷 기준)
            "workplace": workplace,
            "lev": lev,
            "duty_job": it.get("duty_job"),
            "pjt_nm": it.get("pjt_nm"),
            "order_nm": it.get("order_nm"),

            # ✅ 주요기술경력 payload 키(있으면 같이 보냄: 서버가 허용하면 저장 / 아니면 무시)
            "con_detail": it.get("con_detail"),
            "con_type1": it.get("con_type1"),
            "respon": it.get("respon"),
            "duty_field": it.get("duty_field"),
            "fire_div": it.get("fire_div"),
            "fire_office": it.get("fire_office"),
        }

        # career_div는 빈 문자열 유지해야 하니까 keep_empty_keys에 넣음
        payload = cleanup_payload(payload, keep_empty_keys={"career_div"})
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
#   - career_div를 빈 문자열로 보냈으니, 조회도 빈 문자열 / 또는 career_div 없이 둘 다 시도
# ============================================================
def verify_lists(headers, uploaded_items):
    print("==========[VERIFY AFTER UPLOAD]==========")

    code, body = safe_get(URL_GRADE_LIST, headers, params={"user_no": HR_USER_NO})
    cnt = len(body) if isinstance(body, list) else None
    print(f"[VERIFY] grade list user_no={HR_USER_NO} -> {code} count={cnt}")

    code, body = safe_get(URL_COMPANY_LIST, headers, params={"user_no": HR_USER_NO, "limit": 1000})
    cnt = len(body) if isinstance(body, list) else None
    print(f"[VERIFY] company list user_no={HR_USER_NO} -> {code} count={cnt}")

    # area_div만이라도 뽑아서 조회해보기 (career_div는 빈 문자열일 수 있음)
    areas = []
    seen = set()
    for it in uploaded_items:
        if is_pjt_item(it) and not is_company_item(it):
            area_div = it.get("area_div")
            if area_div and area_div not in seen:
                seen.add(area_div)
                areas.append(area_div)

    if not areas:
        print("[VERIFY] pjt list: no area_div found")
    else:
        for area_div in areas:
            # 1) career_div 없이 조회(서버가 전체 반환하는 타입이면 이게 더 확실)
            code1, body1 = safe_get(
                URL_PJT_LIST,
                headers,
                params={"user_no": HR_USER_NO, "area_div": area_div, "limit": 1000},
            )
            cnt1 = len(body1) if isinstance(body1, list) else None
            print(f"[VERIFY] pjt list(no career_div) user_no={HR_USER_NO} area_div={area_div} -> {code1} count={cnt1}")

            # 2) career_div 빈 문자열로 조회(서버가 필수로 받는 타입이면 필요)
            code2, body2 = safe_get(
                URL_PJT_LIST,
                headers,
                params={"user_no": HR_USER_NO, "area_div": area_div, "career_div": "", "limit": 1000},
            )
            cnt2 = len(body2) if isinstance(body2, list) else None
            print(f"[VERIFY] pjt list(career_div='') user_no={HR_USER_NO} area_div={area_div} -> {code2} count={cnt2}")

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
