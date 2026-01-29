import json
import time
import uuid
from datetime import datetime
from pathlib import Path

import requests

# ============================================================
# ✅ 실행 설정 (여기만 바꾸면 됨)
# ============================================================
JSON_PATH = r"변주석.json"

BASE_URL   = "http://172.20.60.71:8080/api"  # 사내 서버
HR_USER_NO = "goodbye3372"
HR_API_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlbXAiLCJ1c2VyX2lkIjoxNCwicm9sZSI6IkVNUExPWUVFIiwiZXhwIjoxNzY5NjQ3NTQ2fQ.SVT5EBIRNEL0cmI4Kxs3G7bu2KBWo977CYqkxR93nsQ"
DRY_RUN = False       # True면 실제 POST 안함
LIMIT = None          # 예: 20 (테스트로 일부만 업로드)
TIMEOUT_SEC = 15

# 🔥 안전모드: 서버가 user_no를 payload 무시/토큰기준으로 저장하면 즉시 중단
STRICT_USER_MATCH = True

# 🔥 업로드 후 검증(GET) 여부
VERIFY_AFTER_UPLOAD = True

# ============================================================
# ✅ 엔드포인트
# ============================================================
URL_COMPANY = f"{BASE_URL}/career-company/"
URL_PJT     = f"{BASE_URL}/career-pjt/"
URL_GRADE   = f"{BASE_URL}/career-grade/"

# (검증용 리스트 GET)
URL_PJT_LIST     = f"{BASE_URL}/career-pjt/"
URL_COMPANY_LIST = f"{BASE_URL}/career-company/"
URL_GRADE_LIST   = f"{BASE_URL}/career-grade/"

# ============================================================
# ✅ 공용: 헤더
# ============================================================
def build_headers():
    token = HR_API_TOKEN.strip()
    if token.lower().startswith("bearer "):
        auth = token
    else:
        auth = f"Bearer {token}"

    return {
        "Authorization": auth,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

# ============================================================
# ✅ 아이템 타입 판별
# ============================================================
def is_grade_item(item: dict) -> bool:
    return any(k in item for k in ("grade_div", "grade_name", "field_name", "field_div"))

def is_company_item(item: dict) -> bool:
    return any(k in item for k in ("carr_strdate", "carr_comdate", "carr_comp"))

def is_pjt_item(item: dict) -> bool:
    return any(k in item for k in ("car_s_date", "car_f_date", "career_div"))

# ============================================================
# ✅ 라우팅 + payload 정규화 (user_no 강제 덮기)
#   ❗️POST에서는 seq 절대 안 보냄
# ============================================================
def route_and_build_payload(raw: dict):
    """
    return: (url, payload, kind)
      kind in {"grade","company","pjt"}
    """
    item = dict(raw)  # 원본 보호

    # user_no는 무조건 HR_USER_NO로 덮어쓰기
    item["user_no"] = HR_USER_NO

    # 1) grade (원래부터 seq 개념 없음)
    if is_grade_item(item) and (not is_company_item(item)) and (not is_pjt_item(item)):
        payload = {
            "user_no": HR_USER_NO,
            "area_div": item.get("area_div"),
            "grade_div": item.get("grade_div"),
            "field_div": item.get("field_div"),
            "field_name": item.get("field_name"),
            "grade_name": item.get("grade_name"),
            "grade_num": item.get("grade_num"),
        }
        return URL_GRADE, payload, "grade"

    # 2) company  ✅ seq 제거
    if is_company_item(item):
        payload = {
            "user_no": HR_USER_NO,
            "area_div": item.get("area_div"),
            "career_div": item.get("career_div"),

            "carr_strdate": item.get("carr_strdate"),
            "carr_comdate": item.get("carr_comdate"),
            "carr_comp": item.get("carr_comp"),
        }
        return URL_COMPANY, payload, "company"

    # 3) pjt ✅ seq 제거
    if is_pjt_item(item):
        payload = {
            "user_no": HR_USER_NO,
            "area_div": item.get("area_div"),
            "career_div": item.get("career_div"),

            "car_s_date": item.get("car_s_date"),
            "car_f_date": item.get("car_f_date"),
            "car_days": item.get("car_days"),

            "pjt_nm": item.get("pjt_nm"),
            "duty_field": item.get("duty_field"),
            "duty_job": item.get("duty_job"),

            "order_nm": item.get("order_nm"),
            "con_type1": item.get("con_type1"),
            "pro_field": item.get("pro_field"),
            "lev": item.get("lev"),

            "con_detail": item.get("con_detail"),
            "respon": item.get("respon"),
            "con_amt": item.get("con_amt"),

            "con_method": item.get("con_method"),
            "con_tech": item.get("con_tech"),
            "new_tech": item.get("new_tech"),
            "facility_div": item.get("facility_div"),

            "memo": item.get("memo"),
            "workplace": item.get("workplace"),
            "work_div": item.get("work_div"),
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
    """
    return: (ok:bool, status_code:int|None, resp_json|text|None)
    """
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
# ✅ (2) 서버가 저장했다고 말하는 user_no 검증
# ============================================================
def enforce_user_match_or_die(kind: str, payload: dict, resp):
    """
    - 서버 응답 dict에 user_no가 있으면 payload의 user_no와 비교
    - 다르면: 토큰 기준 저장/무시 가능성이 크므로 즉시 중단
    """
    if not STRICT_USER_MATCH:
        return

    sent_user_no = payload.get("user_no")

    if isinstance(resp, dict):
        saved_user_no = resp.get("user_no")
        # 응답에 user_no 자체가 없으면 강제 못함(서버 스타일)
        if saved_user_no is not None and saved_user_no != sent_user_no:
            raise RuntimeError(
                f"[FATAL] USER_NO MISMATCH kind={kind} sent={sent_user_no} resp={saved_user_no}\n"
                f"-> 서버가 payload user_no를 무시하고 토큰 기준으로 저장하거나, 다른 스코프로 저장 중일 가능성이 큼.\n"
                f"-> 즉시 중단 (교차 덮어쓰기/삭제 위험)"
            )

# ============================================================
# ✅ (3) 업로드 직후 GET로 저장 스코프 검증
# ============================================================
def verify_lists(headers):
    """
    지금 HR_USER_NO로 실제 데이터가 잡히는지 최소 확인:
      - grade list
      - company list
      - pjt list (기술경력/감리경력 각각)
    """
    print("==========[VERIFY AFTER UPLOAD]==========")

    # grade
    code, body = safe_get(URL_GRADE_LIST, headers, params={"user_no": HR_USER_NO})
    cnt = len(body) if isinstance(body, list) else None
    print(f"[VERIFY] grade list user_no={HR_USER_NO} -> {code} count={cnt}")

    # company
    code, body = safe_get(URL_COMPANY_LIST, headers, params={"user_no": HR_USER_NO, "limit": 1000})
    cnt = len(body) if isinstance(body, list) else None
    print(f"[VERIFY] company list user_no={HR_USER_NO} -> {code} count={cnt}")

    # pjt - 기술경력/감리경력
    for career_div in ["기술경력", "건설사업관리 및 감리경력"]:
        code, body = safe_get(
            URL_PJT_LIST,
            headers,
            params={
                "user_no": HR_USER_NO,
                "area_div": "건설기술인협회",
                "career_div": career_div,
                "limit": 1000,
            },
        )
        cnt = len(body) if isinstance(body, list) else None
        print(f"[VERIFY] pjt list user_no={HR_USER_NO} career_div={career_div} -> {code} count={cnt}")

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

        if kind == "unknown" or url is None:
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

        # ✅ (2) user_no 불일치 방지 체크
        try:
            enforce_user_match_or_die(kind, payload, resp)
        except Exception as e:
            print(str(e))
            # 치명적이므로 즉시 중단
            raise

        # ✅ 로그 (서버가 돌려주는 PK 힌트도 같이 찍어두기)
        sent_user = payload.get("user_no")
        resp_user = resp.get("user_no") if isinstance(resp, dict) else None
        resp_id   = resp.get("id") if isinstance(resp, dict) else None
        resp_seq  = resp.get("seq") if isinstance(resp, dict) else None

        print(f"[OK] idx={idx} kind={kind} status={status} sent_user={sent_user} resp_user={resp_user} resp_id={resp_id} resp_seq={resp_seq}")

    # 실패 저장
    if failures:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = Path(f"upload_failures_{ts}.json")
        out.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[WARN] failures saved: {out} (count={len(failures)})")

    print("====================================================")
    print(f"[DONE] total={len(data)} grade={cnt_grade} company={cnt_company} pjt={cnt_pjt} unknown={cnt_unknown}")
    print("====================================================")

    # ✅ (3) 업로드 결과 스코프 검증
    if (not DRY_RUN) and VERIFY_AFTER_UPLOAD:
        verify_lists(headers)

if __name__ == "__main__":
    main()
