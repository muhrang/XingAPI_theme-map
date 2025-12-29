# xing_step3_3pass_skip_no_exit.py
# ✅ 요구사항 그대로
# - 프로그램 시작할 때마다: 이미지 선택(파일창) + 관심종목 전체 로드
# - STEP3: PASS1(첫 50) 1번 → PASS2(나머지) 1번 → PASS3(미분류/누락만) 1번
# - Gemini가 429/오류 나도 "절대 종료 X" (그냥 스킵하고) 바로 화면(거래대금/등락률/주도주/관심종목) 출력
# - STEP3에서 일부라도 성공하면 mapping.json 저장 / 실패해도 프로그램은 계속 진행

import os
import time
import json
import base64
import re
import unicodedata
import pythoncom
import win32com.client
from dataclasses import dataclass
from tkinter import Tk, filedialog

import requests


# =========================
# 설정 (xing.py 스타일)
# =========================
@dataclass
class XingConfig:
    user_id: str = os.environ.get("XING_USER_ID", "")
    user_pw: str = os.environ.get("XING_USER_PW", "")
    cert_pw: str = os.environ.get("XING_CERT_PW", "")
    server: str = os.environ.get("XING_SERVER", "real")  # real/demo
    timeout_sec: int = 12

CFG = XingConfig()

RES_DIR = r"C:\xingAPI_Program(2025.06.07)\Res"
SERVER_ADDR = {"real": "hts.ebestsec.co.kr", "demo": "demo.ebestsec.co.kr"}

TOP_MONEY = 50
TOP_RATE  = 30

COND_WATCH = "관심종목"
COND_LEAD  = "주도주"

REFRESH_SEC = 40

COOLDOWN_SEC = 1.2
RETRY_MAX = 1
RETRY_SLEEP_SEC = 1.5

PRINT_MIN_RATE = 5.0

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_PATH = os.path.join(BASE_DIR, "mapping.json")

DEFAULT_THEME = "미분류"


# =========================
# Gemini 설정
# =========================
GEMINI_API_KEY_DIRECT = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL_FULLNAME = "models/gemini-2.5-flash"

GEMINI_MAX_TOKENS = 8192
GEMINI_BATCH_SIZE = 50
GEMINI_TIMEOUT_SEC = 180          # read timeout
GEMINI_CONNECT_TIMEOUT_SEC = 10   # connect timeout


# =========================
# 유틸
# =========================
def sstrip(x) -> str:
    return (x or "").strip()

def to_float_or_none(x):
    s = sstrip(x).replace(",", "")
    if s == "" or s == "-":
        return None
    try:
        return float(s)
    except Exception:
        return None

def fmt_rate(rate):
    return "?%" if rate is None else f"{rate:.2f}%"

def clear_screen():
    os.system("cls")

def is_etn(code: str) -> bool:
    return (not code) or (not code.isdigit())

def is_etf(name: str) -> bool:
    if not name:
        return True
    u = name.upper()
    return any(k in u for k in ["KODEX", "TIGER", "KBSTAR", "ARIRANG", "HANARO", "ACE", "SOL", "ETF", "ETN"])

def is_etf_etn(code: str, name: str) -> bool:
    return is_etn(code) or is_etf(name)

def sort_by_rate_desc(rows):
    def key(r):
        v = r.get("rate")
        return (-1e18 if v is None else v)
    return sorted(rows, key=key, reverse=True)

def apply_min_rate_filter(rows, min_rate):
    if min_rate is None:
        return rows or []
    out = []
    for r in rows or []:
        v = r.get("rate")
        if v is not None and v >= min_rate:
            out.append(r)
    return out


# =========================
# 한글 표시폭 정렬
# =========================
def disp_width(s: str) -> int:
    w = 0
    for ch in s:
        ea = unicodedata.east_asian_width(ch)
        w += 2 if ea in ("F", "W") else 1
    return w

def ljust_disp(s: str, width: int) -> str:
    pad = width - disp_width(s)
    if pad <= 0:
        return s
    return s + (" " * pad)

def center_disp(s: str, width: int) -> str:
    w = disp_width(s)
    if w >= width:
        return s
    left = (width - w) // 2
    right = width - w - left
    return (" " * left) + s + (" " * right)


# =========================
# 패널 출력
# =========================
def build_panel_lines(title, rows, min_rate=None):
    body = []
    if rows:
        for r in rows:
            v = r.get("rate")
            if min_rate is not None and (v is None or v < min_rate):
                continue
            body.append((r["name"], fmt_rate(v)))
    if not body:
        body = [("(없음)", "")]

    name_w = max(disp_width(n) for n, _ in body)
    rate_w = max(disp_width(rt) for _, rt in body)
    width = max(disp_width(title), name_w + 2 + rate_w)

    lines = [center_disp(title, width), "=" * width]
    for name, rt in body:
        line = f"{ljust_disp(name, name_w)}  {rt.rjust(rate_w)}"
        lines.append(ljust_disp(line, width))
    return lines, width

def print_panels_side_by_side(panel_infos, gap=" | "):
    panels = [p for p, _ in panel_infos]
    widths = [w for _, w in panel_infos]
    max_len = max(len(p) for p in panels) if panels else 0

    for i in range(max_len):
        cells = []
        for col, p in enumerate(panels):
            if i < len(p):
                cells.append(p[i])
            else:
                cells.append(" " * widths[col])

        last = -1
        for j in range(len(cells) - 1, -1, -1):
            if cells[j].strip():
                last = j
                break
        if last == -1:
            continue
        print(gap.join(cells[:last + 1]))


# =========================
# 이벤트
# =========================
class XASessionEvents:
    def OnLogin(self, code, msg):
        self.parent._login_code = code
        self.parent._login_msg = msg

class XAQueryEvents:
    def OnReceiveData(self, tr_code):
        self.parent._received = True
        self.parent._last_tr = tr_code


# =========================
# XING API
# =========================
class XingAPI:
    def __init__(self):
        self._received = False
        self._last_tr = ""
        self._login_code = None
        self._login_msg = ""

        self.session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASessionEvents)
        self.session.parent = self

        self.query = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEvents)
        self.query.parent = self

    def _wait(self, timeout, tag="TR timeout"):
        st = time.time()
        while not self._received:
            pythoncom.PumpWaitingMessages()
            if time.time() - st > timeout:
                raise TimeoutError(tag)
            time.sleep(0.01)

    def _set_res(self, res_filename: str) -> str:
        path = os.path.join(RES_DIR, res_filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"res 파일 없음: {path}")
        self.query.ResFileName = path
        return path

    def _request_service_or_request0(self, tr_code: str):
        if hasattr(self.query, "RequestService"):
            return self.query.RequestService(tr_code, "")
        return self.query.Request(0)

    def login(self):
        addr = SERVER_ADDR[CFG.server]
        if not self.session.ConnectServer(addr, 20001):
            raise RuntimeError("서버 연결 실패")

        server_type = 0 if CFG.server == "real" else 1
        self.session.Login(CFG.user_id, CFG.user_pw, CFG.cert_pw, server_type, 0)

        st = time.time()
        while self._login_code is None:
            pythoncom.PumpWaitingMessages()
            if time.time() - st > CFG.timeout_sec:
                raise TimeoutError("로그인 응답 타임아웃")
            time.sleep(0.01)

        if self._login_code != "0000":
            raise RuntimeError(f"로그인 실패: {self._login_code} {self._login_msg}")

        print("[LOGIN] 성공")

    def t1463_top(self):
        self._received = False
        self._set_res("t1463.res")

        inb = "t1463InBlock"
        self.query.SetFieldData(inb, "gubun", 0, "0")
        self.query.SetFieldData(inb, "jnilgubun", 0, "0")
        self.query.SetFieldData(inb, "idx", 0, "")

        ret = self.query.Request(0)
        if ret < 0:
            raise RuntimeError(f"t1463 Request 실패 ret={ret}")
        self._wait(CFG.timeout_sec, "t1463 timeout")

        outb = "t1463OutBlock1"
        cnt = self.query.GetBlockCount(outb)

        rows = []
        for i in range(cnt):
            code = sstrip(self.query.GetFieldData(outb, "shcode", i))
            name = sstrip(self.query.GetFieldData(outb, "hname", i))
            rate = to_float_or_none(self.query.GetFieldData(outb, "diff", i))
            if code and not is_etf_etn(code, name):
                rows.append({"code": code, "name": name, "rate": rate})

        return sort_by_rate_desc(rows[:TOP_MONEY])

    def t1441_top(self):
        self._received = False
        self._set_res("t1441.res")

        inb = "t1441InBlock"
        self.query.SetFieldData(inb, "gubun1", 0, "0")
        self.query.SetFieldData(inb, "gubun2", 0, "0")
        self.query.SetFieldData(inb, "gubun3", 0, "0")
        self.query.SetFieldData(inb, "idx", 0, "")

        ret = self.query.Request(0)
        if ret < 0:
            raise RuntimeError(f"t1441 Request 실패 ret={ret}")
        self._wait(CFG.timeout_sec, "t1441 timeout")

        outb = "t1441OutBlock1"
        cnt = self.query.GetBlockCount(outb)

        rows = []
        for i in range(cnt):
            code = sstrip(self.query.GetFieldData(outb, "shcode", i))
            name = sstrip(self.query.GetFieldData(outb, "hname", i))
            rate = to_float_or_none(self.query.GetFieldData(outb, "diff", i))
            if code and not is_etf_etn(code, name):
                rows.append({"code": code, "name": name, "rate": rate})

        return sort_by_rate_desc(rows)[:TOP_RATE]

    def t1866_list(self):
        self._received = False
        self._set_res("t1866.res")

        inb = "t1866InBlock"
        self.query.SetFieldData(inb, "user_id", 0, CFG.user_id)
        self.query.SetFieldData(inb, "gb", 0, "0")
        self.query.SetFieldData(inb, "group_name", 0, "")
        self.query.SetFieldData(inb, "cont", 0, "0")
        self.query.SetFieldData(inb, "cont_key", 0, "")

        ret = self.query.Request(0)
        if ret < 0:
            raise RuntimeError(f"t1866 Request 실패 ret={ret}")
        self._wait(CFG.timeout_sec, "t1866 timeout")

        outb = "t1866OutBlock1"
        cnt = self.query.GetBlockCount(outb)

        rows = []
        for i in range(cnt):
            rows.append({
                "query_index": sstrip(self.query.GetFieldData(outb, "query_index", i)),
                "query_name": sstrip(self.query.GetFieldData(outb, "query_name", i)),
            })
        return rows

    def t1857_snapshot_S0(self, query_index: str):
        self._set_res("t1857.res")

        inb = "t1857InBlock"
        self.query.SetFieldData(inb, "sRealFlag", 0, "0")
        self.query.SetFieldData(inb, "sSearchFlag", 0, "S")
        self.query.SetFieldData(inb, "query_index", 0, str(query_index))

        last_ret = None
        for _ in range(RETRY_MAX):
            time.sleep(COOLDOWN_SEC)
            self._received = False
            ret = self._request_service_or_request0("t1857")
            last_ret = ret
            if ret >= 0:
                self._wait(CFG.timeout_sec, "t1857 timeout")
                break
            time.sleep(RETRY_SLEEP_SEC)

        if last_ret is None or last_ret < 0:
            raise RuntimeError(f"t1857 호출 실패 ret={last_ret}")

        outb = "t1857OutBlock1"
        cnt = self.query.GetBlockCount(outb)

        rows = []
        for i in range(cnt):
            code = sstrip(self.query.GetFieldData(outb, "shcode", i))
            name = sstrip(self.query.GetFieldData(outb, "hname", i))
            rate = to_float_or_none(self.query.GetFieldData(outb, "diff", i))
            if code and not is_etf_etn(code, name):
                rows.append({"code": code, "name": name, "rate": rate})

        return sort_by_rate_desc(rows)


# =========================
# mapping.json 로드/그룹핑
# =========================
def load_mapping_file():
    try:
        with open(MAPPING_PATH, "r", encoding="utf-8") as f:
            j = json.load(f)
        mp = j.get("map", {})
        if isinstance(mp, dict):
            clean = {}
            for k, v in mp.items():
                kk = str(k).strip()
                if kk.isdigit() and len(kk) == 6:
                    clean[kk] = str(v).strip() if v is not None else DEFAULT_THEME
            return clean
    except Exception:
        pass
    return {}

def group_rows_by_theme(rows, code_to_theme: dict):
    buckets = {}
    for r in rows or []:
        code = str(r.get("code", "")).strip()
        theme = code_to_theme.get(code, DEFAULT_THEME)
        buckets.setdefault(theme, []).append(r)
    for t in buckets:
        buckets[t] = sort_by_rate_desc(buckets[t])
    return buckets


# =========================
# Gemini: prompt/파싱/복구
# =========================
def get_gemini_key():
    if GEMINI_API_KEY_DIRECT.strip():
        return GEMINI_API_KEY_DIRECT.strip()
    raise RuntimeError('환경변수 GEMINI_API_KEY가 없습니다. 예) setx GEMINI_API_KEY "AIzaSy...."')

def gemini_prompt():
    return f"""
너는 "분류표 이미지"를 보고 watch_rows(관심종목 리스트)를 테마에 매핑해 JSON을 만드는 프로그램이다.

[절대 규칙]
- 너는 반드시 JSON "한 덩어리"만 출력한다. (설명/문장/코드펜스/마크다운/앞뒤 텍스트 금지)
- JSON 형식은 반드시 아래 스키마 그대로:
{{
  "themes": ["테마1","테마2",...],
  "map": {{
    "종목코드": "테마명",
    ...
  }}
}}
- themes: 이미지에 실제로 보이는 테마명만 넣어라.
- map: watch_rows에 있는 모든 종목코드를 반드시 포함해라. (누락 금지)
- 이미지에서 테마를 찾기 애매하면 해당 종목은 "{DEFAULT_THEME}"로 넣어라.
- 테마명이 이미지에 없으면 "{DEFAULT_THEME}"로 넣어라.

지금부터 이미지와 watch_rows를 보고, 위 스키마 JSON만 출력해라.
""".strip()

def safe_parse_json_from_text(text: str):
    if not text:
        return None
    t = text.strip()
    t = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", t.strip())
    t = re.sub(r"\s*```$", "", t.strip())

    a = t.find("{")
    b = t.rfind("}")
    if a == -1 or b == -1 or b <= a:
        return None
    t = t[a:b+1]

    t = re.sub(r",\s*([}\]])", r"\1", t)
    t = t.replace("\u0000", "").strip()

    try:
        return json.loads(t)
    except Exception:
        return None

def recover_map_by_regex(raw_text: str):
    if not raw_text:
        return {}, []

    themes = []
    m = re.search(r'"themes"\s*:\s*\[(.*?)\]', raw_text, flags=re.DOTALL)
    if m:
        inner = m.group(1)
        themes = [s.strip() for s in re.findall(r'"([^"]+)"', inner)]
        themes = [t for t in themes if t]

    pairs = re.findall(r'"(\d{6})"\s*:\s*"([^"]*)"', raw_text)
    mp = {}
    for code, theme in pairs:
        mp[code] = (theme.strip() or DEFAULT_THEME)

    return mp, themes

def normalize_mapping(parsed: dict, codes: list):
    parsed = parsed or {}
    parsed.setdefault("themes", [])
    parsed.setdefault("map", {})

    if not isinstance(parsed["themes"], list):
        parsed["themes"] = []
    if not isinstance(parsed["map"], dict):
        parsed["map"] = {}

    for c in codes:
        if c not in parsed["map"]:
            parsed["map"][c] = DEFAULT_THEME

    themes = []
    seen = set()
    for t in parsed["themes"]:
        t = str(t).strip()
        if not t:
            continue
        if t not in seen:
            themes.append(t)
            seen.add(t)
    parsed["themes"] = themes

    clean_map = {}
    for k, v in parsed["map"].items():
        kk = str(k).strip()
        vv = str(v).strip() if v is not None else DEFAULT_THEME
        if kk.isdigit() and len(kk) == 6:
            clean_map[kk] = vv if vv else DEFAULT_THEME
    parsed["map"] = clean_map

    return parsed

def _parse_retry_delay_seconds(text: str):
    m = re.search(r"retryDelay[^0-9]*(\d+)\s*s", text, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None

def _checkpoint_save(merged_themes, theme_seen, merged_map):
    out = {
        "themes": merged_themes + ([DEFAULT_THEME] if DEFAULT_THEME not in theme_seen else []),
        "map": dict(merged_map),
    }
    with open(MAPPING_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

def gemini_generate_mapping_3pass(image_bytes: bytes, mime: str, watch_rows: list):
    """
    ✅ PASS1: 첫 50개 1회
    ✅ PASS2: 나머지 1회
    ✅ PASS3: 미분류/누락만 1회
    ✅ 어떤 PASS가 실패(429 포함)해도 예외로 프로그램 죽이지 않고 "스킵"함
    """
    key = get_gemini_key()
    url = f"https://generativelanguage.googleapis.com/v1beta/{GEMINI_MODEL_FULLNAME}:generateContent?key={key}"
    img_b64 = base64.b64encode(image_bytes).decode("ascii")

    slim_rows = [{"code": r.get("code"), "name": r.get("name", "")} for r in watch_rows]
    # 코드 형식 정리
    slim_rows = [r for r in slim_rows if str(r.get("code", "")).isdigit() and len(str(r.get("code", ""))) == 6]
    all_codes = [r["code"] for r in slim_rows]

    merged_map = {}
    merged_themes = []
    theme_seen = set()

    def add_themes(ths):
        for t in ths or []:
            tt = str(t).strip()
            if tt and tt not in theme_seen:
                merged_themes.append(tt)
                theme_seen.add(tt)

    def call_one(pass_name: str, batch_rows):
        payload = {
            "contents": [{
                "role": "user",
                "parts": [
                    {"text": gemini_prompt()},
                    {"inline_data": {"mime_type": mime, "data": img_b64}},
                    {"text": json.dumps({"watch_rows": batch_rows}, ensure_ascii=False)}
                ]
            }],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": GEMINI_MAX_TOKENS,
                "responseMimeType": "application/json"
            }
        }

        backoff = 1.2
        for attempt in range(1, RETRY_MAX + 1):
            try:
                print(f"[GEMINI] {pass_name} send rows={len(batch_rows)} attempt={attempt}", flush=True)
                r = requests.post(url, json=payload, timeout=(GEMINI_CONNECT_TIMEOUT_SEC, GEMINI_TIMEOUT_SEC))
                print(f"[GEMINI] {pass_name} http={r.status_code} bytes={len(r.content)}", flush=True)

                if r.status_code == 200:
                    data = r.json()
                    parts = data["candidates"][0]["content"]["parts"]
                    text = "".join(p.get("text", "") for p in parts if isinstance(p, dict))
                    print(f"[GEMINI] {pass_name} ok text_len={len(text)}", flush=True)
                    return text

                if r.status_code == 429:
                    sec = _parse_retry_delay_seconds(r.text) or 60
                    print(f"[GEMINI] {pass_name} 429 sleep={sec}s", flush=True)
                    time.sleep(sec)
                    continue

                if 500 <= r.status_code <= 599:
                    print(f"[GEMINI] {pass_name} {r.status_code} backoff={backoff}s", flush=True)
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                raise RuntimeError(f"Gemini API 오류 {r.status_code}: {r.text[:200]}")

            except Exception as e:
                print(f"[GEMINI] {pass_name} exception: {e} backoff={backoff}s", flush=True)
                time.sleep(backoff)
                backoff *= 2

        raise RuntimeError(f"{pass_name}: Gemini 호출 재시도 실패")

    def apply_result(pass_name: str, batch_rows):
        """PASS 단위 실패 시 스킵(프로그램 종료 금지)"""
        if not batch_rows:
            return

        batch_codes = [r["code"] for r in batch_rows]

        try:
            text = call_one(pass_name, batch_rows)
        except Exception as e:
            print(f"[GEMINI] {pass_name} FAILED -> skip pass ({e})", flush=True)
            return

        parsed = safe_parse_json_from_text(text)
        if parsed:
            parsed = normalize_mapping(parsed, batch_codes)
            add_themes(parsed.get("themes", []))
            merged_map.update(parsed.get("map", {}))
            _checkpoint_save(merged_themes, theme_seen, merged_map)
            return

        rec_map, rec_themes = recover_map_by_regex(text)
        add_themes(rec_themes)
        for c in batch_codes:
            merged_map[c] = rec_map.get(c, DEFAULT_THEME)
        _checkpoint_save(merged_themes, theme_seen, merged_map)

    # PASS1 / PASS2
    first = slim_rows[:GEMINI_BATCH_SIZE]
    second = slim_rows[GEMINI_BATCH_SIZE:GEMINI_BATCH_SIZE*2]

    apply_result("PASS1(first50)", first)
    apply_result("PASS2(rest)", second)

    # PASS3: 미분류/누락만 "1회"
    pending = []
    for r in slim_rows:
        c = r["code"]
        v = merged_map.get(c, "")
        if not str(v).strip() or str(v).strip() == DEFAULT_THEME:
            pending.append(r)

    if pending:
        print(f"[GEMINI] PASS3 pending={len(pending)} (미분류/누락만 1회)", flush=True)
        # PASS3도 1번만: 최대 50개만 물어보고, 나머지는 그냥 넘어감(원칙 그대로)
        apply_result("PASS3(pending_once)", pending[:GEMINI_BATCH_SIZE])

    # 최종 보정: 비어있으면 미분류
    for c in all_codes:
        v = merged_map.get(c, "")
        if not str(v).strip():
            merged_map[c] = DEFAULT_THEME

    if DEFAULT_THEME not in theme_seen:
        merged_themes.append(DEFAULT_THEME)
        theme_seen.add(DEFAULT_THEME)

    out = {"themes": merged_themes, "map": merged_map}
    _checkpoint_save(merged_themes, theme_seen, merged_map)
    return out


# =========================
# 이미지 파일 선택 (그대로)
# =========================
def pick_image_file():
    root = Tk()
    root.withdraw()
    path = filedialog.askopenfilename(
        title="분류표 이미지 선택 (캡처 파일)",
        filetypes=[("Image files", "*.png;*.jpg;*.jpeg;*.bmp;*.webp"), ("All files", "*.*")]
    )
    root.destroy()
    if not path:
        raise RuntimeError("이미지 선택 취소")
    ext = os.path.splitext(path)[1].lower()
    if ext in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif ext == ".bmp":
        mime = "image/bmp"
    elif ext == ".webp":
        mime = "image/webp"
    else:
        mime = "image/png"
    with open(path, "rb") as f:
        b = f.read()
    return path, b, mime


# =========================
# main
# =========================
def main():
    pythoncom.CoInitialize()

    x = XingAPI()
    x.login()

    conds = x.t1866_list()
    name_to_qidx = {c["query_name"]: c["query_index"] for c in conds if c.get("query_name") and c.get("query_index")}

    watch_qidx = name_to_qidx.get(COND_WATCH, "")
    lead_qidx  = name_to_qidx.get(COND_LEAD, "")

    # STEP1: 이미지
    print("\n[STEP1] 분류표(테마 분류 캡처) 이미지를 선택하세요...")
    img_path, img_bytes, mime = pick_image_file()
    print(f"[OK] 이미지 선택됨: {img_path}")

    # STEP2: 관심종목 전체
    print("\n[STEP2] XING 관심종목(전체) 불러오는 중... (Gemini용: 필터 없음)")
    watch_rows_all = x.t1857_snapshot_S0(watch_qidx) if watch_qidx else []
    watch_rows_all = sort_by_rate_desc(watch_rows_all)
    print(f"[OK] 관심종목 전체 {len(watch_rows_all)}개 로드")

    watch_payload = [{"code": r.get("code"), "name": r.get("name", "")} for r in watch_rows_all]

    # STEP3: Gemini (절대 종료 금지)
    print("\n[STEP3] Gemini 매핑 생성 (PASS1 50개 1회 → PASS2 나머지 1회 → PASS3 미분류/누락 1회) 후 바로 출력")
    try:
        mapping = gemini_generate_mapping_3pass(img_bytes, mime, watch_payload)
        print(f"[OK] mapping.json 저장 완료: {MAPPING_PATH} (codes={len(mapping.get('map', {}))})")
    except Exception as e:
        # ✅ 여기서 프로그램 절대 종료 X
        print(f"[STEP3] Gemini 실패/제한/오류 -> 매핑 없이 계속 진행: {e}")

        # mapping.json이 없으면 최소 파일 하나 만들기(화면 루프는 정상 동작)
        try:
            if not os.path.exists(MAPPING_PATH):
                fallback = {"themes": [DEFAULT_THEME], "map": {}}
                with open(MAPPING_PATH, "w", encoding="utf-8") as f:
                    json.dump(fallback, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # STEP4: 화면 출력 루프 (항상 실행)
    while True:
        try:
            clear_screen()
            now = time.strftime("%Y-%m-%d %H:%M:%S")

            money_rows_raw = x.t1463_top()
            rate_rows_raw  = x.t1441_top()
            lead_rows_raw  = x.t1857_snapshot_S0(lead_qidx)  if lead_qidx  else []
            watch_rows_raw = x.t1857_snapshot_S0(watch_qidx) if watch_qidx else []

            money_rows = sort_by_rate_desc(apply_min_rate_filter(money_rows_raw, PRINT_MIN_RATE))
            rate_rows  = sort_by_rate_desc(apply_min_rate_filter(rate_rows_raw,  PRINT_MIN_RATE))
            lead_rows  = sort_by_rate_desc(apply_min_rate_filter(lead_rows_raw,  PRINT_MIN_RATE))
            watch_rows2 = sort_by_rate_desc(apply_min_rate_filter(watch_rows_raw, PRINT_MIN_RATE))

            p_money = build_panel_lines("[거래대금상위]", money_rows, min_rate=None)
            p_rate  = build_panel_lines("[등락률상위]",   rate_rows,  min_rate=None)
            p_lead  = build_panel_lines("[주도주]",       lead_rows,  min_rate=None)
            p_watch = build_panel_lines("[관심종목]",     watch_rows2, min_rate=None)

            print_panels_side_by_side([p_money, p_rate, p_lead, p_watch], gap=" | ")

            # 테마 패널은 mapping.json이 있어야만(없어도 위 4패널은 계속 출력됨)
            code_to_theme = load_mapping_file()
            if code_to_theme:
                buckets = group_rows_by_theme(watch_rows2, code_to_theme)
                theme_order = sorted(
                    buckets.keys(),
                    key=lambda t: (t == DEFAULT_THEME, -len(buckets[t]), t)
                )

                print("\n" + "=" * 90)
                print(f"[관심종목 테마별 분화] (mapping.json 기준 / 현재 {PRINT_MIN_RATE}% 이상만)")
                print("=" * 90)

                per_row = 3
                for i in range(0, len(theme_order), per_row):
                    chunk = theme_order[i:i+per_row]
                    panel_infos = []
                    for t in chunk:
                        title = f"[{t}] ({len(buckets[t])})"
                        panel_infos.append(build_panel_lines(title, buckets[t], min_rate=None))
                    print_panels_side_by_side(panel_infos, gap=" | ")
                    print("")

            print("\n[TIME]", now)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print("\n[ERROR]", e)

        time.sleep(REFRESH_SEC)


if __name__ == "__main__":
    main()
