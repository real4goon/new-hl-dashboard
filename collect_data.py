#!/usr/bin/env python3
"""
상급종합병원 비급여 데이터 수집기 V2.1
────────────────────────────────────────────
사용법:
  python collect_data.py             → 미수집 병원만 수집
  python collect_data.py --force     → 전체 재수집
  python collect_data.py --hospital 서울대병원  → 특정 병원만
  python collect_data.py --hospital 신규        → 아직 한 번도 수집하지 않은 병원만
  python collect_data.py --list      → 병원 목록 출력

V2.0 변경점
  · 병원 목록을 hospitals.json 으로 분리 (리포 맨 위 또는 data 폴더, 없거나 깨지면 내장 기본 24곳 사용)
  · 수집 결과를 data/last_run.json 에 병원별로 기록 (성공/데이터없음/실패, 비슷한 병원명 후보)
  · 검색명이 공백 차이만 있는 경우 자동 보정 (병원명이 완전히 같을 때만)
  · 기존 수집·저장 방식과 metadata.json 구조는 그대로 유지
V2.1: 검색명 진단 시 "…대학교 ○○병원", "…의과대학 ○○병원" 처럼 띄어쓴 등록명도 찾도록 보강
"""

import requests
import json
import os
import re
import sys
import time
import difflib
import argparse
from xml.etree import ElementTree as ET
from datetime import datetime

# ═══════════════════════════════════════════════════════
#  설정 (필요 시 수정)
# ═══════════════════════════════════════════════════════
_api_key = os.environ.get("HIRA_API_KEY", "")
if not _api_key:
    # 로컬 개발 편의: .env 파일 또는 직접 입력
    _api_key = os.environ.get("HIRA_API_KEY_LOCAL", "")
if not _api_key:
    print("❌ HIRA_API_KEY 환경변수가 설정되지 않았습니다.")
    print("   GitHub Actions: Settings → Secrets → HIRA_API_KEY")
    print("   로컬 실행: export HIRA_API_KEY=<키값>")
    sys.exit(1)

CONFIG = {
    "api_key"   : _api_key,
    "base_url"  : "https://apis.data.go.kr/B551182/nonPaymentDamtInfoService/getNonPaymentItemHospDtlList",
    "output_dir": "data",
    "chunk"     : 500,    # 회당 최대 요청 건수 (서버 부하 방지)
    "delay"     : 1.2,    # 페이지 간 딜레이(초)
    "max_retry" : 3,      # 최대 재시도
    "timeout"   : 20,     # 요청 타임아웃(초)
}

# ═══════════════════════════════════════════════════════
#  서울·경기 상급종합병원 목록
#  형식: "표시명(짧게)": "API 검색용 실제 병원명"
#  ※ API 검색명이 맞지 않으면 데이터 0건 → 터미널에서 확인 후 수정
# ═══════════════════════════════════════════════════════
_DEFAULT_HOSPITALS = {   # hospitals.json 이 없을 때만 쓰는 내장 기본 목록
    # ── 서울 ──────────────────────────────────────────
    "서울대병원"     : "서울대학교병원",
    "삼성서울병원"   : "삼성서울병원",
    "서울아산병원"   : "재단법인아산사회복지재단 서울아산병원",
    "세브란스병원"   : "연세대학교의과대학세브란스병원",
    "강남세브란스"   : "연세대학교의과대학강남세브란스병원",
    "서울성모병원"   : "학교법인가톨릭학원가톨릭대학교서울성모병원",
    "고대안암병원"   : "학교법인 고려중앙학원 고려대학교의과대학부속병원(안암병원)",
    "고대구로병원"   : "고려대학교의과대학부속구로병원",
    "경희대병원"     : "경희대학교병원",
    "한양대병원"     : "한양대학교병원",
    "중앙대병원"     : "중앙대학교병원",
    "목동병원"       : "이화여자대학교의과대학부속목동병원",
    "강북삼성병원"   : "강북삼성병원",
    # ── 경기·인천 ──────────────────────────────────────
    "한림성심병원"   : "한림대학교성심병원",
    "아주대병원"     : "아주대학교병원",
    "분당서울대병원" : "분당서울대학교병원",
    "길병원"         : "의료법인 길의료재단 길병원",
    "인하대병원"     : "인하대학교의과대학부속병원",
    "고대안산병원"   : "고려대학교의과대학부속안산병원",
    "부천성모병원"   : "가톨릭대학교부천성모병원",
    "의정부성모병원" : "가톨릭대학교의정부성모병원",
    "성빈센트병원"   : "가톨릭대학교성빈센트병원",
    "인천성모병원"   : "가톨릭대학교인천성모병원",
    "국립암센터"     : "국립암센터",
}

# 병원 목록 파일: 리포 맨 위(루트)에 있으면 그것을, 없으면 data 폴더 안 파일을 사용
HOSPITALS_FILE = next((p for p in ("hospitals.json", os.path.join(CONFIG["output_dir"], "hospitals.json"))
                       if os.path.exists(p)), "hospitals.json")
LAST_RUN_FILE  = os.path.join(CONFIG["output_dir"], "last_run.json")


def load_hospitals() -> dict:
    """
    data/hospitals.json → {표시명: 병원정보} 반환.
    파일이 없거나 형식이 잘못돼도 수집이 멈추지 않도록 내장 기본 목록으로 대체한다.
    """
    default = {n: {"name": n, "searchName": s} for n, s in _DEFAULT_HOSPITALS.items()}
    if not os.path.exists(HOSPITALS_FILE):
        print(f"ℹ  hospitals.json 없음 → 내장 기본 목록 {len(default)}곳 사용")
        return default
    try:
        with open(HOSPITALS_FILE, encoding="utf-8") as f:
            raw = json.load(f)
        rows = raw.get("hospitals") if isinstance(raw, dict) else raw
        result = {}
        for r in rows:
            name   = str(r.get("name", "")).strip()
            search = str(r.get("searchName", "")).strip()
            if not name or not search:
                print(f"⚠  hospitals.json: name/searchName 이 비어 있어 무시합니다 → {r}")
                continue
            if r.get("enabled", True) is False:
                continue
            if name in result:
                print(f"⚠  hospitals.json: 표시명 중복 '{name}' → 앞의 항목만 사용")
                continue
            result[name] = {**r, "name": name, "searchName": search}
        if not result:
            raise ValueError("유효한 병원 항목이 없음")
        return result
    except Exception as e:
        print(f"⚠  hospitals.json 읽기 실패 ({e}) → 내장 기본 목록 사용")
        return default


HOSPITAL_INFO = load_hospitals()                                   # 표시명 → 상세정보
HOSPITALS     = {n: i["searchName"] for n, i in HOSPITAL_INFO.items()}   # 표시명 → 검색명 (기존 코드 호환)

# ═══════════════════════════════════════════════════════
#  수집 함수
# ═══════════════════════════════════════════════════════
def _norm(s: str) -> str:
    """공백 제거 (병원명 비교용)"""
    return re.sub(r"\s+", "", s or "")


def fetch_hospital(display_name: str, search_name: str, seen_names: set = None, name_filter: str = None) -> list:
    """단일 병원 전 페이지 수집 → 레코드 리스트 반환
    seen_names : 응답에 등장한 병원명(yadmNm)을 모아둘 set (선택)
    name_filter: 공백 제거한 병원명이 이 값과 같은 행만 사용 (선택, 자동 보정 때만 사용)
    """
    cfg    = CONFIG
    result = []
    page   = 1

    while True:
        params = {
            "ServiceKey": cfg["api_key"],
            "pageNo"    : page,
            "numOfRows" : cfg["chunk"],
            "yadmNm"    : search_name,
        }

        # 재시도 로직
        resp = None
        for attempt in range(cfg["max_retry"]):
            try:
                resp = requests.get(cfg["base_url"], params=params, timeout=cfg["timeout"])
                resp.raise_for_status()
                break
            except Exception as e:
                wait = 2 ** attempt
                print(f"    ⚠  시도 {attempt+1}/{cfg['max_retry']} 실패 ({e}) → {wait}초 대기")
                time.sleep(wait)
        else:
            raise ConnectionError("모든 재시도 실패")

        # XML 파싱
        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError:
            raise ValueError(f"XML 파싱 실패 | 응답 앞부분: {resp.text[:300]}")

        # API 오류 코드 확인
        code = root.findtext(".//resultCode") or ""
        if code and code not in ("00", "0000"):
            msg = root.findtext(".//resultMsg") or "알 수 없는 오류"
            raise ValueError(f"API 오류 [{code}]: {msg}")

        items = root.findall(".//item")
        if not items:
            if page == 1:
                print(f"    ℹ  항목 0건 (API 검색명 확인 필요: '{search_name}')")
            break

        for node in items:
            g = lambda t: (node.findtext(t) or "").strip()
            yadm = g("yadmNm")
            if seen_names is not None and yadm:
                seen_names.add(yadm)
            if name_filter and yadm and _norm(yadm) != name_filter:
                continue
            raw = g("adtFrDd")
            date_str = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}" if len(raw) == 8 else (raw or "-")
            try:
                price = int(float(g("curAmt") or 0))
            except ValueError:
                price = 0

            result.append({
                "hospName" : display_name,
                "itemName" : (g("npayKorNm") or g("npayNm") or "명칭없음"),
                "price"    : price,
                "ediCode"  : g("npayCd") or "-",
                "date"     : date_str,
                "unit"     : g("npayUnit") or "-",
                "category" : g("npayClsfNm") or "-",
                "note"     : g("npayNote") or "-",   # 특이사항 (구분 단서)
            })

        print(f"    page {page:3d}: {len(items):4d}건 수집 (누적 {len(result):,}건)")

        if len(items) < cfg["chunk"]:
            break

        page += 1
        time.sleep(cfg["delay"])

    return result


# ═══════════════════════════════════════════════════════
#  검색명 진단 (데이터 0건일 때만 사용 · 어떤 오류도 밖으로 내보내지 않음)
# ═══════════════════════════════════════════════════════
_ENTITY_TOKENS = ("학교법인", "의료법인", "재단법인", "사단법인", "사회복지법인")
_SPACE_AFTER_TOKENS = ("대학교", "의과대학", "재단", "학원")   # 심평원 등록명에서 이 단어 뒤를 띄어쓰는 경우가 있음


def _get_xml(params: dict, attempts: int = 2):
    for attempt in range(attempts):
        try:
            resp = requests.get(CONFIG["base_url"], params=params, timeout=CONFIG["timeout"])
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            code = root.findtext(".//resultCode") or ""
            if code and code not in ("00", "0000"):
                return None
            return root
        except Exception:
            time.sleep(2 ** attempt)
    return None


def _probe_names(prefix: str, max_pages: int = 3) -> set:
    """prefix 로 조회해 응답에 나온 병원명(yadmNm) 모음 (최대 max_pages 페이지)"""
    names = set()
    for page in range(1, max_pages + 1):
        root = _get_xml({"ServiceKey": CONFIG["api_key"], "pageNo": page,
                         "numOfRows": CONFIG["chunk"], "yadmNm": prefix})
        if root is None:
            break
        items = root.findall(".//item")
        if not items:
            break
        for node in items:
            nm = (node.findtext("yadmNm") or "").strip()
            if nm:
                names.add(nm)
        if len(items) < CONFIG["chunk"]:
            break
        time.sleep(CONFIG["delay"])
    return names


def _probe_variants(search_name: str) -> list:
    n, nn = search_name.strip(), _norm(search_name)
    variants = [n[:10]]
    for t in _ENTITY_TOKENS:                       # '학교법인건양교육…' → '학교법인 건양교육'
        if nn.startswith(t):
            variants.append(f"{t} {nn[len(t):len(t) + 6]}")
            break
    for tok in _SPACE_AFTER_TOKENS:                # '가톨릭대학교성빈센트…' → '가톨릭대학교 성빈센트병원'
        i = nn.find(tok)
        if i >= 0 and i + len(tok) < len(nn):
            j = i + len(tok)
            variants.append(f"{nn[:j]} {nn[j:j + 7]}")
    variants.append(nn[:5])
    seen, out = set(), []
    for v in variants:
        if v and v not in seen:
            seen.add(v); out.append(v)
    return out


def diagnose_name(search_name: str):
    """
    반환: (exact, others)
      exact  : 공백만 다르고 글자는 완전히 같은 등록명 (있으면 자동 보정에 사용)
      others : 비슷한 등록명 후보 (최대 5개, 참고용)
    """
    try:
        target, found = _norm(search_name), set()
        for v in _probe_variants(search_name):
            found |= _probe_names(v)
            if any(_norm(x) == target for x in found):
                break
        exact  = sorted(x for x in found if _norm(x) == target)
        others = sorted((x for x in found if _norm(x) != target),
                        key=lambda x: -difflib.SequenceMatcher(None, _norm(x), target).ratio())[:5]
        return exact, others
    except Exception as e:
        print(f"    ℹ  검색명 진단 중 오류(무시): {e}")
        return [], []


def write_last_run(results: dict):
    """data/last_run.json 에 병원별 결과를 합쳐서 기록 (일부 병원만 수집해도 다른 병원 기록은 유지)"""
    if not results:
        return
    try:
        data = {"updated": "", "hospitals": {}}
        if os.path.exists(LAST_RUN_FILE):
            try:
                with open(LAST_RUN_FILE, encoding="utf-8") as f:
                    old = json.load(f)
                if isinstance(old.get("hospitals"), dict):
                    data["hospitals"] = old["hospitals"]
            except Exception:
                pass
        data["hospitals"].update(results)
        data["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        with open(LAST_RUN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
    except Exception as e:
        print(f"⚠  last_run.json 기록 실패(수집 결과에는 영향 없음): {e}")


# ═══════════════════════════════════════════════════════
#  저장 함수
# ═══════════════════════════════════════════════════════
def save_hospital(display_name: str, items: list, metadata: dict, extra: dict = None) -> dict:
    """JSON 저장 + metadata.json 업데이트"""
    out_dir   = CONFIG["output_dir"]
    safe_name = display_name.replace("/", "_")
    filepath  = os.path.join(out_dir, f"{safe_name}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, separators=(",", ":"))

    metadata[display_name] = {
        "updated"    : datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count"      : len(items),
        "filename"   : f"{safe_name}.json",
        "searchName" : HOSPITALS.get(display_name, ""),
    }
    for k, v in (extra or {}).items():        # searchName(자동 보정 시), matchedNames 만 덧붙임
        if v:
            metadata[display_name][k] = v
    _save_meta(metadata)
    return metadata


def _save_meta(metadata: dict):
    path = os.path.join(CONFIG["output_dir"], "metadata.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)


def build_all_data(metadata: dict) -> int:
    """개별 JSON → all_data.json 통합 (HTML에서 사용)"""
    out_dir  = CONFIG["output_dir"]
    combined = []
    for info in metadata.values():
        fp = os.path.join(out_dir, info["filename"])
        if os.path.exists(fp):
            with open(fp, encoding="utf-8") as f:
                combined.extend(json.load(f))

    out_path = os.path.join(out_dir, "all_data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, separators=(",", ":"))
    return len(combined)


# ═══════════════════════════════════════════════════════
#  메인
# ═══════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="상급종합병원 비급여 데이터 수집기")
    parser.add_argument("--force",    action="store_true", help="이미 수집된 병원도 재수집")
    parser.add_argument("--hospital", type=str,            help="특정 병원만 수집 (표시명 입력, '신규' = 미수집 병원만)")
    parser.add_argument("--list",     action="store_true", help="병원 목록 출력 후 종료")
    args = parser.parse_args()

    if args.list:
        print("\n수집 가능한 병원 목록:")
        for i, (name, info) in enumerate(HOSPITAL_INFO.items(), 1):
            tag = f"[{info.get('region', '-')}]" if info.get("region") else ""
            print(f"  {i:2d}. {name:15s} {tag:12s} →  {info['searchName']}")
        return

    os.makedirs(CONFIG["output_dir"], exist_ok=True)

    # 기존 메타데이터 로드
    meta_path = os.path.join(CONFIG["output_dir"], "metadata.json")
    metadata  = {}
    if os.path.exists(meta_path):
        with open(meta_path, encoding="utf-8") as f:
            metadata = json.load(f)

    # 수집 대상
    if args.hospital == "신규":
        targets = {n: s for n, s in HOSPITALS.items()
                   if n not in metadata or not os.path.exists(
                       os.path.join(CONFIG["output_dir"], f"{n.replace('/', '_')}.json"))}
        if not targets:
            print("ℹ  아직 수집하지 않은 병원이 없습니다 (모두 수집됨).")
            return
    elif args.hospital:
        if args.hospital not in HOSPITALS:
            print(f"❌ 알 수 없는 병원: '{args.hospital}'")
            print(f"   사용 가능한 표시명: {list(HOSPITALS.keys())}")
            return
        targets = {args.hospital: HOSPITALS[args.hospital]}
    else:
        targets = HOSPITALS

    # ── 수집 루프 ──────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  상급종합병원 비급여 데이터 수집기 V2.1")
    print(f"  대상: {len(targets)}개 병원")
    print(f"{'='*55}\n")

    success = 0
    failed  = []
    run     = {}      # 이번 실행의 병원별 결과 → last_run.json
    now     = lambda: datetime.now().strftime("%Y-%m-%d %H:%M")

    for display_name, search_name in targets.items():
        cache_path = os.path.join(CONFIG["output_dir"],
                                  f"{display_name.replace('/', '_')}.json")
        already    = os.path.exists(cache_path) and display_name in metadata

        if not args.force and already:
            info = metadata[display_name]
            print(f"⏭  {display_name}: 캐시 사용 ({info['updated']}, {info['count']:,}건)")
            success += 1
            continue

        print(f"\n▶ {display_name} 수집 시작")
        print(f"  검색명: {search_name}")

        try:
            seen        = set()
            used_search = search_name
            resolved    = None
            suggestions = []

            items = fetch_hospital(display_name, search_name, seen_names=seen)

            if not items:      # 0건 → 공백 차이 등으로 못 찾은 것인지 진단
                exact, others = diagnose_name(search_name)
                if exact:
                    print(f"  🔎 공백 표기만 다른 등록명 발견: '{exact[0]}' → 이 이름으로 재수집")
                    seen = set()
                    items = fetch_hospital(display_name, exact[0], seen_names=seen,
                                           name_filter=_norm(search_name))
                    used_search, resolved = exact[0], exact[0]
                else:
                    suggestions = others

            if items:
                metadata = save_hospital(display_name, items, metadata,
                                         extra={"searchName": used_search if resolved else "",
                                                "matchedNames": sorted(seen)})
                print(f"  ✅ 완료: {len(items):,}건 저장")
                success += 1
                entry = {"status": "ok", "count": len(items), "time": now(), "searchName": used_search}
                if resolved:
                    entry["note"] = (f"공백 표기 차이를 자동 보정했습니다. hospitals.json 의 searchName 을 "
                                     f"'{resolved}' 로 바꾸면 다음부터 바로 수집됩니다.")
                if len(seen) > 1 and not resolved:
                    entry["warning"] = "검색 결과에 서로 다른 병원명이 섞여 있습니다: " + ", ".join(sorted(seen))
                    print(f"  ⚠  {entry['warning']}")
                run[display_name] = entry
            else:
                print(f"  ⚠  데이터 없음 → 저장 스킵 (API 검색명 확인 필요)")
                for c in suggestions:
                    print(f"     후보: {c}")
                run[display_name] = {"status": "no_data", "time": now(), "searchName": search_name,
                                     "suggestions": suggestions,
                                     "note": "검색명으로 조회된 데이터가 없습니다. suggestions 가 있으면 그 이름을 hospitals.json 의 searchName 에 넣어 보세요."}
        except Exception as e:
            print(f"  ❌ 실패: {e}")
            failed.append(display_name)
            run[display_name] = {"status": "failed", "time": now(), "error": str(e)[:300],
                                 "keptCache": bool(already)}
            if already:
                print(f"  → 기존 캐시 유지")
                success += 1

    # ── 통합 파일 재생성 ────────────────────────────────
    print(f"\n{'='*55}")
    total = build_all_data(metadata)
    print(f"  all_data.json 생성 완료: 총 {total:,}건")
    print(f"  성공: {success}/{len(targets)}개 병원")
    print(f"{'='*55}\n")

    write_last_run(run)

    no_data = [n for n, r in run.items() if r["status"] == "no_data"]
    if no_data:
        print(f"⚠ 데이터 없음 ({len(no_data)}개): {', '.join(no_data)}  → data/last_run.json 의 suggestions 확인")
    if failed:
        print(f"⚠ 실패 병원 ({len(failed)}개): {', '.join(failed)}")
        if len(failed) == len(targets):
            print("❌ 모든 병원 수집 실패 — GitHub Actions를 실패로 처리합니다.")
            sys.exit(1)

    print("📊 병원별 현황:")
    for name, info in sorted(metadata.items()):
        print(f"   {name:15s}: {info['count']:6,}건  ({info['updated']})")

    print("\n✅ 완료! 이제 아래 명령으로 대시보드를 실행하세요:")
    print("   python -m http.server 8000")
    print("   → 브라우저에서 http://localhost:8000 접속\n")


if __name__ == "__main__":
    main()
