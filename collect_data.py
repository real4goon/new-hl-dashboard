#!/usr/bin/env python3
"""
상급종합병원 비급여 데이터 수집기 V1.0
────────────────────────────────────────────
사용법:
  python collect_data.py             → 미수집 병원만 수집
  python collect_data.py --force     → 전체 재수집
  python collect_data.py --hospital 서울대병원  → 특정 병원만
  python collect_data.py --list      → 병원 목록 출력
"""

import requests
import json
import os
import time
import argparse
from xml.etree import ElementTree as ET
from datetime import datetime

# ═══════════════════════════════════════════════════════
#  설정 (필요 시 수정)
# ═══════════════════════════════════════════════════════
CONFIG = {
    "api_key"   : "0412d9d9b5df0a9504c7e3712efa9c123d1639149c78fac71fd43121f7227b23",
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
HOSPITALS = {
    # ── 서울 ──────────────────────────────────────────
    "서울대병원"     : "서울대학교병원",
    "삼성서울병원"   : "삼성서울병원",
    "서울아산병원"   : "재단법인아산사회복지재단 서울아산병원",
    "세브란스병원"   : "연세대학교의과대학세브란스병원",
    "강남세브란스"   : "강남세브란스병원",
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

# ═══════════════════════════════════════════════════════
#  수집 함수
# ═══════════════════════════════════════════════════════
def fetch_hospital(display_name: str, search_name: str) -> list:
    """단일 병원 전 페이지 수집 → 레코드 리스트 반환"""
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
            })

        print(f"    page {page:3d}: {len(items):4d}건 수집 (누적 {len(result):,}건)")

        if len(items) < cfg["chunk"]:
            break

        page += 1
        time.sleep(cfg["delay"])

    return result


# ═══════════════════════════════════════════════════════
#  저장 함수
# ═══════════════════════════════════════════════════════
def save_hospital(display_name: str, items: list, metadata: dict) -> dict:
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
    parser.add_argument("--hospital", type=str,            help="특정 병원만 수집 (표시명 입력)")
    parser.add_argument("--list",     action="store_true", help="병원 목록 출력 후 종료")
    args = parser.parse_args()

    if args.list:
        print("\n수집 가능한 병원 목록:")
        for i, (name, search) in enumerate(HOSPITALS.items(), 1):
            print(f"  {i:2d}. {name:15s}  →  {search}")
        return

    os.makedirs(CONFIG["output_dir"], exist_ok=True)

    # 기존 메타데이터 로드
    meta_path = os.path.join(CONFIG["output_dir"], "metadata.json")
    metadata  = {}
    if os.path.exists(meta_path):
        with open(meta_path, encoding="utf-8") as f:
            metadata = json.load(f)

    # 수집 대상
    if args.hospital:
        if args.hospital not in HOSPITALS:
            print(f"❌ 알 수 없는 병원: '{args.hospital}'")
            print(f"   사용 가능한 표시명: {list(HOSPITALS.keys())}")
            return
        targets = {args.hospital: HOSPITALS[args.hospital]}
    else:
        targets = HOSPITALS

    # ── 수집 루프 ──────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  상급종합병원 비급여 데이터 수집기 V1.0")
    print(f"  대상: {len(targets)}개 병원")
    print(f"{'='*55}\n")

    success = 0
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
            items = fetch_hospital(display_name, search_name)
            if items:
                metadata = save_hospital(display_name, items, metadata)
                print(f"  ✅ 완료: {len(items):,}건 저장")
                success += 1
            else:
                print(f"  ⚠  데이터 없음 → 저장 스킵 (API 검색명 확인 필요)")
        except Exception as e:
            print(f"  ❌ 실패: {e}")
            if already:
                print(f"  → 기존 캐시 유지")
                success += 1

    # ── 통합 파일 재생성 ────────────────────────────────
    print(f"\n{'='*55}")
    total = build_all_data(metadata)
    print(f"  all_data.json 생성 완료: 총 {total:,}건")
    print(f"  성공: {success}/{len(targets)}개 병원")
    print(f"{'='*55}\n")

    print("📊 병원별 현황:")
    for name, info in sorted(metadata.items()):
        print(f"   {name:15s}: {info['count']:6,}건  ({info['updated']})")

    print("\n✅ 완료! 이제 아래 명령으로 대시보드를 실행하세요:")
    print("   python -m http.server 8000")
    print("   → 브라우저에서 http://localhost:8000 접속\n")


if __name__ == "__main__":
    main()
