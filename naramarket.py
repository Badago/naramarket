# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from io import BytesIO

# 페이지 설정
st.set_page_config(page_title="조달청 납품요구상세", layout="wide")
st.title("🏛️ 조달청 종합쇼핑몰 납품요구상세 현황")

BASE_URL = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getDlvrReqDtlInfoList"

# --- 영문 컬럼명을 한글로 변환하기 위한 매핑 사전 ---
COLUMN_MAP = {
    "dlvrReqNo": "납품요구번호",
    "dlvrReqChngNo": "납품요구변경번호",
    "dlvrReqDate": "납품요구일자",
    "ordrrNm": "주문기관명",
    "ordrrDivNm": "주문기관구분",
    "corpNm": "업체명",
    "corpEntrprsDivNmNm": "기업구분",
    "prdctClsfcNo": "물품분류번호",
    "prdctClsfcNoNm": "물품분류명",
    "prdctIdntNo": "물품식별번호",
    "prdctNm": "품명",
    "prdctSpecNm": "규격",
    "prdctUnit": "단위",
    "prdctQty": "수량",
    "prdctPrc": "단가",
    "prdctAmt": "금액",
    "dlvrDate": "납품기한",
    "dlvrPlcNm": "납품장소",
    "cntrctNo": "계약번호",
    "mainPrdctNm": "대표품명"
}

# --- 세션 상태 초기화 ---
if 'raw_data' not in st.session_state:
    st.session_state['raw_data'] = None
if 'query_log' not in st.session_state:
    st.session_state['query_log'] = ""

# -----------------------------------------
# API 관련 함수들
# -----------------------------------------
def fetch_api(params, max_retry=5):
    for attempt in range(max_retry):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            if response.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == max_retry - 1: raise e
            time.sleep(1)

def parse(data):
    body = data.get("response", {}).get("body", {})
    items = body.get("items", [])
    if isinstance(items, dict): items = [items]
    return pd.DataFrame(items)

# -----------------------------------------
# 사이드바 설정
# -----------------------------------------
with st.sidebar:
    st.header("⚙️ 1. 데이터 수집 설정")
    api_key = st.text_input("API 인증키 (Encoding)", type="password")
    today = datetime.today()
    start = st.date_input("시작일", today - timedelta(days=7))
    end = st.date_input("종료일", today - timedelta(days=1))
    target_product = st.text_input("물품분류명 (API 수집 필터)")

    search_button = st.button("📡 조건에 맞춰 데이터 불러오기", use_container_width=True, type="primary")

    st.markdown("---")
    st.subheader("🎯 2. 수집된 결과 내 필터")
    corp_filter = st.text_input("업체명 필터 (실시간)")
    div_filter = st.text_input("기업구분명 필터 (실시간)", help="예: 중소기업, 중견기업, 대기업 등")

# -----------------------------------------
# 1. API 데이터 수집 (버튼 클릭 시)
# -----------------------------------------
if search_button:
    if not api_key:
        st.error("⚠️ API 인증키를 입력해주세요.")
    else:
        try:
            all_data = []
            total_days = (end - start).days + 1
            progress_bar = st.progress(0)
            status_text = st.empty()

            NUM_OF_ROWS = 100 

            for i in range(total_days):
                current_date = start + timedelta(days=i)
                date_str = current_date.strftime("%Y%m%d")
                page = 1
                status_text.text(f"📅 {date_str} 데이터 요청 중...")

                while True:
                    params = {
                        "ServiceKey": api_key, "pageNo": page, "numOfRows": NUM_OF_ROWS,
                        "type": "json", "inqryDiv": "1", "inqryBgnDate": date_str, "inqryEndDate": date_str,
                    }
                    if target_product.strip():
                        params["prdctClsfcNoNm"] = target_product.strip()

                    data = fetch_api(params)
                    df_tmp = parse(data)

                    if df_tmp.empty: break
                    all_data.append(df_tmp)
                    if len(df_tmp) < NUM_OF_ROWS: break
                    page += 1
                    time.sleep(0.2)

                progress_bar.progress((i + 1) / total_days)

            status_text.empty()
            progress_bar.empty()

            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # 금액 데이터 수치 변환
                if "prdctAmt" in combined_df.columns:
                    combined_df["prdctAmt"] = pd.to_numeric(combined_df["prdctAmt"], errors="coerce").fillna(0)
                
                # 🔥 컬럼명 한글 변환 적용
                combined_df = combined_df.rename(columns=COLUMN_MAP)
                
                st.session_state['raw_data'] = combined_df
                st.session_state['query_log'] = f"{start} ~ {end} (품목: {target_product if target_product else '전체'})"
                st.success(f"✅ 수집 및 한글 변환 완료!")
            else:
                st.warning("⚠️ 해당 조건으로 검색된 데이터가 없습니다.")
                st.session_state['raw_data'] = None

        except Exception as e:
            st.error(f"❌ 오류: {e}")

# -----------------------------------------
# 2. 실시간 다중 필터 및 표시
# -----------------------------------------
if st.session_state['raw_data'] is not None:
    df = st.session_state['raw_data'].copy()

    # 업체명 필터 (이미 한글로 변환된 상태이므로 한글 컬럼명 사용)
    if corp_filter.strip():
        col = "업체명" if "업체명" in df.columns else "corpNm"
        df = df[df[col].astype(str).str.contains(corp_filter.strip(), case=False, na=False)]
    
    # 기업구분명 필터
    if div_filter.strip():
        col = "기업구분" if "기업구분" in df.columns else "corpEntrprsDivNmNm"
        df = df[df[col].astype(str).str.contains(div_filter.strip(), case=False, na=False)]

    st.info(f"📋 수집 기준: {st.session_state['query_log']}")
    
    # 메트릭
    c1, c2, c3 = st.columns(3)
    c1.metric("수집된 총 건수", f"{len(st.session_state['raw_data']):,} 건")
    c2.metric("현재 필터 결과", f"{len(df):,} 건")
    amt_col = "금액" if "금액" in df.columns else "prdctAmt"
    if amt_col in df.columns:
        c3.metric("표시 총액", f"{df[amt_col].sum():,.0f} 원")

    # 업체별 합계
    st.markdown("---")
    st.subheader("🏢 업체별 합계 (필터 결과)")
    corp_col = "업체명" if "업체명" in df.columns else "corpNm"
    if not df.empty and corp_col in df.columns:
        summary = df.groupby(corp_col)[amt_col].agg(['sum', 'count']).reset_index()
        summary.columns = ["업체명", "합계금액", "건수"]
        summary = summary.sort_values("합계금액", ascending=False)
        st.dataframe(summary.style.format({"합계금액": "{:,.0f}원", "건수": "{:,}건"}), use_container_width=True)

    # 상세 내역
    st.subheader("📋 상세 리스트")
    st.dataframe(df, use_container_width=True)

    # 엑셀 다운로드
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="상세내역", index=False)
        if 'summary' in locals():
            summary.to_excel(writer, sheet_name="업체별합계", index=False)
    output.seek(0)
    st.download_button(label="📊 한글 엑셀 다운로드", data=output, 
                       file_name=f"조달청_한글결과_{datetime.now().strftime('%H%M%S')}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)