# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from io import BytesIO

# 페이지 설정
st.set_page_config(page_title="조달청 납품요구상세", layout="wide")
st.title("🏛️ 조달청 종합쇼핑몰 납품요구상세 현황 (누적 수집)")

BASE_URL = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getDlvrReqDtlInfoList"

# --- 컬럼 매핑 사전 ---
COLUMN_MAP = {
    "dlvrReqNo": "납품요구번호", "dlvrReqChngNo": "납품요구변경번호", "dlvrReqDate": "납품요구일자",
    "ordrrNm": "주문기관명", "ordrrDivNm": "주문기관구분", "corpNm": "업체명",
    "corpEntrprsDivNmNm": "기업구분", "prdctClsfcNo": "물품분류번호", "prdctClsfcNoNm": "물품분류명",
    "prdctIdntNo": "물품식별번호", "prdctNm": "품명", "prdctSpecNm": "규격",
    "prdctUnit": "단위", "prdctQty": "수량", "prdctPrc": "단가", "prdctAmt": "금액",
    "dlvrDate": "납품기한", "dlvrPlcNm": "납품장소", "cntrctNo": "계약번호", "mainPrdctNm": "대표품명"
}

# --- 세션 상태 초기화 (누적 데이터 보관용) ---
if 'master_data' not in st.session_state:
    st.session_state['master_data'] = pd.DataFrame()
if 'loaded_dates' not in st.session_state:
    st.session_state['loaded_dates'] = set()  # 이미 불러온 날짜들 저장

# -----------------------------------------
# API 관련 함수
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
    st.header("⚙️ 데이터 수집 설정")
    api_key = st.text_input("API 인증키 (Encoding)", type="password")
    today = datetime.today()
    start = st.date_input("시작일", today - timedelta(days=7))
    end = st.date_input("종료일", today - timedelta(days=1))
    target_product = st.text_input("물품분류명 (API 수집 필터)")

    col1, col2 = st.columns(2)
    search_button = col1.button("📡 데이터 불러오기", use_container_width=True, type="primary")
    reset_button = col2.button("🗑️ 초기화", use_container_width=True)

    if reset_button:
        st.session_state['master_data'] = pd.DataFrame()
        st.session_state['loaded_dates'] = set()
        st.rerun()

    st.markdown("---")
    st.subheader("🎯 실시간 결과 필터")
    corp_filter = st.text_input("업체명 필터")
    div_filter = st.text_input("기업구분명 필터")

# -----------------------------------------
# 1. 누적 데이터 수집 로직
# -----------------------------------------
if search_button:
    if not api_key:
        st.error("⚠️ API 인증키를 입력해주세요.")
    else:
        try:
            # 설정한 기간 중 아직 불러오지 않은 날짜 리스트 생성
            requested_dates = [start + timedelta(days=x) for x in range((end - start).days + 1)]
            new_dates = [d for d in requested_dates if d not in st.session_state['loaded_dates']]

            if not new_dates:
                st.info("ℹ️ 해당 기간의 데이터는 이미 모두 불러온 상태입니다.")
            else:
                all_new_data = []
                progress_bar = st.progress(0)
                status_text = st.empty()

                for i, current_date in enumerate(new_dates):
                    date_str = current_date.strftime("%Y%m%d")
                    status_text.text(f"🌐 새 데이터 수집 중: {date_str} ({i+1}/{len(new_dates)})")
                    
                    page = 1
                    while True:
                        params = {
                            "ServiceKey": api_key, "pageNo": page, "numOfRows": 100,
                            "type": "json", "inqryDiv": "1", "inqryBgnDate": date_str, "inqryEndDate": date_str,
                        }
                        if target_product.strip():
                            params["prdctClsfcNoNm"] = target_product.strip()

                        data = fetch_api(params)
                        df_tmp = parse(data)

                        if df_tmp.empty: break
                        all_new_data.append(df_tmp)
                        if len(df_tmp) < 100: break
                        page += 1
                        time.sleep(0.2)
                    
                    st.session_state['loaded_dates'].add(current_date)
                    progress_bar.progress((i + 1) / len(new_dates))

                status_text.empty()
                progress_bar.empty()

                if all_new_data:
                    new_df = pd.concat(all_new_data, ignore_index=True)
                    # 기존 데이터와 새 데이터 합치기
                    combined = pd.concat([st.session_state['master_data'], new_df], ignore_index=True)
                    
                    # 중복 제거 및 최신 차수 유지 로직 (API 원본 컬럼명 기준)
                    if "prdctAmt" in combined.columns:
                        combined["prdctAmt"] = pd.to_numeric(combined["prdctAmt"], errors="coerce").fillna(0)
                    if "dlvrReqChngNo" in combined.columns:
                        combined["dlvrReqChngNo"] = pd.to_numeric(combined["dlvrReqChngNo"], errors="coerce").fillna(0)
                    else:
                        combined["dlvrReqChngNo"] = 0

                    if "dlvrReqNo" in combined.columns:
                        # 번호와 차수 기준으로 정렬 후 중복 제거
                        combined = combined.sort_values(by=["dlvrReqNo", "dlvrReqChngNo"], ascending=[True, False])
                        combined = combined.drop_duplicates(subset=["dlvrReqNo"], keep="first")
                    
                    st.session_state['master_data'] = combined
                    st.success(f"✅ 새롭게 {len(new_dates)}일치 데이터를 추가 수집했습니다.")

        except Exception as e:
            st.error(f"❌ 오류 발생: {e}")



# -----------------------------------------
# 2. 화면 표시 (필터링 및 분석)
# -----------------------------------------
if not st.session_state['master_data'].empty:
    # 원본 복사 (세션 데이터 보호)
    raw_df = st.session_state['master_data'].copy()
    
    # 🛡️ 1단계: 날짜 컬럼 이름 찾기 (영문 혹은 한글 중 존재하는 것 확인)
    # API 원본은 dlvrReqDate, 변환 후는 납품요구일자
    date_col = None
    for col in ['납품요구일자', 'dlvrReqDate']:
        if col in raw_df.columns:
            date_col = col
            break
            
    # 🛡️ 2단계: 날짜 컬럼이 있을 때만 날짜 형식 변환 및 범위 필터링 진행
    if date_col:
        raw_df[f'{date_col}_dt'] = pd.to_datetime(raw_df[date_col], errors='coerce')
        # 현재 사이드바에서 선택된 시작일/종료일 범위로 필터링
        mask = (raw_df[f'{date_col}_dt'].dt.date >= start) & (raw_df[f'{date_col}_dt'].dt.date <= end)
        df = raw_df[mask].copy()
        
        # 표시용으로 한글 컬럼명 변환 적용
        df = df.rename(columns=COLUMN_MAP)
    else:
        # 날짜 컬럼이 없는 특수 상황 대비
        df = raw_df.rename(columns=COLUMN_MAP)

    # 🛡️ 3단계: 텍스트 필터 적용 (컬럼 존재 확인 후 실행)
    if corp_filter.strip():
        target_col = "업체명" if "업체명" in df.columns else "corpNm"
        if target_col in df.columns:
            df = df[df[target_col].astype(str).str.contains(corp_filter.strip(), case=False, na=False)]
            
    if div_filter.strip():
        target_col = "기업구분" if "기업구분" in df.columns else "corpEntrprsDivNmNm"
        if target_col in df.columns:
            df = df[df[target_col].astype(str).str.contains(div_filter.strip(), case=False, na=False)]

    # --- 화면 출력부 ---
    st.info(f"📅 현재 화면 표시 기간: {start} ~ {end} (조회된 결과: {len(df):,}건)")
    
    if df.empty:
        st.warning("⚠️ 선택한 기간 및 필터 조건에 해당하는 데이터가 없습니다.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("표시 건수", f"{len(df):,} 건")
        
        # 금액 컬럼 합계 (한글/영문 모두 대응)
        amt_col = "금액" if "금액" in df.columns else "prdctAmt"
        if amt_col in df.columns:
            total_amt = pd.to_numeric(df[amt_col], errors='coerce').sum()
            c2.metric("표시 총액", f"{total_amt:,.0f} 원")
        
        c3.metric("누적 수집 날짜 수", f"{len(st.session_state['loaded_dates'])} 일")

        st.markdown("---")
        
        # 업체별 합계
        corp_col = "업체명" if "업체명" in df.columns else "corpNm"
        if corp_col in df.columns and amt_col in df.columns:
            st.subheader("🏢 업체별 합계")
            summary = df.groupby(corp_col)[amt_col].agg(['sum', 'count']).reset_index()
            summary.columns = ["업체명", "합계금액", "건수"]
            st.dataframe(summary.sort_values("합계금액", ascending=False).style.format({"합계금액": "{:,.0f}원", "건수": "{:,}건"}), use_container_width=True)

        # 상세 리스트 (불필요한 내부 처리용 날짜 컬럼은 제외하고 표시)
        st.subheader("📋 상세 리스트")
        display_cols = [c for c in df.columns if not c.endswith('_dt')]
        st.dataframe(df[display_cols], use_container_width=True)

        # 엑셀 다운로드
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df[display_cols].to_excel(writer, sheet_name="상세내역", index=False)
            if 'summary' in locals():
                summary.to_excel(writer, sheet_name="업체별합계", index=False)
        output.seek(0)
        st.download_button(label="📊 엑셀 다운로드", data=output, file_name=f"조회결과_{datetime.now().strftime('%H%M%S')}.xlsx", use_container_width=True)

else:
    st.info("💡 데이터를 불러오면 여기에 표시됩니다. 사이드바의 [데이터 불러오기] 버튼을 눌러주세요.")