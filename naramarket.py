# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from io import BytesIO

# =========================================================
# 🛠️ [사용자 설정] 표 크기 및 컬럼 너비 조절 (픽셀 단위)
# =========================================================
# 1. 표 전체 높이
SUMMARY_TABLE_HEIGHT = 400
DETAIL_TABLE_HEIGHT =400

# 2. 업체별 합계 표 컬럼 너비
SUMMARY_WIDTHS = {
    "순위": 60,
    "업체명": 250,
    "총 합계금액": 150,
    "우수제품 합계": 150,
    "MAS 합계": 150,
    "건수": 80
}

# 3. 상세 리스트 표 컬럼 너비
DETAIL_WIDTHS = {
    "납품요구번호": 130,
    "납품요구변경번호": 80,
    "납품요구일자": 110,
    "납품요구접수일자": 110,
    "품목일련번호": 100,
    "업체명": 200,
    "주문기관명": 150,
    "수요기관명": 150,
    "세부품명": 180,
    "품명": 180,
    "규격": 200,
    "수량": 80,
    "단가": 120,
    "금액": 150,
    "납품기한": 110,
    "납품장소": 200
}
# =========================================================

CACHE_FILE = "naramarket_cache.csv"
DATES_FILE = "loaded_dates.txt"

st.set_page_config(page_title="조달청 납품요구상세", layout="wide")
st.title("🏛️ 조달청 종합쇼핑몰 납품요구상세 현황")

BASE_URL = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getDlvrReqDtlInfoList"

COLUMN_MAP = {
    "dlvrReqNo": "납품요구번호", "dlvrReqChngNo": "납품요구변경번호", "dlvrReqDate": "납품요구일자",
    "dlvrReqRcptDate": "납품요구접수일자", "prdctSno": "품목일련번호", "ordrrNm": "주문기관명", 
    "ordrrDivNm": "주문기관구분", "corpNm": "업체명", "corpEntrprsDivNmNm": "기업구분", 
    "prdctClsfcNo": "물품분류번호", "prdctClsfcNoNm": "물품분류명", "prdctIdntNo": "물품식별번호", 
    "prdctIdntNoNm": "세부품명", "prdctNm": "품명", "prdctSpecNm": "규격", "prdctUnit": "단위", 
    "prdctQty": "수량", "prdctPrc": "단가", "prdctAmt": "금액", "dlvrDate": "납품기한", 
    "dlvrPlcNm": "납품장소", "cntrctNo": "계약번호", "mainPrdctNm": "대표품명", 
    "exclcProdctYn": "우수제품여부", "masYn": "MAS여부", "dminsttNm": "수요기관명"
}

REVERSE_MAP = {v: k for k, v in COLUMN_MAP.items()}

def save_data(df_to_save, dates_to_save):
    df_to_save.to_csv(CACHE_FILE, index=False, encoding='utf-8-sig')
    with open(DATES_FILE, "w", encoding="utf-8") as f:
        for d in sorted(list(dates_to_save)):
            f.write(d.strftime("%Y-%m-%d") + "\n")

def load_data():
    loaded_df, loaded_dates = pd.DataFrame(), set()
    if os.path.exists(CACHE_FILE) and os.path.getsize(CACHE_FILE) > 0:
        try: loaded_df = pd.read_csv(CACHE_FILE, encoding='utf-8-sig')
        except: pass
    if os.path.exists(DATES_FILE):
        with open(DATES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try: loaded_dates.add(datetime.strptime(line.strip(), "%Y-%m-%d").date())
                except: continue
    return loaded_df, loaded_dates

if 'master_data' not in st.session_state:
    st.session_state['master_data'], st.session_state['loaded_dates'] = load_data()

def fetch_api(params, max_retry=5):
    for attempt in range(max_retry):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            if response.status_code == 429:
                time.sleep(2 * (attempt + 1)); continue
            response.raise_for_status()
            return response.json()
        except:
            if attempt == max_retry - 1: raise
            time.sleep(1)

def parse(api_data):
    items = api_data.get("response", {}).get("body", {}).get("items", [])
    if isinstance(items, dict): items = [items]
    return pd.DataFrame(items)

with st.sidebar:
    st.header("⚙️ 데이터 수집 설정")
    api_key = st.text_input("API 인증키 (Encoding)", type="password")
    today = datetime.today().date()
    start = st.date_input("조회 시작일", today - timedelta(days=7))
    end = st.date_input("조회 종료일", today - timedelta(days=1))
    target_product = st.text_input("물품분류명 (API 수집 필터)")

    c1, c2 = st.columns(2)
    if c1.button("📡 데이터 불러오기", use_container_width=True, type="primary"):
        if not api_key: st.error("⚠️ API 인증키를 입력해주세요.")
        else:
            try:
                requested_dates = [start + timedelta(days=x) for x in range((end - start).days + 1)]
                new_dates = [d for d in requested_dates if d not in st.session_state['loaded_dates']]
                if not new_dates: st.info("ℹ️ 이미 수집된 기간입니다.")
                else:
                    all_new = []
                    prog, status = st.progress(0), st.empty()
                    for i, cur_date in enumerate(new_dates):
                        d_str = cur_date.strftime("%Y%m%d")
                        status.text(f"🌐 수집 중: {d_str}")
                        page = 1
                        while True:
                            p = {"ServiceKey": api_key, "pageNo": page, "numOfRows": 100, "type": "json", "inqryDiv": "1", "inqryBgnDate": d_str, "inqryEndDate": d_str}
                            if target_product.strip(): p["prdctClsfcNoNm"] = target_product.strip()
                            data = fetch_api(p)
                            df_tmp = parse(data)
                            if df_tmp.empty: break
                            all_new.append(df_tmp)
                            if len(df_tmp) < 100: break
                            page += 1; time.sleep(0.2)
                        st.session_state['loaded_dates'].add(cur_date)
                        prog.progress((i + 1) / len(new_dates))
                    if all_new:
                        new_df = pd.concat(all_new, ignore_index=True)
                        combined = pd.concat([st.session_state['master_data'], new_df], ignore_index=True)
                        st.session_state['master_data'] = combined
                        save_data(combined, st.session_state['loaded_dates'])
                        st.rerun()
            except Exception as e: st.error(f"❌ 오류: {e}")

    if c2.button("🗑️ 초기화", use_container_width=True):
        for f in [CACHE_FILE, DATES_FILE]:
            if os.path.exists(f): os.remove(f)
        st.session_state['master_data'], st.session_state['loaded_dates'] = pd.DataFrame(), set()
        st.rerun()

    st.markdown("---")
    corp_filter = st.text_input("업체명 필터")
    div_filter = st.text_input("기업구분명 필터")
    inst_filter = st.text_input("수요기관명 필터")

if not st.session_state['master_data'].empty:
    df = st.session_state['master_data'].copy()
    for kor, eng in REVERSE_MAP.items():
        if kor in df.columns and eng not in df.columns: df[eng] = df[kor]

    for col in ["prdctAmt", "dlvrReqChngNo", "prdctSno", "prdctPrc", "prdctQty"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if 'dlvrReqNo' in df.columns:
        curr_year_short = str(datetime.now().year)[2:4]
        df['no_year_code'] = df['dlvrReqNo'].astype(str).str[1:3]
        df = df[df['no_year_code'] == curr_year_short].copy()

    if not df.empty and 'dlvrReqDate' in df.columns:
        df['dt_temp'] = pd.to_datetime(df['dlvrReqDate'].astype(str).str.replace("-",""), format='%Y%m%d', errors='coerce')
        df = df.sort_values(['dlvrReqNo', 'prdctSno', 'dlvrReqChngNo'], ascending=[True, True, False])
        df = df.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first')
        df = df[(df['dt_temp'].dt.date >= start) & (df['dt_temp'].dt.date <= end)]
    
    df_view = df.rename(columns=COLUMN_MAP)
    
    if corp_filter.strip(): df_view = df_view[df_view["업체명"].astype(str).str.contains(corp_filter.strip(), case=False, na=False)]
    if div_filter.strip() and "기업구분" in df_view.columns: df_view = df_view[df_view["기업구분"].astype(str).str.contains(div_filter.strip(), case=False, na=False)]
    if inst_filter.strip():
        inst_col = "수요기관명" if "수요기관명" in df_view.columns else "주문기관명"
        if inst_col in df_view.columns: df_view = df_view[df_view[inst_col].astype(str).str.contains(inst_filter.strip(), case=False, na=False)]

    st.info(f"📅 조회 기간: {start} ~ {end} (총 {len(df_view):,}건)")
    
    if not df_view.empty:
        # --- 업체별 합계 ---
        st.subheader("🏢 업체별 금액 합계")
        df_view["우수금액"] = df_view.apply(lambda x: x["금액"] if str(x.get("우수제품여부", "N")).upper() == "Y" else 0, axis=1)
        df_view["MAS금액"] = df_view.apply(lambda x: x["금액"] if str(x.get("MAS여부", "N")).upper() == "Y" else 0, axis=1)
        summary = df_view.groupby("업체명").agg({"금액": "sum", "우수금액": "sum", "MAS금액": "sum", "납품요구번호": "count"}).reset_index()
        summary.columns = ["업체명", "총 합계금액", "우수제품 합계", "MAS 합계", "건수"]
        summary = summary.sort_values("총 합계금액", ascending=False).reset_index(drop=True)
        summary.insert(0, "순위", summary.index + 1)
        
        # 합계 표 출력
        st.dataframe(
            summary.style.format({"총 합계금액": "{:,.0f}원", "우수제품 합계": "{:,.0f}원", "MAS 합계": "{:,.0f}원", "건수": "{:,}건"}), 
            use_container_width=True, hide_index=True, height=SUMMARY_TABLE_HEIGHT,
            column_config={k: st.column_config.Column(width=v) for k, v in SUMMARY_WIDTHS.items()}
        )

        # --- 상세 리스트 ---
        st.subheader("📋 상세 리스트")
        disp_cols = ["납품요구번호", "납품요구변경번호", "납품요구일자", "납품요구접수일자", "품목일련번호", "업체명", "주문기관명", "수요기관명", "세부품명", "품명", "규격", "수량", "단가", "금액", "납품기한", "납품장소"]
        final_disp = [c for c in disp_cols if c in df_view.columns]
        fmt_dict = {c: "{:,.0f}" for c in ["금액", "단가", "수량", "품목일련번호", "납품요구변경번호"] if c in final_disp}
        
        # 상세 표 출력
        st.dataframe(
            df_view[final_disp].style.format(fmt_dict, na_rep="-"), 
            use_container_width=True, hide_index=True, height=DETAIL_TABLE_HEIGHT,
            column_config={k: st.column_config.Column(width=v) for k, v in DETAIL_WIDTHS.items()}
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_view[final_disp].to_excel(writer, sheet_name="상세내역", index=False)
            summary.to_excel(writer, sheet_name="업체별합계", index=False)
        st.download_button(label="📊 엑셀 다운로드", data=output.getvalue(), file_name=f"조달현황_{datetime.now().strftime('%H%M%S')}.xlsx", use_container_width=True)
else:
    st.info("💡 사이드바에서 [데이터 불러오기]를 실행해 주세요.")