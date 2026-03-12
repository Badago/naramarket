# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import time
import os
import re
import glob
from datetime import datetime, timedelta
from io import BytesIO

# =========================================================
# 🛠️ [사용자 설정] 표 크기 및 컬럼 너비 조절
# =========================================================
SUMMARY_TABLE_HEIGHT = 400
DETAIL_TABLE_HEIGHT = 400

SUMMARY_WIDTHS = {
    "순위": 60, "업체명": 250, "총 합계금액": 150, "우수제품 합계": 150, "MAS 합계": 150, "건수": 80
}

DETAIL_WIDTHS = {
    "납품요구번호": 130, "납품요구변경번호": 80, "납품요구일자": 110, "납품요구접수일자": 110,
    "품목일련번호": 100, "업체명": 200, "주문기관명": 150, "수요기관명": 150,
    "세부품명": 180, "품명": 180, "규격": 200, "수량": 80, "단가": 120, "금액": 150,
    "납품기한": 110, "납품장소": 200
}
# =========================================================

st.set_page_config(page_title="조달청 납품요구상세", layout="wide")
st.title("🏛️ 조달청 종합쇼핑몰 납품요구상세 현황")

BASE_URL = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getDlvrReqDtlInfoList"

COLUMN_MAP = {
    "dlvrReqNo": "납품요구번호", "dlvrReqChngNo": "납품요구변경번호", "dlvrReqDate": "납품요구일자",
    "dlvrReqRcptDate": "납품요구접수일자", "prdctSno": "품목일련번호", "ordrrNm": "주문기관명", 
    "corpNm": "업체명", "corpEntrprsDivNmNm": "기업구분", 
    "prdctClsfcNo": "물품분류번호", "prdctClsfcNoNm": "물품분류명", 
    "prdctIdntNo": "물품식별번호", "prdctIdntNoNm": "세부품명", "prdctNm": "품명", 
    "prdctSpecNm": "규격", "prdctQty": "수량", "prdctPrc": "단가", "prdctAmt": "금액", 
    "dlvrDate": "납품기한", "dlvrPlcNm": "납품장소", "exclcProdctYn": "우수제품여부",
    "masYn": "MAS여부", "dminsttNm": "수요기관명"
}

REVERSE_MAP = {v: k for k, v in COLUMN_MAP.items()}

def get_safe_filename(name):
    if not name: return "all"
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()

def save_data_by_key(key, df, dates):
    safe_key = get_safe_filename(key)
    df.to_csv(f"data_{safe_key}.csv", index=False, encoding='utf-8-sig')
    with open(f"dates_{safe_key}.txt", "w", encoding="utf-8") as f:
        for d in sorted(list(dates)):
            f.write(d.strftime("%Y-%m-%d") + "\n")

def load_data_by_key(key):
    safe_key = get_safe_filename(key)
    df_file = f"data_{safe_key}.csv"
    date_file = f"dates_{safe_key}.txt"
    loaded_df, loaded_dates = pd.DataFrame(), set()
    if os.path.exists(df_file) and os.path.getsize(df_file) > 0:
        try: loaded_df = pd.read_csv(df_file, encoding='utf-8-sig')
        except: pass
    if os.path.exists(date_file):
        with open(date_file, "r", encoding="utf-8") as f:
            for line in f:
                try: loaded_dates.add(datetime.strptime(line.strip(), "%Y-%m-%d").date())
                except: continue
    return loaded_df, loaded_dates

# 모든 로컬 파일 통합 로드 함수 (에러 방지 보완 버전)
def load_all_combined_data():
    all_files = glob.glob("data_*.csv")
    if not all_files: return pd.DataFrame()
    
    df_list = []
    for f in all_files:
        try: 
            tmp_df = pd.read_csv(f, encoding='utf-8-sig')
            if not tmp_df.empty:
                df_list.append(tmp_df)
        except: continue
        
    if not df_list: return pd.DataFrame()
    
    combined = pd.concat(df_list, ignore_index=True)
    
    # [수정] 컬럼 존재 여부를 확인하여 안전하게 정렬 및 중복 제거
    sort_cols = []
    # 정렬에 필요한 컬럼이 있는지 확인
    for col in ['dlvrReqNo', 'prdctSno', 'dlvrReqChngNo']:
        if col in combined.columns:
            sort_cols.append(col)
            
    if sort_cols:
        # 변경번호(dlvrReqChngNo)가 있다면 내림차순 정렬하여 최신본이 위로 오게 함
        if 'dlvrReqChngNo' in combined.columns:
            combined = combined.sort_values('dlvrReqChngNo', ascending=False)
        
        # 필수 키(번호, 일련번호)가 있다면 중복 제거
        subset_cols = [c for c in ['dlvrReqNo', 'prdctSno'] if c in combined.columns]
        if subset_cols:
            combined = combined.drop_duplicates(subset=subset_cols, keep='first')
            
    return combined

# -----------------------------------------
# 사이드바
# -----------------------------------------
with st.sidebar:
    st.header("⚙️ 데이터 수집 및 통합")
    api_key = st.text_input("API 인증키 (Encoding)", type="password")
    today = datetime.today().date()
    start = st.date_input("조회 시작일", today - timedelta(days=7))
    end = st.date_input("조회 종료일", today - timedelta(days=1))
    
    target_product = st.text_input("물품분류명 (개별 저장용)")
    current_key = target_product.strip() if target_product.strip() else "전체"
    
    # [추가] 통합 조회 옵션
    is_combined_view = st.checkbox("📊 모든 로컬 데이터 통합 조회", value=False)

    if is_combined_view:
        master_df = load_all_combined_data()
        loaded_dates = set() # 통합뷰에서는 수집 날짜 체크 무시
        view_title = "🌐 통합 데이터 (전체 품목)"
    else:
        master_df, loaded_dates = load_data_by_key(current_key)
        view_title = f"📂 개별 품목: [{current_key}]"

    c1, c2 = st.columns(2)
    if c1.button("📡 데이터 불러오기", use_container_width=True, type="primary"):
        if not api_key: st.error("⚠️ API 인증키를 입력해주세요.")
        elif is_combined_view: st.warning("⚠️ 통합 조회 모드에서는 수집이 불가능합니다. 체크를 해제하고 품목별로 수집해주세요.")
        else:
            try:
                requested_dates = [start + timedelta(days=x) for x in range((end - start).days + 1)]
                new_dates = [d for d in requested_dates if d not in loaded_dates]
                if not new_dates: st.info(f"ℹ️ '{current_key}'의 해당 기간은 이미 수집되었습니다.")
                else:
                    all_new = []
                    prog, status = st.progress(0), st.empty()
                    for i, cur_date in enumerate(new_dates):
                        d_str = cur_date.strftime("%Y%m%d")
                        status.text(f"🌐 [{current_key}] 수집 중: {d_str}")
                        page = 1
                        while True:
                            p = {"ServiceKey": api_key, "pageNo": page, "numOfRows": 100, "type": "json", "inqryDiv": "1", "inqryBgnDate": d_str, "inqryEndDate": d_str}
                            if target_product.strip(): p["prdctClsfcNoNm"] = target_product.strip()
                            response = requests.get(BASE_URL, params=p, timeout=30)
                            items = response.json().get("response", {}).get("body", {}).get("items", [])
                            if isinstance(items, dict): items = [items]
                            df_tmp = pd.DataFrame(items)
                            if df_tmp.empty: break
                            all_new.append(df_tmp)
                            if len(df_tmp) < 100: break
                            page += 1; time.sleep(0.1)
                        loaded_dates.add(cur_date)
                        prog.progress((i + 1) / len(new_dates))
                    if all_new:
                        new_df = pd.concat(all_new, ignore_index=True)
                        master_df = pd.concat([master_df, new_df], ignore_index=True)
                        save_data_by_key(current_key, master_df, loaded_dates)
                        st.rerun()
            except Exception as e: st.error(f"❌ 오류: {e}")

    if c2.button("🗑️ 현재항목 초기화", use_container_width=True):
        if is_combined_view: st.error("⚠️ 통합 모드에서는 초기화가 불가능합니다.")
        else:
            safe_key = get_safe_filename(current_key)
            for f in [f"data_{safe_key}.csv", f"dates_{safe_key}.txt"]:
                if os.path.exists(f): os.remove(f)
            st.rerun()

    st.markdown("---")
    corp_filter = st.text_input("업체명 필터")
    div_filter = st.text_input("기업구분명 필터")
    inst_filter = st.text_input("수요기관명 필터")

# -----------------------------------------
# 화면 표시 로직 (기존과 동일)
# -----------------------------------------
if not master_df.empty:
    df = master_df.copy()
    for kor, eng in REVERSE_MAP.items():
        if kor in df.columns and eng not in df.columns: df[eng] = df[kor]
    for col in ["prdctAmt", "dlvrReqChngNo", "prdctSno", "prdctPrc", "prdctQty"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    if 'dlvrReqNo' in df.columns:
        curr_year_short = str(datetime.now().year)[2:4]
        df = df[df['dlvrReqNo'].astype(str).str[1:3] == curr_year_short].copy()
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
        df_view = df_view[df_view[inst_col].astype(str).str.contains(inst_filter.strip(), case=False, na=False)]

    st.subheader(view_title)
    st.info(f"📅 조회 데이터: {len(df_view):,}건")
    
    if not df_view.empty:
        # 업체별 합계 및 상세 리스트 표시 (기존 코드와 동일)
        df_view["우수금액"] = df_view.apply(lambda x: x["금액"] if str(x.get("우수제품여부", "N")).upper() == "Y" else 0, axis=1)
        df_view["MAS금액"] = df_view.apply(lambda x: x["금액"] if str(x.get("MAS여부", "N")).upper() == "Y" else 0, axis=1)
        summary = df_view.groupby("업체명").agg({"금액": "sum", "우수금액": "sum", "MAS금액": "sum", "납품요구번호": "count"}).reset_index()
        summary.columns = ["업체명", "총 합계금액", "우수제품 합계", "MAS 합계", "건수"]
        summary = summary.sort_values("총 합계금액", ascending=False).reset_index(drop=True)
        summary.insert(0, "순위", summary.index + 1)
        
        st.dataframe(summary.style.format({"총 합계금액": "{:,.0f}원", "우수제품 합계": "{:,.0f}원", "MAS 합계": "{:,.0f}원", "건수": "{:,}건"}), use_container_width=True, hide_index=True, height=SUMMARY_TABLE_HEIGHT, column_config={k: st.column_config.Column(width=v) for k, v in SUMMARY_WIDTHS.items()})

        disp_cols = ["납품요구번호", "납품요구변경번호", "납품요구일자", "납품요구접수일자", "품목일련번호", "업체명", "주문기관명", "수요기관명", "세부품명", "품명", "규격", "수량", "단가", "금액", "납품기한", "납품장소"]
        final_disp = [c for c in disp_cols if c in df_view.columns]
        st.dataframe(df_view[final_disp].style.format({c: "{:,.0f}" for c in ["금액", "단가", "수량"] if c in final_disp}, na_rep="-"), use_container_width=True, hide_index=True, height=DETAIL_TABLE_HEIGHT, column_config={k: st.column_config.Column(width=v) for k, v in DETAIL_WIDTHS.items()})
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_view[final_disp].to_excel(writer, sheet_name="상세내역", index=False)
            summary.to_excel(writer, sheet_name="업체별합계", index=False)
        st.download_button(label="📊 엑셀 다운로드", data=output.getvalue(), file_name=f"조달통합_{datetime.now().strftime('%H%M%S')}.xlsx", use_container_width=True)
else:
    st.info("💡 데이터가 없습니다. 수집을 진행하거나 통합 조회 체크박스를 확인해 주세요.")