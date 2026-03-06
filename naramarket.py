# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from io import BytesIO

st.set_page_config(page_title="조달청 납품요구상세", layout="wide")
st.title("🏛️ 조달청 종합쇼핑몰 납품요구상세 현황")

BASE_URL = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getDlvrReqDtlInfoList"


# -----------------------------------------
# API 호출 (429 대응)
# -----------------------------------------
def fetch_api(params, max_retry=5):
    for attempt in range(max_retry):
        response = requests.get(BASE_URL, params=params, timeout=30)

        if response.status_code == 429:
            wait = 2 * (attempt + 1)
            st.warning(f"⏳ 호출 제한 → {wait}초 대기")
            time.sleep(wait)
            continue

        response.raise_for_status()
        return response.json()

    raise Exception("API 호출 제한 지속 발생")


def parse(data):
    body = data.get("response", {}).get("body", {})
    items = body.get("items", [])
    total = body.get("totalCount", 0)

    if isinstance(items, dict):
        items = [items]

    return pd.DataFrame(items), total


# -----------------------------------------
# 사이드바
# -----------------------------------------
with st.sidebar:

    api_key = st.text_input("API 인증키 (Encoding)", type="password")

    today = datetime.today()

    start = st.date_input("시작일", today - timedelta(days=7))
    end = st.date_input("종료일", today - timedelta(days=1))

    corpNm = st.text_input("업체명 필터 (선택)")
    prdctNmFilter = st.text_input("물품분류명 필터 (선택)")


if not api_key:
    st.warning("API 키를 입력하세요.")
    st.stop()

if start > end:
    st.error("시작일은 종료일보다 클 수 없습니다.")
    st.stop()

try:

    all_data = []
    total_days = (end - start).days + 1

    progress = st.progress(0)

    # ==========================================
    # 🔥 하루 단위 반복 수집 (누락 방지 핵심)
    # ==========================================
    for i in range(total_days):

        current_date = start + timedelta(days=i)
        date_str = current_date.strftime("%Y%m%d")

        page = 1

        while True:

            params = {
                "ServiceKey": api_key,
                "pageNo": page,
                "numOfRows": 1000,
                "type": "json",
                "inqryDiv": "1",
                "inqryBgnDate": date_str,
                "inqryEndDate": date_str,
            }

            data = fetch_api(params)
            df_tmp, total = parse(data)

            if df_tmp.empty:
                break

            all_data.append(df_tmp)

            # 마지막 페이지면 종료
            if len(df_tmp) < 1000:
                break

            page += 1
            time.sleep(0.1)

        progress.progress((i + 1) / total_days)

    progress.empty()

    if not all_data:
        st.warning("해당 기간에 데이터가 없습니다.")
        st.stop()

    df_all = pd.concat(all_data, ignore_index=True)

    period_total = len(df_all)

    # ==========================================
    # 필터 적용
    # ==========================================
    df_filtered = df_all.copy()

    if corpNm.strip():
        df_filtered = df_filtered[
            df_filtered["corpNm"].astype(str)
            .str.contains(corpNm.strip(), case=False, na=False)
        ]

    if prdctNmFilter.strip():
        df_filtered = df_filtered[
            df_filtered["prdctClsfcNoNm"].astype(str)
            .str.contains(prdctNmFilter.strip(), case=False, na=False)
        ]

    filtered_total = len(df_filtered)

    # ==========================================
    # 상태 메시지
    # ==========================================
    if corpNm.strip() or prdctNmFilter.strip():
        st.success(
            f"조회 성공 ✅ 기간 전체 {period_total:,}건 → 필터 적용 {filtered_total:,}건"
        )
    else:
        st.success(f"조회 성공 ✅ 기간 전체 {period_total:,}건")

    # ==========================================
    # 데이터 표시
    # ==========================================
    st.dataframe(df_filtered, use_container_width=True)

    # ==========================================
    # 업체별 합계
    # ==========================================
    summary = pd.DataFrame()

    if "corpNm" in df_filtered.columns and "prdctAmt" in df_filtered.columns:

        df_filtered["prdctAmt"] = pd.to_numeric(
            df_filtered["prdctAmt"], errors="coerce"
        ).fillna(0)

        summary = (
            df_filtered.groupby("corpNm", as_index=False)["prdctAmt"]
            .sum()
            .sort_values("prdctAmt", ascending=False)
        )

        summary.rename(columns={"prdctAmt": "계약금액합계"}, inplace=True)

        st.subheader("🏢 업체별 계약금액 합계")
        st.dataframe(summary, use_container_width=True)

    # ==========================================
    # 엑셀 저장
    # ==========================================
    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_filtered.to_excel(writer, sheet_name="RawData", index=False)

        if not summary.empty:
            summary.to_excel(writer, sheet_name="업체별합계", index=False)

    output.seek(0)

    st.download_button(
        label="📊 엑셀 다운로드",
        data=output,
        file_name="조달청_납품요구_조회결과.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

except Exception as e:
    st.error(f"오류 발생: {e}")