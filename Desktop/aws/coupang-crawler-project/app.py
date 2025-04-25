import streamlit as st
import pandas as pd
import plotly.express as px
from typing import List
import re

CATEGORY_LEVELS = [
    'CPH_LEVEL_1_NAME',
    'CPH_LEVEL_2_NAME',
    'CPH_LEVEL_3_NAME',
    'CPH_LEVEL_4_NAME',
    'PROD_ID',
]

@st.cache_data
def load_data():
    return pd.read_excel('coupang_merged.xlsx')

def initialize_session_state():
    """세션 상태 초기화 및 유지"""
    if 'filters' not in st.session_state:
        st.session_state['filters'] = {}  # {level: [선택값,...]} 구조로 단순화
    
    if 'date_range' not in st.session_state:
        st.session_state['date_range'] = None
    
    if 'group_col' not in st.session_state:
        st.session_state['group_col'] = None
    
    if 'filtered_options' not in st.session_state:
        st.session_state['filtered_options'] = {}  # {level: [가능한 옵션,...]}
    
    if 'expanded_filters' not in st.session_state:
        st.session_state['expanded_filters'] = {}  # {level: True/False} - 각 필터 섹션의 확장 상태

def format_level_name(level: str) -> str:
    """레벨 이름을 더 읽기 쉽게 포맷팅"""
    if level == 'PROD_ID':
        return '📦 Product'
    
    # CPH_LEVEL_N_NAME 패턴에서 N에 해당하는 숫자 추출
    match = re.match(r'CPH_LEVEL_(\d)_NAME', level)
    if match:
        level_num = int(match.group(1))
        icons = ['📱', '📱', '🔧', '💎']  # 레벨에 따른 아이콘
        icon = icons[level_num-1] if level_num <= len(icons) else '🏷️'
        return f"{icon} {format_category_name(level)}"
    
    return level.replace('_NAME', '')

def format_category_name(level_name: str) -> str:
    """카테고리 레벨 이름을 사용자 친화적으로 변환"""
    level_mapping = {
        'CPH_LEVEL_1_NAME': 'CPH LV1',
        'CPH_LEVEL_2_NAME': 'CPH LV2',
        'CPH_LEVEL_3_NAME': 'CPH LV3',
        'CPH_LEVEL_4_NAME': 'CPH LV4',
        'PROD_ID': 'PRODUCT ID'
    }
    return level_mapping.get(level_name, level_name)

def get_filtered_options(df, level_idx):
    """현재 수준 이전의 필터를 적용하여 현재 수준에서 사용 가능한 옵션 반환"""
    filtered_df = df.copy()
    
    # 이전 레벨들의 선택에 따라 데이터 필터링
    for i in range(level_idx):
        prev_level = CATEGORY_LEVELS[i]
        if prev_level in df.columns and prev_level in st.session_state['filters'] and st.session_state['filters'][prev_level]:
            filtered_df = filtered_df[filtered_df[prev_level].isin(st.session_state['filters'][prev_level])]
    
    current_level = CATEGORY_LEVELS[level_idx]
    if current_level in filtered_df.columns:
        return list(filtered_df[current_level].dropna().unique())
    return []

def render_multiselect_with_search(level: str, options: List[str], default_values: List[str], key_prefix: str):
    """검색 기능이 있는 향상된 멀티셀렉트 UI 렌더링"""
    # 확장/축소 상태 초기화
    if level not in st.session_state['expanded_filters']:
        st.session_state['expanded_filters'][level] = True
        
    # 검색어 상태
    search_key = f"{key_prefix}_search_{level}"
    if search_key not in st.session_state:
        st.session_state[search_key] = ""
    
    # 선택 항목 상태
    selection_key = f"{key_prefix}_selection_{level}"
    if selection_key not in st.session_state:
        st.session_state[selection_key] = default_values
    
    # 타이틀로 사용할 수 있는 포맷된 레벨 이름
    formatted_level = format_level_name(level)
    selected_count = len(st.session_state[selection_key])
    total_count = len(options)
    
    # 확장/축소 상태 표시 및 토글
    expander_title = f"{formatted_level} ({selected_count}/{total_count})"
    with st.sidebar.expander(expander_title, expanded=st.session_state['expanded_filters'][level]):
        # 검색 입력
        st.text_input(
            "🔍 Search",
            value=st.session_state[search_key],
            key=f"{search_key}_input",
            on_change=lambda: setattr(st.session_state, search_key, st.session_state[f"{search_key}_input"])
        )
        
        # 검색어 필터링
        search_term = st.session_state[search_key].lower()
        filtered_options = [opt for opt in options if search_term in str(opt).lower()] if search_term else options
        
        # 전체 선택/해제 버튼
        cols = st.columns([1, 1])
        with cols[0]:
            if st.button("✅ All", key=f"select_all_{level}"):
                st.session_state[selection_key] = filtered_options if search_term else options
                st.rerun()
        with cols[1]:
            if st.button("❌ None", key=f"clear_{level}"):
                st.session_state[selection_key] = []
                st.rerun()
        
        # 옵션이 많으면 스크롤 가능한 컨테이너에 표시
        if len(filtered_options) > 10:
            container_height = min(300, len(filtered_options) * 35)  # 옵션 수에 따라 컨테이너 높이 조정
            with st.container():
                # 스타일링
                st.markdown(f"""
                <style>
                    div[data-testid="stVerticalBlock"] > div[style*="flex-direction: column"] > div[data-testid="stVerticalBlock"] {{
                        max-height: {container_height}px;
                        overflow-y: auto;
                    }}
                </style>
                """, unsafe_allow_html=True)
                
                # 검색 결과가 없을 때 메시지 표시
                if not filtered_options:
                    st.info("No matching options")
                
                # 각 옵션을 체크박스로 표시
                new_selections = []
                for option in filtered_options:
                    is_selected = option in st.session_state[selection_key]
                    option_display = str(option)
                    
                    # 특별한 옵션 (예: iPhone, iPad 등)에 대한 아이콘 추가
                    if level == 'CPH_LEVEL_1_NAME':
                        icon_map = {
                            'iPhone': '', 
                            'iPad': '', 
                            'Mac': '', 
                            'Watch': '', 
                            'AirPods': ''
                        }
                        prefix = icon_map.get(option, '')
                        option_display = f"{prefix}{option}"
                    
                    if st.checkbox(
                        option_display,
                        value=is_selected,
                        key=f"{key_prefix}_opt_{level}_{option}"
                    ):
                        new_selections.append(option)
                
                # UI에서 선택을 업데이트
                st.session_state[selection_key] = new_selections
        else:
            # 옵션이 적을 때는 일반 체크박스 목록
            new_selections = []
            for option in filtered_options:
                is_selected = option in st.session_state[selection_key]
                option_display = str(option)
                
                # 특별한 옵션 아이콘 추가
                if level == 'CPH_LEVEL_1_NAME':
                    icon_map = {
                        'iPhone': '', 
                        'iPad': '', 
                        'Mac': '', 
                        'Watch': '', 
                        'AirPods': ''
                    }
                    prefix = icon_map.get(option, '')
                    option_display = f"{prefix}{option}"
                
                if st.checkbox(
                    option_display,
                    value=is_selected,
                    key=f"{key_prefix}_opt_{level}_{option}"
                ):
                    new_selections.append(option)
            
            st.session_state[selection_key] = new_selections
    
    # 세션 상태에서 현재 상태 가져오기
    return st.session_state[selection_key]

def render_sidebar(df):
    st.sidebar.header('🔎 Filters')
    
    # 날짜 범위 필터
    min_date, max_date = df['DATE'].min(), df['DATE'].max()
    if st.session_state['date_range'] is None:
        st.session_state['date_range'] = [min_date, max_date]
    
    date_range = st.sidebar.date_input(
        '📅 Date Range', 
        st.session_state['date_range'],
        min_value=min_date, 
        max_value=max_date,
        key='date_input'
    )
    st.session_state['date_range'] = date_range
    
    # 그룹 컬럼 선택
    group_options = [c for c in CATEGORY_LEVELS if c in df.columns]
    if st.session_state['group_col'] is None and group_options:
        st.session_state['group_col'] = group_options[0]
    
    prev_group = st.session_state['group_col']
    
    # 그룹 선택기에 사용자 친화적인 레이블 사용
    group_labels = {col: format_category_name(col) for col in group_options}
    formatted_options = [format_category_name(opt) for opt in group_options]
    
    group_col_display = st.sidebar.selectbox(
        '📊 Group by', 
        formatted_options,
        index=formatted_options.index(format_category_name(st.session_state['group_col'])) if st.session_state['group_col'] in group_options else 0,
        key='group_selector'
    )
    
    # 표시 이름을 실제 컬럼 이름으로 변환
    group_col = next((col for col, label in group_labels.items() if label == group_col_display), group_options[0])
    
    # 그룹 변경 감지 및 처리
    if prev_group != group_col:
        st.session_state['group_col'] = group_col
        # 그룹 변경 시 필터링된 옵션 캐시 초기화
        st.session_state['filtered_options'] = {}
    
    # 필터 리셋 버튼 (스타일 적용)
    st.sidebar.markdown("""
    <style>
    div.stButton > button {
        width: 100%;
        background-color: #f0f2f6;
        border: 1px solid #ddd;
        padding: 10px;
        border-radius: 5px;
        color: #444;
    }
    div.stButton > button:hover {
        background-color: #e6e9ef;
        color: #000;
    }
    </style>
    """, unsafe_allow_html=True)
    
    if st.sidebar.button('🔄 Reset All Filters', key='reset_button'):
        # 날짜와 그룹 선택은 유지
        current_date = st.session_state['date_range']
        current_group = st.session_state['group_col']
        
        # 필터 관련 세션 상태 초기화
        st.session_state['filters'] = {}
        st.session_state['filtered_options'] = {}
        
        # 멀티셀렉트 선택 상태 초기화
        for level in CATEGORY_LEVELS:
            selection_key = f"ms_selection_{level}"
            if selection_key in st.session_state:
                st.session_state[selection_key] = []
        
        # 날짜와 그룹 선택 복원
        st.session_state['date_range'] = current_date
        st.session_state['group_col'] = current_group
        st.rerun()  # 페이지 새로고침
    
    # 구분선 추가
    st.sidebar.markdown("---")
    
    # 향상된 멀티셀렉트 필터 처리
    filtered_df = df.copy()
    for i, level in enumerate(CATEGORY_LEVELS):
        if level in df.columns:
            # 필터링된 옵션 계산 (캐싱 적용)
            if level not in st.session_state['filtered_options']:
                options = get_filtered_options(df, i)
                st.session_state['filtered_options'][level] = options
            else:
                options = st.session_state['filtered_options'][level]
            
            # 특별한 순서가 필요한 레벨 처리
            if level == 'CPH_LEVEL_1_NAME':
                preferred_order = ['iPhone', 'iPad', 'Mac', 'Watch', 'AirPods']
                options = [x for x in preferred_order if x in options] + [x for x in options if x not in preferred_order]
            else:
                options.sort()
            
            # 필터에 현재 레벨 값이 없으면 초기화
            if level not in st.session_state['filters']:
                st.session_state['filters'][level] = []

            # 이전 선택된 값 중 현재 옵션에 있는 것만 유지
            valid_selections = [v for v in st.session_state['filters'][level] if v in options]
            
            # 향상된 멀티셀렉트 렌더링
            selected = render_multiselect_with_search(
                level=level,
                options=options,
                default_values=valid_selections,
                key_prefix="ms"
            )
            
            # 선택 값 세션에 저장
            st.session_state['filters'][level] = selected

            # 선택된 값으로 데이터프레임 필터링하여 다음 레벨의 옵션 결정
            if selected:
                filtered_df = filtered_df[filtered_df[level].isin(selected)]
                # 이 레벨 선택 후 하위 레벨 옵션 캐시 무효화
                for next_level in CATEGORY_LEVELS[i+1:]:
                    if next_level in st.session_state['filtered_options']:
                        del st.session_state['filtered_options'][next_level]
            elif i > 0:  # 첫 레벨이 아닌데 선택 없으면 빈 데이터프레임
                filtered_df = filtered_df.iloc[0:0]
    
    # 선택 결과 반환
    selections = {'DATE': date_range, 'group_col': group_col}
    for level in CATEGORY_LEVELS:
        if level in df.columns:
            selections[level] = st.session_state['filters'].get(level, [])
    
    return selections, group_col

def filter_data(df, selections):
    df_filtered = df.copy()
    start_date, end_date = selections['DATE']
    df_filtered = df_filtered[(df_filtered['DATE'] >= pd.Timestamp(start_date)) & (df_filtered['DATE'] <= pd.Timestamp(end_date))]
    for level in CATEGORY_LEVELS:
        if level in selections and selections[level]:
            df_filtered = df_filtered[df_filtered[level].astype(str).isin(selections[level])]
        elif level in selections and level in df.columns and not selections[level]:
            # 해당 레벨에 선택이 없으면 모든 값 허용 (상위 레벨 필터가 적용된 상태)
            pass
    
    # 필터 계층 강제 적용 (상위 레벨 선택이 없는데 하위 레벨 선택이 있으면 빈 결과)
    for i, level in enumerate(CATEGORY_LEVELS):
        if i > 0 and level in selections and selections[level]:
            prev_level = CATEGORY_LEVELS[i-1]
            if prev_level in selections and not selections[prev_level]:
                return df_filtered.iloc[0:0]  # 빈 데이터프레임 반환
    
    return df_filtered

def group_and_aggregate(df, group_col):
    if df.empty or group_col not in df.columns:
        return pd.DataFrame()
    agg_df = df.groupby(group_col).agg(
        price_avg=('PRICE', 'mean'),
        product_count=('PROD_ID', 'count'),
        discount_rate_avg=('DISCOUNT_RATE', 'mean')
    ).reset_index()
    # 혹시 이미 포맷 컬럼이 있으면 삭제
    for col in ['Price Avg. (KRW)', 'Discount Rate Avg. (%)']:
        if col in agg_df.columns:
            agg_df = agg_df.drop(columns=[col])
    # 포맷 컬럼 생성
    agg_df['Price Avg. (KRW)'] = agg_df['price_avg'].apply(lambda x: f'{int(round(x)):,}' if pd.notnull(x) else '')
    agg_df['Discount Rate Avg. (%)'] = agg_df['discount_rate_avg'].apply(lambda x: f'{x*100:.1f}%' if pd.notnull(x) else '')
    # 원본 price_avg, discount_rate_avg 컬럼은 숨김
    show_cols = [c for c in agg_df.columns if c not in ['price_avg','discount_rate_avg']] + ['Price Avg. (KRW)','Discount Rate Avg. (%)']
    # 컬럼 중복 방지
    show_cols = list(dict.fromkeys(show_cols))
    return agg_df[show_cols]

def render_chart(df, group_col):
    st.subheader(f'📈 Daily Trend: {format_category_name(group_col)}')
    if df.empty:
        st.info('선택된 조건에 해당하는 데이터가 없습니다.')
        return
    # 날짜, group_col별 평균 가격 및 할인율 집계
    if 'DISCOUNT_RATE' in df.columns:
        chart_df = df.groupby(['DATE', group_col], as_index=False).agg({'PRICE':'mean', 'DISCOUNT_RATE':'mean'})
        chart_df['할인율(%)'] = chart_df['DISCOUNT_RATE'].apply(lambda x: f'{x*100:.1f}%' if pd.notnull(x) else '')
    else:
        chart_df = df.groupby(['DATE', group_col], as_index=False)['PRICE'].mean()
        chart_df['할인율(%)'] = ''
    chart_df['PRICE_KRW'] = chart_df['PRICE'].apply(lambda x: f'{int(round(x)):,}')

    # group_col 순서 지정 (LEVEL_1은 지정 순서, 나머지는 기존 방식)
    if group_col == 'CPH_LEVEL_1_NAME':
        preferred_order = ['iPhone', 'iPad', 'Mac', 'Watch', 'AirPods']
        group_order = [g for g in preferred_order if g in chart_df[group_col].unique()]
        group_order += [g for g in chart_df[group_col].unique() if g not in group_order]
        # 상위 20개 제한
        group_order = group_order[:20]
    else:
        latest_date = chart_df['DATE'].max()
        order_df = chart_df[chart_df['DATE'] == latest_date].sort_values('PRICE', ascending=False)
        group_order = order_df[group_col].tolist()
        group_order = group_order[::-1][:20]
    chart_df = chart_df[chart_df[group_col].isin(group_order)]

    fig = px.line(
        chart_df,
        x='DATE',
        y='PRICE',
        color=group_col,
        markers=True,
        title='',
        category_orders={group_col: group_order},
        custom_data=['할인율(%)','PRICE_KRW']
    )
    fig.update_traces(
        hovertemplate='<b>%{fullData.name}</b><br>Date=%{x}<br>Price=%{customdata[1]}<br>Discount=%{customdata[0]}'
    )
    st.plotly_chart(fig, use_container_width=True)

def render_table(df, agg_df):
    # 집계 테이블
    st.subheader('집계 결과 테이블')
    if agg_df.empty:
        st.info('선택된 조건에 해당하는 집계 데이터가 없습니다.')
    else:
        # agg_df는 이미 group_and_aggregate에서 포맷 컬럼만 남기고 반환됨. 중복 생성 금지.
        st.dataframe(agg_df, use_container_width=True, hide_index=True)

    # 상세 데이터 테이블
    show_detail = st.checkbox('상세 데이터 보기', value=False, key='show_detail_check')
    if show_detail:
        st.subheader('상세 데이터 테이블')
        if df.empty:
            st.info('선택된 조건에 해당하는 상세 데이터가 없습니다.')
        else:
            df_display = df.copy()
            # 가격 포맷: KRW, 소수점 제거, 천 단위 쉼표
            for col in ['PRICE','ORIGIN_PRICE','COUPON_PRICE','AC_PRICE']:
                if col in df_display.columns:
                    df_display[col + ' (KRW)'] = df_display[col].apply(lambda x: f'{int(round(x)):,}' if pd.notnull(x) else '')
            # 할인율 포맷: %
            if 'DISCOUNT_RATE' in df_display.columns:
                df_display['할인율(%)'] = df_display['DISCOUNT_RATE'].apply(lambda x: f'{x*100:.1f}%' if pd.notnull(x) else '')
            # (KRW), (%) 컬럼만 표시, 원본 가격/할인율 컬럼은 숨김
            krw_cols = [c for c in df_display.columns if c.endswith('(KRW)')]
            percent_cols = [c for c in df_display.columns if c.endswith('(%)')]
            base_cols = [c for c in df_display.columns if not any(c.endswith(s) for s in ['(KRW)','(%)']) and c not in ['PRICE','ORIGIN_PRICE','COUPON_PRICE','AC_PRICE','DISCOUNT_RATE']]
            show_cols = base_cols + krw_cols + percent_cols
            st.dataframe(df_display[show_cols].sort_values(by='DATE', ascending=False) if 'DATE' in df_display.columns else df_display[show_cols], use_container_width=True, hide_index=True)

def render_discount_topn_price_diff(df, n=10, group_col=None):
    st.subheader('TOP N')
    if df.empty or 'PRICE' not in df.columns:
        st.info('No price data available.')
        return
    n_days = st.number_input('Compare with N days ago', min_value=1, max_value=60, value=1, step=1, key='topn_ndays')
    latest_date = df['DATE'].max()
    prev_date = latest_date - pd.Timedelta(days=n_days)
    today_df = df[df['DATE'] == latest_date].copy()
    prev_df = df[df['DATE'] == prev_date].copy()
    if today_df.empty or prev_df.empty:
        st.info('비교할 날짜 데이터가 부족합니다.')
        return
    # group_col이 PROD_ID가 아니면 group_col 단위 집계, PROD_ID면 상품 단위
    if group_col and group_col != 'PROD_ID':
        # group_col별 평균 가격 집계
        today_g = today_df.groupby(group_col)['PRICE'].mean().reset_index().rename(columns={'PRICE':'PRICE_today'})
        prev_g = prev_df.groupby(group_col)['PRICE'].mean().reset_index().rename(columns={'PRICE':'PRICE_prev'})
        merged = pd.merge(today_g, prev_g, on=group_col, how='inner')
        merged['Price Diff'] = merged['PRICE_today'] - merged['PRICE_prev']
        merged['Price Diff (%)'] = merged['Price Diff'] / merged['PRICE_prev'] * 100
        merged = merged.sort_values('Price Diff (%)', ascending=False).head(n)
        merged['Today Price (KRW)'] = merged['PRICE_today'].apply(lambda x: f'{int(round(x)):,}' if pd.notnull(x) else '')
        merged[f'{n_days}d Ago Price (KRW)'] = merged['PRICE_prev'].apply(lambda x: f'{int(round(x)):,}' if pd.notnull(x) else '')
        merged['Price Diff (KRW)'] = merged['Price Diff'].apply(lambda x: f'{int(round(x)):,}' if pd.notnull(x) else '')
        merged['Price Diff (%)'] = merged['Price Diff (%)'].apply(lambda x: f'{x:+.1f}%' if pd.notnull(x) else '')
        show_cols = [group_col, 'Today Price (KRW)', f'{n_days}d Ago Price (KRW)', 'Price Diff (KRW)', 'Price Diff (%)']
        st.dataframe(merged[show_cols], use_container_width=True, hide_index=True)
        if not merged.empty:
            fig = px.bar(merged, x=group_col, y='Price Diff (%)',
                         hover_data=show_cols,
                         labels={'Price Diff (%)':'Price Diff (%)',group_col:group_col},
                         title=f'TOP {n} {group_col} Price Drop (%) (Today vs {n_days}d Ago)')
            fig.update_traces(hovertemplate=f'<b>%{{x}}</b><br>Price Diff (%)=%{{y}}')
            fig.update_layout(xaxis_title=group_col, yaxis_title='Price Diff (%)', xaxis={'categoryorder':'array','categoryarray':merged[group_col].tolist()})
            st.plotly_chart(fig, use_container_width=True)
    else:
        # PROD_ID 단위로 기존 방식
        merge_cols = [col for col in ['PROD_ID','CPH_LEVEL_1_NAME','CPH_LEVEL_2_NAME','CPH_LEVEL_3_NAME','CPH_LEVEL_4_NAME'] if col in df.columns]
        if not merge_cols:
            merge_cols = [df.columns[0]]
        merged = pd.merge(
            today_df, prev_df, on=merge_cols, suffixes=('_today','_prev'), how='inner'
        )
        merged['Price Diff'] = merged['PRICE_today'] - merged['PRICE_prev']
        merged['Price Diff (%)'] = merged['Price Diff'] / merged['PRICE_prev'] * 100
        merged = merged.sort_values('Price Diff (%)', ascending=False).head(n)
        merged['Today Price (KRW)'] = merged['PRICE_today'].apply(lambda x: f'{int(round(x)):,}' if pd.notnull(x) else '')
        merged[f'{n_days}d Ago Price (KRW)'] = merged['PRICE_prev'].apply(lambda x: f'{int(round(x)):,}' if pd.notnull(x) else '')
        merged['Price Diff (KRW)'] = merged['Price Diff'].apply(lambda x: f'{int(round(x)):,}' if pd.notnull(x) else '')
        merged['Price Diff (%)'] = merged['Price Diff (%)'].apply(lambda x: f'{x:+.1f}%' if pd.notnull(x) else '')
        show_cols = [c for c in ['CPH_LEVEL_1_NAME','CPH_LEVEL_2_NAME','CPH_LEVEL_3_NAME','CPH_LEVEL_4_NAME','PROD_ID','Today Price (KRW)',f'{n_days}d Ago Price (KRW)','Price Diff (KRW)','Price Diff (%)'] if c in merged.columns]
        st.dataframe(merged[show_cols], use_container_width=True, hide_index=True)
        if not merged.empty:
            fig = px.bar(merged, x='PROD_ID', y='Price Diff (%)',
                         color='CPH_LEVEL_1_NAME' if 'CPH_LEVEL_1_NAME' in merged.columns else None,
                         hover_data=show_cols,
                         labels={'Price Diff (%)':'Price Diff (%)','PROD_ID':'Product'},
                         title=f'TOP {n} Price Drop (Today vs {n_days}d Ago)')
            fig.update_traces(hovertemplate='<b>%{x}</b><br>Price Diff (%)=%{y}')
            fig.update_layout(xaxis_title='Product', yaxis_title='Price Diff (%)')
            st.plotly_chart(fig, use_container_width=True)

def render_volatility(df, group_col):
    st.subheader('Price Volatility by Group')
    if df.empty:
        st.info('No data.')
        return
    vol_df = df.groupby(group_col)['PRICE'].agg(['std','max','min','mean']).reset_index()
    vol_df['Range'] = vol_df['max'] - vol_df['min']
    fig = px.bar(vol_df, x=group_col, y='std', title='Price Std Dev', labels={'std':'Std Dev'})
    st.plotly_chart(fig, use_container_width=True)
    fig2 = px.bar(vol_df, x=group_col, y='Range', title='Price Range (Max-Min)', labels={'Range':'Range'})
    st.plotly_chart(fig2, use_container_width=True)
    st.dataframe(vol_df[[group_col,'std','Range','mean','min','max']], use_container_width=True, hide_index=True)

def render_minmax_history(df, group_col):
    st.subheader('Min/Max Price History')
    if df.empty:
        st.info('No data.')
        return
    min_df = df.groupby(['DATE', group_col])['PRICE'].min().reset_index()
    max_df = df.groupby(['DATE', group_col])['PRICE'].max().reset_index()
    fig = px.line(min_df, x='DATE', y='PRICE', color=group_col, title='Min Price by Date')
    st.plotly_chart(fig, use_container_width=True)
    fig2 = px.line(max_df, x='DATE', y='PRICE', color=group_col, title='Max Price by Date', line_dash_sequence=['dash'])
    st.plotly_chart(fig2, use_container_width=True)

def render_periodic_heatmap(df, group_col):
    st.subheader('Monthly Price/Discount Heatmap')
    if df.empty:
        st.info('No data.')
        return
    df['Month'] = df['DATE'].dt.to_period('M').astype(str)
    heat_df = df.groupby(['Month', group_col]).agg({'PRICE':'mean','DISCOUNT_RATE':'mean'}).reset_index()
    pivot_price = heat_df.pivot(index=group_col, columns='Month', values='PRICE')
    pivot_discount = heat_df.pivot(index=group_col, columns='Month', values='DISCOUNT_RATE')
    st.write('**Price Heatmap**')
    fig = px.imshow(pivot_price, aspect='auto', color_continuous_scale='YlGnBu', labels={'color':'Price'})
    st.plotly_chart(fig, use_container_width=True)
    st.write('**Discount Rate Heatmap**')
    fig2 = px.imshow(pivot_discount, aspect='auto', color_continuous_scale='YlOrRd', labels={'color':'Discount Rate'})
    st.plotly_chart(fig2, use_container_width=True)

def render_option_diff(df):
    st.subheader('Option Price Distribution')
    if df.empty:
        st.info('No data.')
        return
    # 옵션 컬럼 자동 탐색 (LEVEL_3, LEVEL_4 등)
    option_cols = [c for c in ['CPH_LEVEL_3_NAME','CPH_LEVEL_4_NAME'] if c in df.columns]
    if not option_cols:
        st.info('No option columns.')
        return
    for col in option_cols:
        st.write(f'**{col}**')
        fig = px.box(df, x=col, y='PRICE', points='all', title=f'Price by {col}')
        st.plotly_chart(fig, use_container_width=True)

def main():
    # 앱 설정 및 스타일링
    st.set_page_config(
        page_title='Coupang Price Tracker',
        page_icon='📊',
        layout='wide',
        initial_sidebar_state='expanded'
    )
    
    # 글로벌 CSS 스타일 적용
    st.markdown("""
    <style>
        /* 폰트 및 전체 스타일 */
        .main .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        
        /* 사이드바 스타일 */
        .sidebar .sidebar-content {
            background-color: #f8f9fa;
        }
        
        /* 헤더 스타일 */
        h1, h2, h3 {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            color: #2c3e50;
        }
        
        /* 필터 그룹 스타일 */
        .stExpander {
            border: 1px solid #eee;
            border-radius: 4px;
            margin-bottom: 10px;
        }
        
        /* 버튼 스타일 */
        .stButton>button {
            border-radius: 4px;
            font-weight: 500;
        }
        
        /* 데이터프레임 스타일 */
        .dataframe-container {
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        
        /* 푸터 스타일 */
        footer {display: none !important;}
        .footer {
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            background-color: white;
            color: #888;
            text-align: center;
            padding: 10px;
            font-size: 0.8rem;
            border-top: 1px solid #eee;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # 앱 제목 및 설명
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title('📊 Coupang Price Tracker')
    with col2:
        st.markdown("""
        <div style="text-align:right; padding-top:10px;">
            <span style="background-color:#f0f2f6; padding:8px 15px; border-radius:20px; font-size:0.8rem;">
                Last updated: 2023-04-25
            </span>
        </div>
        """, unsafe_allow_html=True)
    
    # 세션 상태 초기화 및 유지
    initialize_session_state()
    
    # 데이터 로드
    df = load_data()
    
    # 사이드바 렌더링 및 필터 적용
    selections, group_col = render_sidebar(df)
    df_filtered = filter_data(df, selections)
    agg_df = group_and_aggregate(df_filtered, group_col)
    
    # 필터 적용 상태 표시
    st.markdown(f"""
    <div style="padding: 10px 15px; background-color: #f8f9fa; border-radius: 5px; margin-bottom: 20px; border-left: 5px solid #4CAF50;">
        <p style="margin:0; font-size:0.9rem;">
            <strong>🔍 Applied Filters:</strong> {len([s for s in selections.values() if isinstance(s, list) and s])} active filters | 
            <strong>📅 Date Range:</strong> {selections['DATE'][0].strftime('%Y-%m-%d')} to {selections['DATE'][1].strftime('%Y-%m-%d')} | 
            <strong>📊 Group By:</strong> {format_category_name(selections['group_col'])}
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # 결과 데이터 건수 표시
    st.markdown(f"""
    <div style="display: flex; margin-bottom: 20px;">
        <div style="flex: 1; text-align: center; padding: 15px; background-color: #e8f4f8; border-radius: 5px; margin-right: 10px;">
            <h3 style="margin:0; font-size:1.8rem; font-weight:bold; color:#0077b6;">{len(df_filtered):,}</h3>
            <p style="margin:0; font-size:0.9rem; color:#555;">Filtered Records</p>
        </div>
        <div style="flex: 1; text-align: center; padding: 15px; background-color: #e8f5e9; border-radius: 5px; margin-right: 10px;">
            <h3 style="margin:0; font-size:1.8rem; font-weight:bold; color:#2e7d32;">{len(agg_df):,}</h3>
            <p style="margin:0; font-size:0.9rem; color:#555;">Unique {format_category_name(group_col)}</p>
        </div>
        <div style="flex: 1; text-align: center; padding: 15px; background-color: #f9f1e8; border-radius: 5px;">
            <h3 style="margin:0; font-size:1.8rem; font-weight:bold; color:#ed6c02;">
                {df_filtered['PRICE'].mean():,.0f}원
            </h3>
            <p style="margin:0; font-size:0.9rem; color:#555;">Avg. Price (KRW)</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # 탭 생성
    tabs = st.tabs([
        "📈 Price Trend",
        "📉 Price Diff.",
        "📊 Price Analysis"  # 추가 분석 탭
    ])
    
    with tabs[0]:
        render_chart(df_filtered, group_col)
        show_table = st.checkbox('테이블 보기 (집계/상세)', value=False, key='show_table_check')
        if show_table:
            render_table(df_filtered, agg_df)
    
    with tabs[1]:
        render_discount_topn_price_diff(df_filtered, n=10, group_col=group_col)
    
    with tabs[2]:
        analysis_type = st.radio(
            "분석 유형 선택",
            ["가격 변동성", "월별 가격 추이", "옵션별 가격 분포"],
            key="analysis_type",
            horizontal=True
        )
        
        if analysis_type == "가격 변동성":
            render_volatility(df_filtered, group_col)
        elif analysis_type == "월별 가격 추이":
            render_periodic_heatmap(df_filtered, group_col)
        elif analysis_type == "옵션별 가격 분포":
            render_option_diff(df_filtered)
    
    # 푸터
    st.markdown("""
    <div class="footer">
        Coupang Price Tracker • Data source: coupang_merged.xlsx • Developed with Streamlit
    </div>
    """, unsafe_allow_html=True)

if __name__ == '__main__':
    main()