from glob import glob
import pandas as pd
import conn_db
import helper

# 수출입 금액 단위, 무역규모, 무역수지, ton당 금액 계산
def _add_units(df):
    trade_types = ['수출','수입']
    unit_types = ['백만', '억']
    unit_dict = {'백만': 1000,
                '억': 100000}

    for unit_type in unit_types:
        for trade_type in trade_types:
            df[f'{trade_type}금액 ({unit_type}$)'] = df[f'{trade_type}금액 (천$)']/unit_dict[unit_type]
        df[f'무역규모 ({unit_type}$)'] = df[f'수출금액 ({unit_type}$)'] + df[f'수입금액 ({unit_type}$)']
        df[f'무역수지 ({unit_type}$)'] = df[f'수출금액 ({unit_type}$)'] - df[f'수입금액 ({unit_type}$)']

    df[f'무역수지 (천$)'] = df['수출금액 (천$)'] - df['수입금액 (천$)']
    df[f'무역규모 (천$)'] = df['수출금액 (천$)'] + df['수입금액 (천$)']

    try: # 국가별 수출입에는 중량이 없음
        df['수출ton당 (천$/ton)'] = df['수출금액 (천$)'] / df['수출중량 (ton)']
        df['수입ton당 (천$/ton)'] = df['수입금액 (천$)'] / df['수입중량 (ton)']
    except: # 국가별 수출입에만 건수가 있음
        df['수출 건당 (천$/건)'] = df['수출금액 (천$)'] / df['수출건수 (건)']
        df['수입 건당 (천$/건)'] = df['수입금액 (천$)'] / df['수입건수 (건)']

    return df

# 신성질별 수출입
@helper.timer
def clean_trade_by_new_type():
    # 불러와서 합치기
    all_files = glob(conn_db.get_path('신성질별_수출입_raw') + "*.xls")
    df = pd.concat([pd.read_excel(file, skiprows=4)
                   for file in all_files], axis=0).drop_duplicates()

    filt1 = df['성질명'].str.startswith('(')
    filt2 = df['성질명'].str.endswith(')')
    filt3 = df['기간'] != '총계'
    filt = filt1 & filt2 & filt3
    df = df.loc[filt].reset_index(drop=True)

    df = helper.drop_column(df, 'Unnamed')

    df['중량'] = pd.to_numeric(df['중량'].str.replace(',', ''))
    df['금액'] = pd.to_numeric(df['금액'].str.replace(',', ''))
    df.rename(columns={'기간': '날짜'}, inplace=True)
    df['날짜'] = pd.to_datetime(df['날짜'])

    trade_types = ['수출','수입']
    result_df = pd.DataFrame()
    for trade_type in trade_types:
        temp = df.loc[df['수출입구분'] == trade_type].copy()
        names = {'중량': trade_type+'중량 (ton)',
                 '금액': trade_type+'금액 (천$)'}
        temp = temp.rename(columns=names).drop(columns='수출입구분')
        result_df = result_df.append(temp)

    result_df.reset_index(drop=True, inplace=True)

    cols = ['날짜', '성질명']
    result_df = result_df.groupby(cols).agg(sum).reset_index()
    result_df['성질명'] = result_df['성질명'].str[1:-1].str.strip()

    # 맵핑
    map_df = conn_db.from_('Master_수출입품목', '신성질별_short')

    df = map_df.merge(result_df, left_on='세세분류명', right_on='성질명',
                      how='inner').drop(columns='성질명')

    df = _add_units(df)

    [df.rename(columns={col: col[:-1]}, inplace=True)
     for col in df.columns.tolist() if '분류명' in col]

    cols_long = ['대분류', '중분류','소분류','세분류','세세분류','설명','날짜',
                '수입중량 (ton)','수입금액 (천$)', '수출중량 (ton)', '수출금액 (천$)',
                '수입금액 (백만$)', '수출금액 (백만$)', '무역규모 (백만$)', '무역수지 (백만$)',
                '수입금액 (억$)', '수출금액 (억$)', '무역규모 (억$)', '무역수지 (억$)', '무역수지 (천$)',
                '무역규모 (천$)', '수출ton당 (천$/ton)', '수입ton당 (천$/ton)']

    cols_short = ['날짜','대분류', '중분류','소분류','세분류','세세분류','설명',
                    '수출중량 (ton)','수입중량 (ton)','수입금액 (천$)',  '수출금액 (천$)',
                    '무역수지 (천$)','무역규모 (천$)']
    # 저장
    conn_db.export_(df[cols_long], '수출입_신성질별')
    for sheet in ['DB_수출입_신성질별','수출입_신성질별_대시보드']:
        conn_db.to_(df[cols_short], sheet, 'import')

# 품목별 수출입
@helper.timer
def clean_trade_by_product_type():
    all_files = glob(conn_db.get_path('품목별_수출입') + "*.xls")
    df = pd.concat([pd.read_excel(file, skiprows=4)
                    for file in all_files], axis=0).drop_duplicates()

    filt = df['기간']!='총계'
    df = df.loc[filt]
    names = {'기간': '날짜',
            '품목명': '대분류',
            '수출중량': '수출중량 (ton)',
            '수입중량': '수입중량 (ton)',
            '수출금액': '수출금액 (천$)',
            '수입금액': '수입금액 (천$)',
            '무역수지': '무역수지 (천$)'}
    df = df.rename(columns=names).drop(columns='품목코드')
    df['날짜'] = pd.to_datetime(df['날짜'])

    matcher = ['수출', '수입', '무역']
    all_cols = df.columns.tolist()
    value_cols = [col for col in all_cols if any(
        prcnt in col for prcnt in matcher)]
    for col in value_cols:
        df[col] = pd.to_numeric(df[col].str.replace(',', ''))

    df = _add_units(df)

    cols_long = ['날짜','대분류','수출중량 (ton)','수입중량 (ton)',
                '수출금액 (천$)','수입금액 (천$)', '무역규모 (천$)','무역수지 (천$)',
                '수출금액 (백만$)','수입금액 (백만$)','무역규모 (백만$)','무역수지 (백만$)',
                '수출금액 (억$)', '수입금액 (억$)', '무역규모 (억$)', '무역수지 (억$)',
                '수출ton당 (천$/ton)', '수입ton당 (천$/ton)']

    cols_short = ['날짜','대분류','수출중량 (ton)','수입중량 (ton)',
                '수출금액 (천$)','수입금액 (천$)', '무역규모 (천$)','무역수지 (천$)',
                '수출ton당 (천$/ton)', '수입ton당 (천$/ton)']

    # 저장
    conn_db.export_(df[cols_long], '수출입_품목별')
    conn_db.to_(df[cols_short], 'DB_수출입_품목별', 'import')

# 국가별 수출입
@helper.timer
def clean_trade_by_country():
    all_files = glob(conn_db.get_path('국가별_수출입_raw') + "*.xls")
    df = pd.concat([pd.read_excel(file, skiprows=4)
                    for file in all_files], axis=0).drop_duplicates()

    filt = df['기간'] != '총계'
    df = df.loc[filt]
    names = {'기간': '날짜',
            '국가명': '국가',
            '수출건수': '수출건수 (건)',
            '수입건수': '수입건수 (건)',
            '수출금액': '수출금액 (천$)',
            '수입금액': '수입금액 (천$)',
            '무역수지': '무역수지 (천$)'}
    df.rename(columns=names, inplace=True)
    df['날짜'] = pd.to_datetime(df['날짜'])

    matcher = ['건수', '금액', '무역']
    all_cols = df.columns.tolist()
    value_cols = [col for col in all_cols if any(
        prcnt in col for prcnt in matcher)]

    for col in value_cols:
        df[col] = pd.to_numeric(df[col].str.replace(',', ''))

    df = _add_units(df)
    # 저장
    conn_db.export_(df[cols_long], '수출입_국가별')
    for sheet in ['DB_수출입_국가별','DB_수출입_국가별_대시보드']:
        conn_db.to_(df[cols_short], sheet, 'import')

# 수출입 전체 취합본
def union_trade_data():
    datasets = {'수출입_신성질별' : '신성질별 수출입',
                '수출입_품목별': '품목별 수출입',
                '수출입_국가별': '국가별전체'}

    df = pd.DataFrame()
    for dataset in datasets.keys():
        temp = conn_db.import_('수출입_신성질별')
        temp['dataset'] = datasets[dataset]
        df = df.append(temp, ignore_index=True)

    # 저장
    conn_db.export_(df,'수출입실적')

# 국가별 신성질별 수출입
@helper.timer
def clean_trady_by_country_new_type():
    folder = conn_db.get_path('국가별_신성질별_수출입')
    all_files = glob(folder + "*.xls")
    df_raw = pd.concat([pd.read_excel(file, skiprows=4) for file in all_files], axis=0).drop_duplicates()

    filt1 = df_raw['성질명'].str.startswith('(')
    filt2 = df_raw['성질명'].str.endswith(')')
    filt3 = df_raw['기간'] != '총계'
    filt = filt1 & filt2 & filt3

    df_raw = df_raw.loc[filt].copy()
    df_raw['중량'] = pd.to_numeric(df_raw['중량'].str.replace(',', ''))
    df_raw['금액'] = pd.to_numeric(df_raw['금액'].str.replace(',', ''))
    trade_types = df_raw['수출입구분'].unique().tolist()

    df = pd.DataFrame()
    for trade_type in trade_types:
        temp = df_raw.loc[df_raw['수출입구분'] == trade_type].copy()
        temp.rename(columns={'중량': trade_type+'중량 (ton)',
                            '금액': trade_type+'금액 (천$)'}, inplace=True)
        temp.drop(columns='수출입구분', inplace=True)
        df = df.append(temp)

    df = df.rename(columns={'기간': '날짜'}).reset_index(drop=True)
    cols = ['날짜', '국가명', '성질명']
    df = df.groupby(cols).agg(sum).reset_index()
    df['성질명'] = df['성질명'].str[1:-1].str.strip()

    # 분류 맵핑
    map_df = common.from_db('Master_수출입품목', '신성질별_short')
    df = map_df.merge(df, left_on='세세분류명', right_on='성질명', how='inner')
    df = df.drop(columns='성질명').rename(columns={'국가명': '국가'})

    df = _add_units(df)

    [df.rename(columns={col: col[:-1]}, inplace=True) for col in df.columns.tolist() if '분류명' in col]
    df['날짜'] = df['날짜'].str.replace('.', '-')

# 수출입실적 HS코드10자리 데이터 전처리
@helper.timer
def clean_data_from_unipass_by_product_10hscode():
    # 다운받을때 마다 오류가 계속 발생해서 수작업으로 jupyter에서 진행후
    # 다운로드폴더에 취합된 '취합본'만 남겨놓고 이 코드 실행

    print('10자리 HS코드 전처리 시작')
    path = conn_db.get_path('10자리 HS코드')

    df = pd.read_pickle(helper.download_folder + "취합본.pkl")

    filt = df['기간'] != '총계'
    df = df[filt]
    df['HS코드_6자리'] = df['품목코드'].str[:6]

    matcher = ['수출', '수입', '무역']
    all_cols = df.columns.tolist()
    value_cols = [col for col in all_cols if any(
        prcnt in col for prcnt in matcher)]

    for col in value_cols:
        df[col] = pd.to_numeric(df[col].str.replace(',', ''))

    # 월별로 별도로 저장
    for date in df['기간'].unique().tolist():
        temp = df[df['기간'] == date].reset_index(drop=True)  # 특정월만 있는 df
        date = date.replace('.', '년')+'월'  # 파일명에 들어갈 날짜
        if len(temp) > 0:
            temp.to_pickle(path + f'HSCODE_10_{date}.pkl')
        else:
            pass

    files = glob(path+'*.pkl')
    df = pd.concat([pd.read_pickle(file) for file in files])
    df = df.drop_duplicates().reset_index(drop=True)

    df = df.loc[df['기간'] != '총계'].copy().drop(columns=['품목명'])
    df = df.rename(columns={'기간': '날짜', '품목코드': 'HS코드_10자리',
                         '수출중량': '수출중량 (ton)', '수입중량': '수입중량 (ton)',
                         '수출금액': '수출금액 (천$)', '수입금액': '수입금액 (천$)',
                         '무역수지': '무역수지 (천$)'})

    df['날짜'] = df['날짜'].str.replace('.', '-')

    trade_types = ['수출', '수입']
    for unit_type in unit_types:
        for trade_type in trade_types:
            df[f'{trade_type}금액 ({unit_type}$)'] = df[f'{trade_type}금액 (천$)'] / \
                            unit_dict[unit_type]
    df['수출ton당 (천$/ton)'] = df['수출금액 (천$)'] / df['수출중량 (ton)']
    df['수입ton당 (천$/ton)'] = df['수입금액 (천$)'] / df['수입중량 (ton)']

    #------------ ------------ ------------
    # drop_cols = ['세번2단위품명','HS코드_2자리', '세번4단위품명', 'HS코드_4자리','HS코드_6자리']
    drop_cols = ['HS코드_2자리', 'HS코드_4자리', 'HS코드_6자리']
    code_map_df = conn_db.from_('Master_수출입품목', '신성질_HS코드품목연계')
    code_map_df.drop(columns=drop_cols, inplace=True)

    df = code_map_df.merge(df, on='HS코드_10자리', how='inner')
    # df = df[['HS코드_6자리', 'HS코드_10자리', '날짜', '수출중량 (ton)', '수입중량 (ton)', '수출금액 (천$)',
    #         '수입금액 (천$)', '무역수지 (천$)', '수출금액 (백만$)', '수입금액 (백만$)',
    #         '수출금액 (억$)', '수입금액 (억$)', '수출ton당 (천$/ton)', '수입ton당 (천$/ton)']]

    path = conn_db.get_path('HSCODE_10_취합본') + 'HSCODE_10_취합본.pkl'
    df.to_pickle(path)
    print('10자리 HS코드 전처리 완료후 저장')

# 수출입 hs코드 신성질별 분류파일 정리용
@helper.timer
def clean_hscode_file():
    '''
    수출입 hs코드 신성질별 분류파일 정리용
    '''
    path = conn_db.get_path('hs코드 신성질별 분류')
    file = path + "2019년_HS_신성질별 성질별 연계(홈페이지 게재용).xlsx"

    # 신성질별 map
    df_map = pd.read_excel(file, encoding='utf-8', skiprows=1,
                           usecols='B:O', dtype='str').drop_duplicates()
    df_map['HS코드_6자리'] = df_map['Unnamed: 1'].str[:6]
    cols = ['세번2단위품명', '세번4단위품명', '대분류코드', '중분류코드',
            '소분류코드', '세분류코드', '세세분류코드']

    df_map = df_map.rename(columns={'Unnamed: 1': 'HS코드_10자리'}).drop(columns=cols)
    df_map['예시_전체'] = df_map['세번10단위품명'] + "(" + df_map['HS코드_10자리'] + ")"

    #저장
    df_map_short = df_map.groupby(['세세분류명'], as_index=False).head(2)
    df_map_short = df_map_short.drop(columns=['HS코드_10자리', '예시_전체'])
    df_map_short.rename(columns={'세번10단위품명': '설명'}, inplace=True)
    cols = ['대분류명', '중분류명', '소분류명', '세분류명', '세세분류명']
    df_map_short = df_map_short.groupby(cols)['설명'].apply(', '.join).reset_index()
    conn_db.to_(df_map_short, 'Master_수출입품목', '신성질별_short')

    #-------------------------------
    cols = ['HS코드_10자리', 'HS코드_6자리', '예시_전체']
    df_map_example = df_map.drop(columns=cols).drop_duplicates()
    df_map_example = df_map_example.groupby(['세세분류명'])['세번10단위품명'].apply(', '.join).reset_index()

    cols = ['HS코드_10자리', '예시_전체', '세번10단위품명']
    df_map_6code = df_map.drop(columns=cols).drop_duplicates()
    df_map_6code = df_map_6code.groupby(['세세분류명'])['HS코드_6자리'].apply(', '.join).reset_index()

    df_map_all = df_map.groupby(['세세분류명'])['예시_전체'].apply(', '.join).reset_index()
    df_map_all = df_map_example.merge(df_map_6code, on='세세분류명').merge(df_map_all, on='세세분류명')
    df_map_all.rename(columns={'세번10단위품명': '예시'}, inplace=True)
    df_map_all['예시'] = [string+' 등' for string in df_map_all['예시'].str.replace(' 기타,', '')]

    #합쳐서 저장--------------------------
    df_map = df_map[['대분류명', '중분류명', '소분류명', '세분류명', '세세분류명']].drop_duplicates()
    df_map = df_map.merge(df_map_all, on='세세분류명')
    conn_db.to_(df_map, 'Master_수출입품목', '신성질별_long')
    print('신성질별 분류 구글시트 업로드 완료')

    # 품목별 map. 품목별로 신성징 분류랑 6자리까지 있는 HS코드랑 합치기
    df = pd.read_excel(file, skiprows=1, usecols='B:O', dtype='str')
    df = df.drop(columns=['대분류코드', '중분류코드', '소분류코드',
                       '세분류코드', '세세분류코드']).drop_duplicates()
    dummy = [df.rename(columns={col: col[:-1]}, inplace=True)
          for col in df.columns.tolist() if '분류명' in col]
    df['HS코드_6자리'] = df['Unnamed: 1'].str[:6]
    df['HS코드_4자리'] = df['Unnamed: 1'].str[:4]
    df['HS코드_2자리'] = df['Unnamed: 1'].str[:2]
    df.rename(columns={'Unnamed: 1': 'HS코드_10자리'}, inplace=True)

    #------- 관세청에서 받은 6자리 HScode명 df와 합치기------------------------
    df_6_code = conn_db.from_('Master_수출입품목', 'HS코드품목_6자리').drop(
        columns={'세번6단위품명(영문)', '적용개시일자'})
    df = df.merge(df_6_code, on='HS코드_6자리', how='left').drop_duplicates()
    df['세번6단위품명'].fillna(df['세번10단위품명'], inplace=True)

    # 컬럼순서
    cols = ['대분류', '중분류', '소분류', '세분류', '세세분류', '세번2단위품명', 'HS코드_2자리',
         '세번4단위품명', 'HS코드_4자리', '세번6단위품명', 'HS코드_6자리', '세번10단위품명', 'HS코드_10자리']

    # 정렬순서
    sort_cols = ['HS코드_2자리', '대분류', '중분류', '소분류', '세분류', '세세분류',
              'HS코드_2자리',  'HS코드_4자리', 'HS코드_6자리', 'HS코드_10자리']

    # 컬럼 순서와 행순서 정렬 후 업로드
    df = df[cols].sort_values(by=sort_cols, ascending=True).reset_index(drop=True)
    conn_db.to_(df, 'Master_수출입품목', '신성질_HS코드품목연계')
    print('품목별 HSCODE표 구글시트 업로드 완료')

# istans에서 받은 코드표 정리
@helper.timer
def istans_hs_code_table():
    df = conn_db.from_('Master_수출입품목', 'istans_원본수정')

    for x in ['ISTANS Code', '10차 KSIC']:
        cols = df.columns.tolist()
        temp = df[x].str.split(' ', expand=True)
        df[x] = df[x].str.split(' ', expand=True)[0]
        df = temp.merge(df, left_on=0, right_on=x, how='left')
        df = df.melt(id_vars=cols, var_name='temp', value_name='Code').dropna(
            subset=['Code']).reset_index(drop=True)
        df = df.drop(columns=['temp', x]).rename(columns={'Code': x})

    cols = ['GRC Code', 'ISTANS Code']
    df_istans_ksic = df.sort_values(by=cols).reset_index(drop=True)
    conn_db.to_(df_istans_ksic, 'Master_수출입품목', 'istans_ksic')

    df_istans_hs = df.drop_duplicates(subset='ISTANS Code').drop(columns='10차 KSIC')
    df_istans_hs = df_istans_hs.sort_values(by=['GRC Code']).reset_index(drop=True)
    df_istans_hs['len'] = df_istans_hs['ISTANS Code'].apply(len)

    # 수출입품목 연게표랑 istans-hs 합치기
    istans_hs = conn_db.from_('Master_수출입품목', 'istans_hs')
    hs_code = conn_db.from_('Master_수출입품목', '신성질_HS코드품목연계')
    hs_code = hs_code.merge(istans_hs, left_on='HS코드_6자리', right_on='hsc', how='inner')
    hs_code.drop(columns=['hsc'], inplace=True)

    df = pd.DataFrame()
    for num in [2, 3, 4]:
        hs_code[f'isc5_{num}'] = hs_code['isc5'].str[:num]
        hs_code_map_col = hs_code[f'isc5_{num}']
        temp = hs_code.merge(df_istans_hs[df_istans_hs['len'] == num],
                       left_on=hs_code_map_col, right_on='ISTANS Code')
        df = df.append(temp)

    for col in 'isc':
        df = helper.drop_column(df, col)
    df = df.drop(columns=['GRC Code', 'len']).reset_index(drop=True)

    conn_db.to_(df, 'Master_수출입품목', 'mapping')
