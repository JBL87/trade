from glob import glob
import pandas as pd
import time
import conn_db, helper

from selenium import webdriver
from selenium.webdriver.support.select import Select
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC

download_folder = helper.download_folder

# 관세청 품목별 수출입 데이터가져오기 (hs코드 10자리)
@helper.timer
def get_data_from_unipass_by_product_10hscode(code_list, start_date, end_date):
    '''
    관세청 품목별 수출입 데이터 hs코드 10자리 까지 다운받는것
    start_date, end_date = 'yyyy.mm'
    end_date를 최근으로 하려면 0 입력
    '''
    # 1. 관세청 홈페이지 들어가기
    driver = webdriver.Chrome()
    driver.get('https://unipass.customs.go.kr/ets/index.do')
    time.sleep(5)

    # 2. 실제 통계 목록 있는 화면으로 들어가기
    selector = '#maincont > div > article > ul > li:nth-child(1) > div > ul.leftArea.listype01 > li:nth-child(3) > a'
    driver.find_element_by_css_selector(selector).click()
    time.sleep(5)

    # 3. 품목별 수출입실적 메뉴 클릭
    selector = '#left > nav > ul > li:nth-child(3) > a'
    driver.find_element_by_css_selector(selector).click()
    time.sleep(5)

    # 품목별 실적용
    by_items_id = '#TRS0104008Q'  # 품목별 실적 전용 ID

    # 4.0 조회기간 설정
    selector = f'{by_items_id}_priodKind > option:nth-child(2)'
    driver.find_element_by_css_selector(selector).click()  # 조회기간 '월'로 변경

    # 4.1 시작날짜 설정
    # 시작날짜 선택 드룹다운 열기
    driver.find_element_by_css_selector(f'{by_items_id}_priodFr').click()
    # 시작날짜 설정
    Select(driver.find_element_by_css_selector(f'{by_items_id}_priodFr')).select_by_visible_text(start_date)

    # 4.2 종료날짜 설정
    # 조회기간 'to' 클릭해서 dropdown 열기
    driver.find_element_by_css_selector(f'{by_items_id}_priodTo').click()
    if end_date == 0:  # 0일 경우 항상 가장 최근 날짜로 선택
        Select(driver.find_element_by_css_selector(f'{by_items_id}_priodTo')).select_by_index(0)
    else:  # parameter로 받은 종료날짜 설정
        Select(driver.find_element_by_css_selector(f'{by_items_id}_priodTo')).select_by_visible_text(end_date)
    # driver.find_element_by_css_selector(f'{by_items_id}_priodTo').click()  # 조회기간 'to' dropdown 닫음

    # 5 품목코드 입력
    for code in code_list:
        # 첫 2자리 코드 입력
        first_code = str(code[:2])
        driver.find_element_by_id('TRS0104008Q_hsSgn02').send_keys(first_code)  # HSCODE 1,2 번째 2자리 입력
        time.sleep(1)

        # 3~4번째 자리 코드 입력
        second_code = str(code[2:4])
        driver.find_element_by_id('TRS0104008Q_hsSgn04').send_keys(second_code)  # HSCODE 3,4 번째 2자리 입력
        time.sleep(1)

        # if len(code)>4: # HSCODE 5,6 번째 2자리 입력. 필요한 경우 입력
        third_code = str(code[-2:])
        driver.find_element_by_id('TRS0104008Q_hsSgn06').send_keys(third_code)
        time.sleep(1)

        # 6 조회버튼클릭
        selector = f'{by_items_id}_fmSearch > div > footer > button'
        driver.find_element_by_css_selector(selector).submit()
        time.sleep(3)

        while True: # 조회되길 기다리기
            selector = '#TRS0104008Q_table > div.blockUI.blockMsg.blockElement > div'
            try:
                driver.find_element_by_css_selector(selector).get_attribute("style") == ''
                time.sleep(1)
            except:
                break
        try:
            driver.switch_to.alert.dismiss()  # 다운로드 경고 창 확인 클릭

        # 7 엑셀다운로드
        except:  
            # 엑셀다운로드
            driver.find_element_by_css_selector(f'{by_items_id}_downExcel_double').send_keys('\n')
            file_count = len(glob(excel_download_folder + "*.xls"))
            time.sleep(5)
            try:  # 데이터가 없다는 경고창이 뜨는 경우
                driver.switch_to.alert.dismiss()  # 다운로드 경고 창 확인 클릭
            except:  # 다운로드 경고창이 안뜰때
                max_wait = 5
                # 다운로드 폴더에 있는 파일 갯수
                while len(glob(excel_download_folder + "*.xls")) == file_count:
                    time.sleep(3)  # 조회결가 나오기 기다리기
                    if max_wait == 0:
                        print('다운로드 안되는 문제 생겨서 멈춤')
                        break
                    else:
                        max_wait -= 1

        # 코드 입력칸 내용 지우기
        driver.find_element_by_id('TRS0104008Q_hsSgn02').clear()
        driver.find_element_by_id('TRS0104008Q_hsSgn04').clear()
        if len(code) > 4:
            driver.find_element_by_id('TRS0104008Q_hsSgn06').clear()
        time.sleep(2)
    driver.quit()

# 신성질별 수출입 실적 다운로드
@helper.timer
def get_data_from_unipass_by_new_product(start_date, end_date=0):
    '''
    신성질별 수출입 실적 다운로드
    start_date, end_date = 'yyyy.mm'
    end_date를 최근으로 하려면 0(숫자) 입력
    '''
    # 1. 관세청 홈페이지 들어가기
    driver = webdriver.Chrome()

    driver.get('https://unipass.customs.go.kr/ets/index.do')
    time.sleep(10)

    # 2. 실제 통계 목록 있는 화면으로 들어가기
    # 신성질별 수출입실적 메뉴 클릭.
    time.sleep(10)
    selector = '#maincont > div > article > ul > li:nth-child(1) > div > ul.leftArea.listype01 > li:nth-child(5) > a'
    driver.find_element_by_css_selector(selector).click()
    time.sleep(10)

    # 조회기간 '월'로 변경
    selector = '#TRS0104011Q_priodKind > option:nth-child(2)'
    driver.find_element_by_css_selector(selector).click()

    # 시작날짜 설정
    driver.find_element_by_css_selector('#TRS0104011Q_priodFr').click()  # 시작날짜 선택 드룹다운 열기
    Select(driver.find_element_by_css_selector('#TRS0104011Q_priodFr')).select_by_visible_text(start_date)  # 시작날짜 설정

    # 종료날짜 설정. 조회기간 'to' 클릭해서 dropdown 열기
    driver.find_element_by_css_selector('#TRS0104011Q_priodTo').click()
    if end_date == 0:
        # 항상 처음 있는 월 선택
        Select(driver.find_element_by_css_selector('#TRS0104011Q_priodTo')).select_by_index(0)
    else: # 종료날짜 설정
        Select(driver.find_element_by_css_selector('TRS0104011Q_priodTo')).select_by_visible_text(end_date)
    driver.find_element_by_css_selector('#TRS0104011Q_detailTmpr').click()  # 세부성질 클릭

    # 수출, 수입 선택후 다운로드
    trade_types = {'expTpcd': '수출',
                'impTpcd': '수입'}
    path = conn_db.get_path('신성질별_수출입_raw')

    for trade_type in trade_types.keys():
        # 수출, 수입 선택
        driver.find_element_by_css_selector(f'#TRS0104011Q_{trade_type}').click()

        # 조회클릭
        selector = '#TRS0104011Q_fmSearch > div > footer > button'
        driver.find_element_by_css_selector(selector).submit()
        time.sleep(30)

        start = len(glob(download_folder+'*.xls'))
        try:  # 다운로드 버튼 클릭
            driver.find_element_by_css_selector(
                '#TRS0104011Q_downExcel_double').send_keys('\n')
        except:  # 다운로드 버튼 재클릭
            time.sleep(5)
            driver.find_element_by_css_selector(
                '#TRS0104011Q_downExcel_double').send_keys('\n')

        while len(glob(download_folder+'*.xls')) == start:
            time.sleep(3)

        old_file = glob(download_folder+'*.xls')[0]
        new_file = path + f'신성질별 {trade_types[trade_type]}실적_{start_date[:4]}.xls'

        shutil.move(src=old_file, dst=new_file)

# 관세청 HScode 6자리 명칭 가져오기
def get_hscode(hscode):
    url = f'https://unipass.customs.go.kr/ets/hmpg/openTRS0107001Q.do?hsSgn={hscode}&hsSgnLen=6'
    driver.get(url)
    time.sleep(3)
     
     # 창열기
    selector = "#TRS0107001Q_table > tbody > tr > td"
    element = driver.find_elements_by_css_selector(selector)

    # 결과 list로 추출
    element_to_list = [row.text for row in element]  

    # 결과가 없는 경우가 있음. 이럴 경우 1줄로 '조회결과없다'고 나옴
    if len(element_to_list) > 1:
        try:
            codes = element_to_list[1::5]  # HS부호
            names_kr = element_to_list[2::5]  # HS부호명
            names_en = element_to_list[3::5]  # HS부호영문명
            apply_date = element_to_list[4::5]  # HS부호적용개시일자
            return pd.DataFrame([codes, names_kr, names_en, apply_date]).T
        except:
            print(f'{hscode} 작업실패')
    else:
        print(f'{hscode} 결과없음')

# 관세청에서 HScode 6자리 명칭 update
@helper.timer
def update_6_hscode_from_unipass():
    '''
    HScode 6자리 명칭 가져와서 구글시트 업로드
    '''
    driver = webdriver.Chrome()
    hs_codes_all = conn_db.from_('Master_수출입품목', '신성질_HS코드품목연계')
    hs_codes_all = hs_codes_all['HS코드_4자리'].unique().tolist()
    
    df = pd.DataFrame()
    for code in hs_codes_all:
        temp = get_hscode(code)
        df = df.append(temp, ignore_index=True)
        time.sleep(2)
    driver.quit()
    
    names = {0:'HS코드_6자리', 
            1: '세번6단위품명',
            2: '세번6단위품명(영문)', 
            3: '적용개시일자'}
    df.rename(columns=names, inplace=True)
    df = df.drop_duplicates().reset_index(drop=True)
    conn_db.to_(df, 'Master_Master_수출입품목', 'HS코드품목_6자리')
