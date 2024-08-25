from functools import wraps
import traceback
import re
from time import sleep
from datetime import datetime
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import sys

sys.path.append('../common_functions')
# import dropbox_function
# import status_sheet
# import scraping_functions
# import chatwork_function
# 価格規制・送料無料条件:
# https://docs.google.com/spreadsheets/d/17shVHYBhQFBxNZcJ9C6VovAi87T6_t2OmOwMoysOGyM/edit#gid=2108787332
print(sys.executable)


S_TIME = "5:55"
E_TIME = "8:05"


def skip_execution_during(start_time, end_time):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            current_time = now.time()

            start_time_obj = datetime.strptime(start_time, "%H:%M").time()
            end_time_obj = datetime.strptime(end_time, "%H:%M").time()

            if start_time_obj <= current_time <= end_time_obj:
                end_datetime = datetime.combine(now.date(), end_time_obj)
                wait_time = (end_datetime - now).total_seconds()
                print(
                    f"Currently in the no-execution window. Waiting for {wait_time} seconds.")
                sleep(wait_time)

            return func(*args, **kwargs)
        return wrapper
    return decorator



@skip_execution_during(S_TIME, E_TIME)
def get_control(driver_self, driver_no, items_url):
    '''取得先ページアクセスコントロール'''
    driver_self.driver[driver_no].get(items_url)
    # 待機
    sleep(driver_self.interval)
    driver_self.wait[driver_no].until(EC.presence_of_all_elements_located)

    # ページ数取得
    get_page_num(driver_self, driver_no)
    # 待機
    sleep(driver_self.interval)
    driver_self.wait[driver_no].until(EC.presence_of_all_elements_located)

    if driver_self.page_num[driver_no] is None:
        return
    
    for page_num in range(driver_self.page_num[driver_no]):
        # ページ移動
        page_transition(driver_self, driver_no, page_num)
        # 取得
        get_item_detail(driver_self, driver_no)



@skip_execution_during(S_TIME, E_TIME)
def get_page_num(driver_self, driver_no):
    """
    ページ数を取得
    """
    if driver_self.now_login[driver_no]:
        for _ in range(driver_self.retry):
            try:
                element_text = driver_self.driver[driver_no].find_element(
                    By.XPATH, '//div[@class="bl_coment-wrap"]/ul/li').text
                if 'ログアウトしました' in element_text:
                    driver_self.restart_driver(driver_no)
                soup = driver_self.return_soup(driver_self.driver[driver_no].page_source)
                if '該当商品がありませんでした' in soup:
                    return

                elem_page_info = soup.find('div', class_='bl_hit-count')
                if elem_page_info is not None:
                    page_info = elem_page_info.get_text()
                    # 数字のみを抽出
                    numbers = re.findall(r'\d+', page_info)
                    driver_self.total_item_count = int(numbers[0])
                    driver_self.page_num[driver_no] = int(
                        numbers[len(numbers) - 1])

                return
            except BaseException:
                if _ == driver_self.retry - 1:
                    driver_self.logger.error('failed to getting page num.')
                    driver_self.logger.error(traceback.format_exc())


@skip_execution_during(S_TIME, E_TIME)
def page_transition(driver_self, driver_no, page_num):
    """
    指定ページへ遷移
    """
    try:
        url = driver_self.driver[driver_no].current_url
        # 'page=' を含む部分を処理
        if 'page=' in url:
            start_index = url.find('page=')
            end_index = url.find('&', start_index)
            if end_index == -1:
                end_index = len(url)
            else:
                end_index += 1
            # 'page=' を削除
            current_url = url[:start_index] + url[end_index:]
        else:
            current_url = url

        current_url_list = current_url.split('filtering_search?')
        access_url = f'{current_url_list[0]}filtering_search?page={page_num + 1}&{current_url_list[1]}'
        driver_self.driver[driver_no].get(access_url)
        # 待機
        sleep(driver_self.interval)
        driver_self.wait[driver_no].until(EC.presence_of_all_elements_located)

        # 「長時間操作が無かったため」の要素を探す
        try:
            # モーダルの要素を取得
            modal = driver_self.driver[driver_no].find_element(
                By.CSS_SELECTOR, "div.modal.el_modal-wrap[data-modalindex='99']")
            # モーダルが表示されているかどうかをチェック
            if modal.is_displayed():
                driver_self.restart_driver_cur_page(driver_no)
        except Exception:
            # モーダルの要素がページに存在しない場合
            print("ポップアップの要素が見つかりませんでした。")
        return True

    except IndexError:
        # URL作成失敗
        if driver_self.price_switch[driver_no] is False:
            driver_self.price_search_count[driver_no]['flag'] = True
        return False


@skip_execution_during(S_TIME, E_TIME)
def get_item_detail(driver_self, driver_no):
    """
    商品詳細情報を取得する
    """

    for count in range(driver_self.retry):
        try:
            # パース
            soup = driver_self.return_soup(driver_self.driver[driver_no].page_source)
            item_wrapper = soup.find_all(class_='bl_item-area')
            for i in range(0, len(item_wrapper)):
                # 画像 <figure class="el_item-img">
                figure_box = item_wrapper[i].find(
                    'figure', class_='el_item-img')
                img_url = figure_box.find('img')['src']
                # URL,商品名,商品コード,品番,JAN <section class="bl_item-spec">
                section_box = item_wrapper[i].find(
                    'section', class_='bl_item-spec')
                # 商品名とメーカー名分割
                name_str = section_box.find(
                    'h2', class_='el_lv02headline-ver03').get_text()
                clean_string = name_str.strip().replace('\n', '').strip()
                split_string = re.split(r'／', clean_string, maxsplit=1)
                if len(split_string) >= 2:
                    maker = split_string[0]
                    item_name = split_string[1]
                item_url = section_box.find('a')['href']
                section_table = section_box.find(
                    'dl', class_='el_info-list')
                section_table = section_table.get_text().split('\n')
                for index, obj in enumerate(section_table):
                    if '商品コード' in obj:
                        order_code = section_table[index + 1]
                    elif '品番' in obj:
                        item_code = section_table[index + 1]
                    elif 'JANコード' in obj:
                        jan = section_table[index + 1]

                if jan in list(driver_self.item_info["JAN"]):
                    continue

                # 定価,仕切価格,最低出荷単位,在庫,数量 <section class="bl_item-spec">
                item_box = item_wrapper[i].find(
                    'div', class_='bl_item-box')
                item_table = item_box.find('dl', class_='bl_item-data')
                item_table = item_table.get_text().split('\n')
                for index, obj in enumerate(item_table):
                    if '定価' in obj:
                        price = item_table[index + 1]
                        price = price.replace(",", "")
                    elif '仕切価格' in obj:
                        jodai = item_table[index + 1]
                        jodai = jodai.replace(",", "")
                    elif '最低出荷単位' in obj:
                        lot_num = item_table[index + 1]

                stock_table = item_box.find(
                    'dl', class_='bl_stock-quantity')
                stock_table = stock_table.get_text().split('\n')
                for index, obj in enumerate(stock_table):
                    if '在庫' in obj:
                        stock = stock_table[index + 1]

                cart_button = item_wrapper[i].find(
                    'button', class_='cart_add el_cart-add el_box-style el_bg-red')
                if cart_button is None:
                    select = '廃番'
                else:
                    if 'カートに入れる' in cart_button.get_text():
                        select = '選択'
                    elif '廃番' in cart_button.get_text():
                        select = '廃番'
                    else:
                        select = '廃番'

                item_dict = {
                    'JAN': jan,
                    '品番': item_code,
                    '商品名': item_name,
                    '税抜き定価': price,
                    '税抜き仕入れ値': jodai,
                    '発注単位': lot_num,
                    '在庫数': stock,
                    '画像URL': img_url,
                    '商品URL': item_url,
                    'メーカー名': maker,
                    '注文番号': order_code,
                }

                if select == '選択':
                    # データ格納
                    driver_self.item_info.append(item_dict, ignore_index=True)
                    # データをキューに追加
                    driver_self.csv_data_queue.put(item_dict)
                else:
                    print(f"廃盤:\n{item_dict}")
            return

        except BaseException:
            driver_self.logger.error(traceback.format_exc())
            if count == driver_self.retry - 1:
                driver_self.logger.error(
                    'failed to get item detail.\nurl: {}'.format(
                        driver_self.driver[driver_no].current_url))
                try:
                    print('i: {}'.format(i))
                    print(item_name)
                except BaseException:
                    pass
                # driver_self.driver_ready[driver_no] = True
                return

            driver_self.restart_driver(driver_no)
            continue