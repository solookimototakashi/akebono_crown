from functools import wraps
from queue import Queue
import string
from error_class import ErrorHandlingClass
import requests
import json
import pandas as pd
import os
import re
import threading
import sqlite3
import random
from cmath import nan
from time import sleep
from bs4 import BeautifulSoup
from datetime import datetime
from logging import getLogger, StreamHandler, Formatter
from logging.handlers import RotatingFileHandler
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from tqdm import tqdm
import copy
import shutil
import sys
import main_create_item_list
from multiprocessing import Lock
from urllib.parse import urlparse, parse_qs, urlencode, unquote

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


class AkebonoCrown():
    def __init__(self, config_path):
        self.test = True
        # False = サーバー
        self.local_flag = True
        # 設定読み込み
        try:
            config = json.load(open(config_path, "r", encoding="utf-8"))
        except FileNotFoundError as e:
            print("[ERROR] Config file is not found.")
            raise e
        except ValueError as e:
            print("[ERROR] Json file is invalid.")
            raise e

        # LINE設定読み込み
        try:
            config_LINE = json.load(
                open(
                    'settings_LINE.json',
                    "r",
                    encoding="utf-8"))
            self.line_access_token = config_LINE['access_token']
        except BaseException:
            self.line_access_token = ''

        # settingの内容を取得
        self.url = config['url']
        self.id = config['id']
        self.pw = config['pw']
        self.db_name = config['db_name']
        self.interval = config['access_interval_min']
        self.log_level = config["log_level"]
        self.driver_interval = config["driver_restart_interval"]
        self.retry = config["retry_count"]
        self.retry_limit = config["retry_limit"]
        self.retry_max_limit = config["retry_max_limit"]
        self.lock = False
        # 並行処理の数
        self.thread_num = 1  # config["thread_num"]
        # self.thread_num = 2 # config["thread_num"]

        # chromeドライバー
        self.driver = []
        self.wait = []
        # ロガー
        self.logger = None
        # データベース
        self.conn = None
        self.cur = None
        # テーブル名
        self.table_name = 'akebono'        
        # DBにあったdf
        self.db_df = self.read_db()
        # Lockオブジェクトの初期化（クラスの一部として定義するか、グローバルとして定義して渡す）
        self.process_lock = Lock()
        # 取得データ格納用
        self.columns = [
            "品番",
            "JAN",
            "商品名",
            "税抜き定価",
            "税抜き仕入れ値",
            "受注生産",
            "取寄せ",
            "発注単位",
            "在庫数",
            "プロパー用在庫",
            "画像URL",
            "商品URL",
            "メーカー名",
            "注文番号"
        ]
        self.df_columns = [
            "JAN",
            "品番",
            "商品名",
            "税抜き定価",
            "税抜き仕入れ値",
            "発注単位",
            "在庫数",
            "画像URL",
            "商品URL",
            "メーカー名",
            "注文番号"
        ]
        # df
        self.item_df = pd.DataFrame()
        # ログ
        self.log_info = {
            'start': None,    # 開始時間
            'end': None,      # 終了時間
            'error': []
        }
        # ログインできてるかのシグナル
        self.now_login = {}
        # ドライバ使用回数
        self.driver_count = []
        # ドライバリトライ回数
        self.driver_retry_count = []
        # 各ドライバの処理終了シグナル
        self.driver_ready = []

        # ページ数
        self.page_num = {item: None for item in range(self.thread_num)}
        # 商品数
        self.total_item_count = 0
        # 商品情報
        self.item_info = []
        # 商品ページURLリスト
        self.item_list = []
        # エラーが発生したurl
        self.error_page_list = []
        # 商品検索URL
        self.base_url = None
        # ドロップダウンリスト
        self.maker_list = []
        # 価格検索情報
        self.price_search_count = {item: {
            'count': 0,
            'flag': False,
        } for item in range(self.thread_num)}
        # 価格変動検索フラグ
        self.price_switch = {item: False for item in range(self.thread_num)}
        # 商品詳細保存フラグ
        self.write_flag = False
        self.base_price = {item: [0, 0] for item in range(self.thread_num)}
        self.price_stock = {item: [0, 0] for item in range(self.thread_num)}
        self.target_price = {item: [0, 0] for item in range(self.thread_num)}
        self.change_page = {item: None for item in range(self.thread_num)}
        self.multiplication = {item: 2 for item in range(self.thread_num)}
        self.down_count = {item: 0 for item in range(self.thread_num)}
        # 英字のリスト
        alphabet = list(string.ascii_uppercase)  # 英字の大文字のみ

        # 数字のリスト
        numbers = list(string.digits)  # 数字

        # ひらがなのリスト（小文字を除く）
        hiragana_start = ord('あ')
        hiragana_end = ord('ん')
        small_hiragana_codes = [
            0x3041,
            0x3043,
            0x3045,
            0x3047,
            0x3049,
            0x3063,
            0x3083,
            0x3085,
            0x3087,
            0x308E]
        exclude_hiragana_codes = [
            ord('が'),
            ord('ぎ'),
            ord('ぐ'),
            ord('げ'),
            ord('ご'),
            ord('ざ'),
            ord('じ'),
            ord('ず'),
            ord('ぜ'),
            ord('ぞ'),
            ord('だ'),
            ord('ぢ'),
            ord('づ'),
            ord('で'),
            ord('ど'),
            ord('ば'),
            ord('び'),
            ord('ぶ'),
            ord('べ'),
            ord('ぼ'),
            ord('ぱ'),
            ord('ぴ'),
            ord('ぷ'),
            ord('ぺ'),
            ord('ぽ')]
        self.hiragana = [
            chr(code) for code in range(
                hiragana_start,
                hiragana_end +
                1) if code not in small_hiragana_codes and code not in exclude_hiragana_codes]
        # 結合して全ての文字のリストを作成
        self.characters = alphabet + numbers  # + hiragana
        self.closed_manufacturers_list = []
        self.closed_url_list = []
        self.stop_event = threading.Event()
        self.max_search_price = 10000000
        self.hiragana_flow_ready = [True for _ in range(self.thread_num)]
        # スレッドセーフなキューを作成
        self.csv_data_queue = Queue()
        self.page_num = {item: None for item in range(self.thread_num)}
        self.upper_price = {item: 0 for item in range(self.thread_num)}
        self.down_plice_list = {item: [] for item in range(self.thread_num)}
        self.get_lock = {item: False for item in range(self.thread_num)}
        self.up_count = {item: 2 for item in range(self.thread_num)}

    def initialize_logger(self):
        """
        ロガーを初期化します.
        """
        # 古いログが7ファイル以上あれば削除
        # scraping_functions.remove_old_log("./log", 7)
        # インスタンス化
        self.logger = getLogger(__name__)
        self.logger.setLevel(self.log_level)

        # フォーマットの設定
        formatter = Formatter(
            fmt="[%(levelname)s] (%(asctime)s) %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S")

        # streamHandlerの設定
        stream_handler = StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

        # fileHandlerの設定
        dt_now = datetime.now()
        file_name = dt_now.strftime('%Y%m%d_%H%M%S')
        file_handler = RotatingFileHandler(
            filename=os.path.join(
                'log',
                f"{file_name}.log"),
            maxBytes=1024 * 1024 * 5,
            backupCount=30)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(self.log_level)
        self.logger.addHandler(file_handler)

        self.logger.debug("logger initialized.")

    def initialize_driver(self):
        """
        chromeドライバを初期化します.
        """
        self.logger.debug("_initialize_driver start.")
        # chromeドライバインストール
        options = webdriver.ChromeOptions()
        # 無意味なエラーログを非表示
        options.add_experimental_option(
            'excludeSwitches', [
                'enable-logging', 'enable-automation'])
        options.use_chromium = True
        options.add_argument('--disable-gpu')
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-browser-side-navigation")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument('--incognito')
        # 2024-04-21 Oshima
        options.add_argument('--disable-background-networking')
        # # UA
        user_agent = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0']
        UA = user_agent[random.randrange(0, len(user_agent), 1)]
        user_profile = r'C:\Users\racke\AppData\Local\Google\Chrome\User Data\Default'
        # user_profile = r'C:\Users\solookimoto\AppData\Local\Google\Chrome\User Data\Default'
        options.add_argument('--user-agent=' + UA)

        if self.test is True:
            # options.add_argument('--headless=new')
            try:
                # driver = webdriver.Chrome(executable_path=r"./chromedriver.exe", options=options)
                driver = webdriver.Chrome(options=options)
            except Exception as e:
                print(e)
        else:
            # headlessモード
            options.add_argument('--headless')
            if self.local_flag:
                # ローカルドライバー
                filepath = r'./phantomjs-2.1.1-windows/bin/phantomjs.exe'
            else:
                # サーバードライバー
                filepath = r'/home/xs325544/python/scraping/PhantomJS/phantomjs_2/bin/phantomjs'

            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
            driver = webdriver.PhantomJS(
                executable_path=filepath,
                service_args=[
                    '--ignore-ssl-errors=true',
                    '--ssl-protocol=any'],
                desired_capabilities={
                    'phantomjs.page.settings.userAgent': user_agent})

        driver.set_window_size(980, 1280)
        wait = WebDriverWait(driver, 30)
        self.logger.debug("_initialize_driver end.")
        return driver, wait

    @skip_execution_during(S_TIME, E_TIME)
    def restart_driver(self, driver_no):
        """指定したdriver_noのドライバのみ再起動"""

        self.logger.debug('restart driver. 【driver_no: {}】'.format(driver_no))
        for _ in range(self.retry):
            try:
                self.change_page[driver_no] = None
                self.driver[driver_no].close()
                self.driver[driver_no].quit()
                sleep(self.interval)
                self.driver[driver_no], self.wait[driver_no] = self.initialize_driver()
                self.login(driver_no)
                break
            except Exception as e:
                self.log_info['error'].append(
                    ["driver restart failed", "driver:" + str(driver_no)])

    @skip_execution_during(S_TIME, E_TIME)
    def restart_driver_cur_page(self, driver_no):
        """指定したdriver_noのドライバのみ再起動"""
        try:
            self.logger.debug(
                'restart_driver_cur_page. 【driver_no: {}】'.format(driver_no))
            url = self.driver[driver_no].current_url
            self.driver[driver_no].close()
            self.driver[driver_no].quit()
            sleep(self.interval)
            self.driver[driver_no], self.wait[driver_no] = self.initialize_driver()
            self.login(driver_no)
            self.driver[driver_no].get(url)
            # 待機
            sleep(3)
            self.wait[driver_no].until(EC.presence_of_all_elements_located)
            self.driver_count[driver_no] = 0
        except Exception as e:
            self.driver_retry_count[driver_no] += 1
            self.logger.debug(
                'restart_driver_cur_page: {}】success to initialize.'.format(driver_no))

    def initialize(self):
        # ロガー準備
        self.initialize_logger()
        # 開始時間
        str_now = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        self.logger.info(f"start: {str_now}")
        self.log_info['start'] = str_now
        # status_sheet.start("アケボノクラウン新商品取得")
        # データベース接続
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()
        # chromeドライバー準備
        self.logger.debug('multi threading num is {}'.format(self.thread_num))
        for i in range(self.thread_num):
            driver, wait = self.initialize_driver()
            self.driver.append(driver)
            self.wait.append(wait)
            self.login(i)
            self.driver_count.append(0)
            self.driver_retry_count.append(0)
            self.logger.debug(
                '【driver_no: {}】success to initialize.'.format(i))

    def login(self, driver_no):
        """ログインする（webdriver）"""
        for _ in range(self.retry):
            try:
                # ログインページにアクセス
                self.driver[driver_no].get(self.url)
                # 待機
                sleep(3)
                self.wait[driver_no].until(EC.presence_of_all_elements_located)
                # ログインID
                elem_login_id = self.driver[driver_no].find_element(
                    By.NAME, "user_id")
                elem_login_id.send_keys(self.id)
                # ログインPass
                elem_login_pass = self.driver[driver_no].find_element(
                    By.NAME, "password")
                elem_login_pass.send_keys(self.pw)
                # ログインボタン
                elem_login_btn = self.driver[driver_no].find_element(
                    By.CSS_SELECTOR, 'button.el_box-style.el_bg-red.el_text-white.login')
                elem_login_btn.click()
                # 待機
                sleep(self.interval)
                self.wait[driver_no].until(EC.presence_of_all_elements_located)
                # 詳細検索ボタン
                elem_search_btn = self.driver[driver_no].find_element(
                    By.XPATH, "//button[contains(., '詳細検索')]")
                elem_search_btn.click()
                # 待機
                sleep(self.interval)
                self.wait[driver_no].until(EC.presence_of_all_elements_located)
                # ログインシグナル
                self.now_login[driver_no] = True
                return
            except Exception as e:
                self.DEV_send_img_Line('ログイン失敗', 'img.png')
                if _ == self.retry - 1:
                    self.logger.error(
                        '【driver_no: {}】failed to login. '.format(driver_no))
                    self.logger.error(e)
                    self.now_login[driver_no] = False

    # データベース操作###############################################################################################################################
    def read_db(self):
        """
        引数TableをDataFrame化
        """
        # データベース接続
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()                
        table_check = pd.read_sql(f'SELECT COUNT(*) FROM sqlite_master WHERE TYPE=\'table\' AND name=\'{self.table_name}\'', self.conn).iloc[0, 0]
        if table_check:
            db_df = pd.read_sql(f'SELECT * FROM {self.table_name}', self.conn)
            return db_df
        else:
            self.logger.debug("SQLite table none")
            return pd.DataFrame()

    # ############################################################################################################################################  

    @skip_execution_during(S_TIME, E_TIME)
    def make_thread(self, p_no, count):
        """各情報を取得するためのシーケンシャルな処理を実行する"""
        try:
            driver_no = 0

            total_items = len(self.maker_list)
            items_per_process = total_items // count
            start_index = p_no * items_per_process
            end_index = start_index + items_per_process if p_no < count - 1 else total_items
            progress_bar = tqdm(total=end_index - start_index, desc=f'ProcessNo:{p_no}')
            
            for index in range(start_index, end_index):
                if self.stop_event.is_set():
                    break
                maker = self.maker_list[index]
                # メーカーが取得済みリストに存在しない場合、処理を行う
                if maker not in self.closed_manufacturers_list:
                    try:
                        # メーカーごとにデータを処理
                        self.get_action(driver_no, maker)
                        self.DEV_output_file(
                            pd.DataFrame(
                                self.item_list,
                                columns=["URL"]),
                            f"item_list_{p_no}")
                        # 取得済みメーカーとして登録
                        self.closed_manufacturers_list.append(maker)
                        closed_manufacturers_list = pd.DataFrame(
                            self.closed_manufacturers_list)
                        closed_manufacturers_list.to_csv(
                            f'./interrupted_data_export_{p_no}.csv',
                            header=False,
                            encoding='cp932',
                            errors='ignore',
                            index=False)
                    except Exception as e:
                        print(f"Error processing maker {maker}: {e}")

                if self.lock:
                    raise Exception("retry_locked_error")
                # 処理の間隔を持たせる
                sleep(self.interval)
                progress_bar.update(1)

            progress_bar.close()

            self.item_info['品番'] = self.item_info['品番'].astype(str)
            self.item_info.drop_duplicates(subset=['品番'], inplace=True)
            self.db_df['品番'] = self.db_df['品番'].astype(str)                
            self.merge_df = self.item_info[~self.item_info['品番'].isin(self.db_df['品番'])]
            self.item_list = self.merge_df['商品URL'].to_list()

            self.get_inner_item(p_no, count)
            for items_url in tqdm(self.item_list):
                if items_url not in self.closed_url_list:
                    main_create_item_list.get_control(self, driver_no, items_url) 
                    self.closed_url_list.append(items_url) 
                    closed_url_list = pd.DataFrame(
                        self.closed_url_list)
                    closed_url_list.to_csv(
                        f'./interrupted_url_export_{p_no}.csv',
                        header=False,
                        encoding='cp932',
                        errors='ignore',
                        index=False)                    

        except Exception as e:
            self.logger.error(f'{e}')

    def get_inner_item(self, p_no, count):
        """商品詳細取得"""
        total_items = len(self.item_list)
        # items_per_process = total_items // count
        # # start_index = p_no * items_per_process
        # # end_index = start_index + items_per_process if p_no < count - 1 else total_items
        progress_bar = tqdm(total=total_items, desc=f'ProcessNo:{p_no}')
        
        for url_index in range(total_items):
            try:
                driver_no = 0
                items_url = self.item_list[url_index]  # 現在のアイテムを取得
                get_inner = self.closed_url_list

                if items_url not in get_inner:
                    main_create_item_list.get_control(self, driver_no, items_url)
                    
                    # ロックを使用してリストへの追加とファイルへの書き込みを排他制御
                    with self.process_lock:
                        self.closed_url_list.append(items_url)
                        # CSVへの保存
                        closed_url_list_df = pd.DataFrame(self.closed_url_list)
                        closed_url_list_df.to_csv(
                            f'./interrupted_url_export_{p_no}.csv',
                            header=False,
                            encoding='cp932',
                            errors='ignore',
                            index=False)
            except TimeoutException as e:
                self.logger.error(e)
                self.driver_retry_count[driver_no] += 1
                if self.driver_retry_count[driver_no] > self.retry_max_limit:
                    self.lock = True
                    return
                else:                
                    self.restart_driver_cur_page(driver_no)
                    main_create_item_list.get_control(self, driver_no, items_url)
                    
                    # ロックを使用してリストへの追加とファイルへの書き込みを排他制御
                    with self.process_lock:
                        self.closed_url_list.append(items_url)
                        # CSVへの保存
                        closed_url_list_df = pd.DataFrame(self.closed_url_list)
                        closed_url_list_df.to_csv(
                            f'./interrupted_url_export_{p_no}.csv',
                            header=False,
                            encoding='cp932',
                            errors='ignore',
                            index=False)
                    continue
            except Exception as e:
                self.logger.error(e)
                self.driver_retry_count[driver_no] += 1
                if self.driver_retry_count[driver_no] > self.retry_max_limit:
                    self.lock = True
                    return
                else:                
                    self.restart_driver_cur_page(driver_no)
                    main_create_item_list.get_control(self, driver_no, items_url)
                    
                    # ロックを使用してリストへの追加とファイルへの書き込みを排他制御
                    with self.process_lock:
                        self.closed_url_list.append(items_url)
                        # CSVへの保存
                        closed_url_list_df = pd.DataFrame(self.closed_url_list)
                        closed_url_list_df.to_csv(
                            f'./interrupted_url_export_{p_no}.csv',
                            header=False,
                            encoding='cp932',
                            errors='ignore',
                            index=False)
                    continue

            progress_bar.update(1)

        progress_bar.close()


    @skip_execution_during(S_TIME, E_TIME)
    def get_action(self, driver_no, maker):
        """
        商品情報取得フロー
        """
        while True:
            try:
                while self.price_search_count[driver_no]['flag'] is False:

                    if self.stop_event.is_set():
                        break

                    self.change_page[driver_no] = self.transition_frame(
                        driver_no, maker, None, None)
                    if self.change_page[driver_no] == '価格検索開始':
                        self.price_switch[driver_no] = True
                    elif self.change_page[driver_no] == "価格検索継続":
                        if "search_error" not in self.driver[
                                driver_no].current_url and 'tryangle/search_detail' not in self.driver[driver_no].current_url:
                            self.logger.info(
                                f'driver:{driver_no}_{maker}_価格帯:{self.base_price[driver_no]}_取得リスト格納')
                            self.item_list.append(
                                self.driver[driver_no].current_url)
                        self.process_search_continuation(driver_no, maker)
                        self.multiplication[driver_no] += 1
                    elif self.change_page[driver_no] == "価格検索継続_減額":
                        if "search_error" not in self.driver[
                                driver_no].current_url and 'tryangle/search_detail' not in self.driver[driver_no].current_url:
                            self.logger.info(
                                f'driver:{driver_no}_{maker}_価格帯:{self.base_price[driver_no]}_取得リスト格納')
                            self.item_list.append(
                                self.driver[driver_no].current_url)
                        self.process_search_continuation_discount(
                            driver_no, maker)
                    elif self.change_page[driver_no] == "価格検索継続_品番検索追加":
                        # self.item_list.append(self.driver[driver_no].current_url)
                        self.process_search_continuation_add_item_search(
                            driver_no, maker)
                    elif self.change_page[driver_no] in ["価格検索不要", "価格検索限度額終了"]:
                        if "search_error" not in self.driver[
                                driver_no].current_url and 'tryangle/search_detail' not in self.driver[driver_no].current_url:
                            self.logger.info(
                                f'driver:{driver_no}_{maker}_価格帯:{self.base_price[driver_no]}_取得リスト格納')
                            self.item_list.append(
                                self.driver[driver_no].current_url)
                        self.process_search_not_required_or_limit_reached(
                            driver_no, maker)
                    elif self.change_page[driver_no] == "取得不要":
                        self.price_switch[driver_no] = False
                        self.price_search_count[driver_no]['flag'] = True
                        self.logger.info(f'driver:{driver_no}_{maker}_取得不要')
                    if self.stop_event.is_set():
                        return

                # 価格帯リセット
                self.base_price[driver_no] = [0, 0]
                self.price_search_count[driver_no]['flag'] = False
                self.price_switch[driver_no] = False
                self.logger.info(f'メーカー名:{maker}_取得完了')

                return
            except Exception as e:
                self.logger.error(e)
                # リトライ処理
                self.driver_count[driver_no] += 1
                if self.driver_count[driver_no] > self.retry_limit:
                    if self.driver_retry_count[driver_no] > self.retry_max_limit:
                        # self.driver_ready[driver_no] = True
                        self.lock = True
                        return
                    else:
                        self.restart_driver_cur_page(driver_no)

    @skip_execution_during(S_TIME, E_TIME)
    def process_search_continuation(self, driver_no, maker):
        '''価格検索継続'''
        try:
            self.price_switch[driver_no] = True
            self.transition_frame(driver_no, maker, None, None)
            # 価格帯変更
            self.base_price[driver_no][0] = self.base_price[driver_no][1] + 1
            self.base_price[driver_no][1] = min(int(
                self.base_price[driver_no][1] * self.multiplication[driver_no]), self.max_search_price)
            self.price_search_count[driver_no]['count'] += 1
            if self.base_price[driver_no][1] > self.max_search_price:
                self.base_price[driver_no][1] = self.max_search_price
        except Exception as e:
            self.logger.error(e)

    @skip_execution_during(S_TIME, E_TIME)
    def process_search_continuation_discount(self, driver_no, maker):
        '''価格検索継続_減額'''
        try:
            while True:
                self.down_count[driver_no] += 1
                # 価格帯変更減額
                upper_price = int(self.base_price[driver_no][1] * 0.5)
                if upper_price < self.base_price[driver_no][0]:
                    upper_price = self.base_price[driver_no][0] + 10
                self.base_price[driver_no][1] = upper_price
                self.change_page[driver_no] = self.transition_frame(
                    driver_no, maker, None, None)
                if self.change_page[driver_no] != "価格検索継続_減額":
                    break
            i = i - i
        except Exception as e:
            self.logger.error(e)

    @skip_execution_during(S_TIME, E_TIME)
    def process_search_continuation_add_item_search(self, driver_no, maker):
        '''価格検索継続_品番検索追加'''
        try:
            # self.characters = ['X','Y','Z']
            # multiplicationがNoneの場合にデフォルト値を設定
            # charactersに対してループ
            for _, chara in enumerate(self.characters):
                self.change_page[driver_no] = self.transition_frame(
                    driver_no, maker, chara, None)
                # 価格検索継続_商品名検索追加の場合の処理
                if self.change_page[driver_no] == "価格検索継続_商品名検索追加":
                    sleep(self.interval)
                    hiragana_search = self.product_name_hiragana_search(
                        maker, chara, driver_no)
                    if hiragana_search is True:
                        self.multiplication[driver_no] = 2
                    else:
                        self.logger.info(
                            f'driver:{driver_no}_{maker}_品番:{chara}_価格帯:{self.base_price[driver_no]}_ひらがな検索失敗')

                # 価格検索不要 or 価格検索継続_品番検索追加の場合の処理
                elif self.change_page[driver_no] in ["価格検索不要", "価格検索継続_品番検索追加"]:
                    self.logger.info(
                        f'driver:{driver_no}_{maker}_品番:{chara}_価格帯:{self.base_price[driver_no]}_取得不要')
                    continue
                else:
                    if "search_error" not in self.driver[
                            driver_no].current_url and 'tryangle/search_detail' not in self.driver[driver_no].current_url:
                        self.logger.info(
                            f'driver:{driver_no}_{maker}_品番:{chara}_価格帯:{self.base_price[driver_no]}_取得リスト格納')
                        self.item_list.append(
                            self.driver[driver_no].current_url)
                    else:
                        pass

            # 価格帯変更
            self.base_price[driver_no][0] = self.base_price[driver_no][1] + 1
            self.base_price[driver_no][1] = min(int(
                self.base_price[driver_no][1] * self.multiplication[driver_no]), self.max_search_price)
            self.price_search_count[driver_no]['count'] += 1
            self.multiplication[driver_no] += 1
        except Exception as e:
            self.logger.error(e)

    @skip_execution_during(S_TIME, E_TIME)
    def process_search_not_required_or_limit_reached(self, driver_no, maker):
        '''"価格検索不要", "価格検索限度額終了"'''
        try:
            self.price_switch[driver_no] = False
            self.price_search_count[driver_no]['flag'] = True
        except Exception as e:
            self.logger.error(e)

    def normalize_url(self, url):
        """ソートしたパラメータを再エンコード"""
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)        
        sorted_query_params = sorted((k, unquote(v[0])) for k, v in query_params.items())
        normalized_query = urlencode(sorted_query_params, doseq=True)
        normalized_url = f'{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{normalized_query}'
        return normalized_url

    def url_in_list(self, url):
        """URLパラメータの並びを無視して取得済判定"""
        normalized_url = self.normalize_url(url)
        for item in self.item_list:
            if self.normalize_url(item) == normalized_url:
                return False
        return True

    @skip_execution_during(S_TIME, E_TIME)
    def transition_frame(self, driver_no, maker, part_number, hiragana):
        """
        詳細検索ページにアクセス
        """
        if self.price_switch[driver_no] is False:
            url = f'https://akebonocrown.co.jp/tryangle/productsearch/filtering_search?maker_name={maker}'
            if self.url_in_list(url):
                self.driver[driver_no].get(url)
                # 待機
                sleep(3)
                self.wait[driver_no].until(EC.presence_of_all_elements_located)
        else:
            if part_number is None:
                if self.base_price[driver_no][0] >= self.base_price[driver_no][1]:
                    self.base_price[driver_no] = [
                        self.base_price[driver_no][1], int(
                            self.base_price[driver_no][1]) + 50]
                url = f'https://akebonocrown.co.jp/tryangle/productsearch/filtering_search?maker_name={maker}&base_price_over={self.base_price[driver_no][0]}&base_price_under={self.base_price[driver_no][1]}'
                if self.url_in_list(url):
                    self.driver[driver_no].get(url)
                    # 待機
                    sleep(3)
                    self.wait[driver_no].until(EC.presence_of_all_elements_located)
                else:
                    return "価格検索継続"
            else:
                if hiragana is None:
                    url = f'https://akebonocrown.co.jp/tryangle/productsearch/filtering_search?maker_name={maker}&part_number={part_number}&base_price_over={self.base_price[driver_no][0]}&base_price_under={self.base_price[driver_no][1]}'
                    if self.url_in_list(url):
                        self.driver[driver_no].get(url)
                        # 待機
                        sleep(3)
                        self.wait[driver_no].until(
                            EC.presence_of_all_elements_located)
                    else:
                        return "価格検索継続"                        
                else:
                    url = f'https://akebonocrown.co.jp/tryangle/productsearch/filtering_search?part_number={part_number}&maker_name={maker}&haiban=1&base_price_over={self.base_price[driver_no][0]}&base_price_under={self.base_price[driver_no][1]}&keyword={hiragana}&keyword_pattern=2'
                    if self.url_in_list(url):
                        self.driver[driver_no].get(url)
                        # 待機
                        sleep(3)
                        self.wait[driver_no].until(
                            EC.presence_of_all_elements_located)
                    else:
                        return "価格検索継続"                        

        # 検索結果の判定1
        soup = self.return_soup(self.driver[driver_no].page_source)
        # 取得結果処理分け
        if self.base_price[driver_no][1] > self.max_search_price:
            return "価格検索限度額終了"
        if '該当商品が多すぎるので検索条件を追加してください' in soup.text:
            if self.price_switch[driver_no] is False:
                return "価格検索開始"
            else:
                self.price_search_count[driver_no]['count'] += 1
                if part_number is None:
                    if self.price_search_count[driver_no]['count'] >= 1:
                        return "価格検索継続_品番検索追加"
                else:
                    if hiragana is None:
                        return "価格検索継続_商品名検索追加"
                    else:
                        return "価格検索継続_商品名検索_減額"
        elif '該当商品がありませんでした' in soup.text:
            if self.price_switch[driver_no] is False:
                return "取得不要"
            else:
                self.price_search_count[driver_no]['count'] += 1
                if part_number is None:
                    if self.price_search_count[driver_no]['count'] >= 1:
                        if self.price_switch[driver_no] is False:
                            return "価格検索継続_品番検索追加"
                        else:
                            return "価格検索継続"
                else:
                    if hiragana is None:
                        return "価格検索継続_品番検索追加"
                    else:
                        return "価格検索継続_商品名検索_減額"
        else:
            if self.price_switch[driver_no] is False:
                return "価格検索不要"
            else:
                return "価格検索継続"


    @skip_execution_during(S_TIME, E_TIME)
    def create_maker(self, driver, wait):
        """
        メーカー名ドロップダウンリストの作成
        """
        try:
            if self.test is True:
                maker_list_df = pd.read_csv(
                    './DEVELOPMENT/maker_list.csv', encoding='cp932').values
                self.maker_list = [item[0] for item in maker_list_df]

            else:
                start = ord('あ')
                end = ord('ん')
                hiragana_list = [chr(code) for code in range(start, end + 1)]

                for hiragana in hiragana_list:
                    # メーカー名ボックスの選択
                    element_maker = driver.find_element(By.NAME, "maker_name")
                    element_maker.clear()
                    sleep(3)
                    element_maker.send_keys(hiragana)
                    # 待機
                    sleep(3)
                    wait.until(EC.presence_of_all_elements_located)
                    element_list = driver.find_elements(
                        By.CLASS_NAME, "el_suggestion-item")
                    for element in element_list:
                        if element.text not in self.maker_list:
                            self.maker_list.append(element.text)

            maker_list = pd.DataFrame(self.maker_list, columns=['name'])
            self.DEV_output_file(maker_list, 'maker_list')

            return
        except Exception as e:
            print(e)


    def product_name_hiragana_search(self, maker, part_number, driver_no):
        """
        商品名ひらがな検索開始
        """
        try:
            for hiragana in self.hiragana:
                self.price_stock[driver_no] = copy.deepcopy(
                    self.base_price[driver_no])
                self.product_name_hiragana_search_sub(
                    driver_no, maker, part_number, hiragana)
            sleep(1)
            return True
        except Exception as e:
            self.logger.error(f'hiragana_driver:{driver_no}_ひらがな検索エラー:\n{e}')
            return False

    def product_name_hiragana_search_sub(
            self, driver_no, maker, part_number, hiragana):
        try:
            self.driver[driver_no].get(
                f'https://akebonocrown.co.jp/tryangle/productsearch/filtering_search?part_number={part_number}&maker_name={maker}&haiban=1&base_price_over={self.base_price[driver_no][0]}&base_price_under={self.base_price[driver_no][1]}&keyword={hiragana}&keyword_pattern=2')
            sleep(3)
            self.wait[driver_no].until(EC.presence_of_all_elements_located)
            soup = self.return_soup(self.driver[driver_no].page_source)
            # 取得結果処理分け
            if self.base_price[driver_no][1] > self.max_search_price or '該当商品がありませんでした' in soup.text:
                return "価格検索限度額終了"
            if '該当商品が多すぎるので検索条件を追加してください' in soup.text:
                self.down_count[driver_no] = 0  # 価格帯変更減額回数
                self.down_plice_list[driver_no] = []  # 減額値リスト
                # 初期減額価格帯上限値
                self.upper_price[driver_no] = max(
                    int(self.base_price[driver_no][1] * 0.5), self.base_price[driver_no][0] + 10)
                self.down_plice_list[driver_no].append(
                    self.upper_price[driver_no])
                self.base_price[driver_no][1] = self.upper_price[driver_no]
                # 初期価格帯までの増額フラグ
                self.get_lock[driver_no] = False
                self.up_count[driver_no] = 2
                while (
                        self.base_price[driver_no][1] -
                        self.base_price[driver_no][0]) != 10:
                    self.change_page[driver_no] = self.transition_frame(
                        driver_no, maker, part_number, hiragana)
                    if self.change_page[driver_no] == "価格検索継続":
                        # 初期価格帯までの増額開始
                        self.get_lock[driver_no] = True
                        if "search_error" not in self.driver[
                                driver_no].current_url and 'tryangle/search_detail' not in self.driver[driver_no].current_url:
                            self.logger.info(
                                f'driver:{driver_no}_{maker}_品番:{part_number}__商品名:{hiragana}_価格帯{self.base_price[driver_no]}_取得リスト格納')
                            self.item_list.append(
                                self.driver[driver_no].current_url)
                        # 現在取得上限値が開始上限値なら抜ける
                        if self.base_price[driver_no][1] == self.price_stock[driver_no][1]:
                            break
                        self.base_price[driver_no][0] = self.base_price[driver_no][1] + 1
                        self.base_price[driver_no][1] = self.base_price[driver_no][1] + (50 * (
                            self.up_count[driver_no] * self.up_count[driver_no] * self.up_count[driver_no]))
                        if self.base_price[driver_no][1] > self.price_stock[driver_no][1]:
                            self.base_price[driver_no][1] = self.price_stock[driver_no][1]
                        else:
                            self.up_count[driver_no] += 1
                    elif self.change_page[driver_no] != "価格検索継続_商品名検索_減額":
                        self.up_count[driver_no] = 2
                        self.get_lock[driver_no] = False
                        # 減額
                        break
                    else:
                        # 初期価格帯までの増額フラグに応じて処理分け
                        if self.base_price[driver_no][0] >= self.price_stock[driver_no][1]:
                            break
                        if self.get_lock[driver_no] is True:
                            if self.change_page[driver_no] == "価格検索継続_商品名検索_減額":
                                self.down_count[driver_no] += 1
                                self.base_price[driver_no][1] = self.base_price[driver_no][1] - (
                                    50 * (self.down_count[driver_no] * self.down_count[driver_no]))
                                if self.base_price[driver_no][1] < self.base_price[driver_no][0]:
                                    self.base_price[driver_no][1] = self.base_price[driver_no][0] + 20
                                    if self.down_count[driver_no] == 1:

                                        self.base_price[driver_no][0] = self.base_price[driver_no][1] + 1
                                        self.base_price[driver_no][1] = self.base_price[driver_no][1] + (50 * (
                                            self.up_count[driver_no] * self.up_count[driver_no] * self.up_count[driver_no]))
                                        if self.base_price[driver_no][1] > self.price_stock[driver_no][1]:
                                            self.base_price[driver_no][1] = self.price_stock[driver_no][1]
                                        else:
                                            self.up_count[driver_no] += 1

                                    self.down_count[driver_no] = 0
                            else:
                                self.logger.info(
                                    f'driver:{driver_no}_{maker}_品番:{part_number}__商品名:{hiragana}_価格帯{self.base_price[driver_no]}_取得不要')
                                self.base_price[driver_no][0] = self.base_price[driver_no][1] + 1
                                self.base_price[driver_no][1] = self.base_price[driver_no][1] + (50 * (
                                    self.up_count[driver_no] * self.up_count[driver_no] * self.up_count[driver_no]))
                                if self.base_price[driver_no][1] > self.price_stock[driver_no][1]:
                                    self.base_price[driver_no][1] = self.price_stock[driver_no][1]
                                else:
                                    self.up_count[driver_no] += 1
                        else:
                            self.upper_price[driver_no] = max(
                                int(self.base_price[driver_no][1] * 0.5), self.base_price[driver_no][0] + 10)
                            self.down_plice_list[driver_no].append(
                                self.upper_price[driver_no])
                            self.base_price[driver_no][1] = self.upper_price[driver_no]
                            self.down_count[driver_no] += 1

                    if (self.base_price[driver_no][1] -
                            self.base_price[driver_no][0]) == 10:
                        self.logger.error(
                            f'driver:{driver_no}_{maker}_品番:{part_number}__商品名:{hiragana}_価格帯{self.base_price[driver_no][0]}-{self.down_plice_list[driver_no]}_これ以上絞込みできません。')
                        break

                    self.driver_count[driver_no] += 1
                    if self.driver_count[driver_no] > self.retry_limit:
                        if self.driver_retry_count[driver_no] > self.retry_limit:
                            raise Exception(
                                'failed to product_name_hiragana_search_sub.')
                        else:
                            self.restart_driver_cur_page(driver_no)

                # 価格帯を戻す
                self.base_price[driver_no] = copy.deepcopy(
                    self.price_stock[driver_no])
                return
            else:
                if "search_error" not in self.driver[driver_no].current_url and 'tryangle/search_detail' not in self.driver[driver_no].current_url:
                    self.logger.info(
                        f'driver:{driver_no}_{maker}_品番:{part_number}__商品名:{hiragana}_価格帯{self.base_price[driver_no]}_取得リスト格納')
                    self.item_list.append(self.driver[driver_no].current_url)
                return
        except Exception as e:
            self.logger.error(e)

    def return_soup(self, html):
        """与えられたURLのsoupを返す."""
        soup = BeautifulSoup(html, 'html.parser')

        return soup


    def parse_price(self, x):
        if x == '' or str(x) == '0' or 'OPEN' in str(
                x) or x == ' ' or x == '　' or x == 'ー' or x == '-' or x == '－':
            return 999999
        elif re.search(r'[0-9]+', str(x)) is not None:
            return int(re.search(r'[0-9]+', str(x)).group())
        else:
            return x

    def csv_worker(self, process):
        """書き込み専用スレッド"""
        while True:
            # キューからデータを取り出す
            data = self.csv_data_queue.get()
            data = list(data.values())

            # ファイルにデータを書き込む
            if self.first_write:  # 一回目の書き込み時の処理
                # output.txtをoutput_backup.txtにバックアップ
                shutil.copy2(f'./output_{process}.txt', f'./output_{process}_backup.txt')

                # output.txtを空にしてから書き込む
                with open(f'./output_{process}.txt', 'w', encoding='cp932', errors='ignore') as f:
                    f.write(','.join(map(str, data)) + '\n')

                # 一回目の書き込みが完了したらフラグをFalseにする
                self.first_write = False
            else:  # 二回目以降の書き込み時の処理
                with open(f'./output_{process}.txt', 'a', encoding='cp932', errors='ignore') as f:
                    f.write(','.join(map(str, data)) + '\n')

            # タスクが完了したことをキューに通知
            self.csv_data_queue.task_done()


    def DEV_output_file(self, df: pd.DataFrame, file_name):
        """開発時に中身確認用にcsvを出力"""

        signal = True
        if signal:
            df.to_csv(
                './DEVELOPMENT/{}.csv'.format(file_name),
                encoding='cp932',
                errors='ignore',
                index=False)
        return

    def DEV_send_img_Line(self, txt, img_path):
        """開発時にLineに送信用"""

        if self.line_access_token != '':
            try:
                url = "https://notify-api.line.me/api/notify"
                headers = {'Authorization': 'Bearer ' + self.line_access_token}
                payload = {'message': txt}
                files = {'imageFile': open(img_path, 'rb')}
                res = requests.post(
                    url, headers=headers, params=payload, files=files,)
            except BaseException:
                pass


    def main_function(self, arg):
        # クラスを使った例外処理のブロック
        with ErrorHandlingClass(self.df_columns, arg[3], arg[1]) as EC:
            self.first_write = EC.first_write
            self.item_info = EC.item_info
            self.item_list = EC.item_list
            self.closed_manufacturers_list = EC.closed_manufacturers_list
            self.closed_url_list = EC.closed_url_list
            # ワーカースレッドを作成して開始
            csv_worker_thread = threading.Thread(
                target=self.csv_worker, args=(arg[1],), daemon=True)
            csv_worker_thread.start()
            # ドライバー起動からログイン
            self.initialize()
            # メーカー名情報取得
            self.create_maker(self.driver[0], self.wait[0])
            # メーカーリストを並列で処理
            self.make_thread(int(arg[1]), int(arg[2]))

    def main(self):
        """メイン処理"""
        # コマンドライン引数を取得
        arg = sys.argv  
        self.main_function(arg) 

if __name__ == '__main__':

    crawler = AkebonoCrown('settings.json')
    crawler.main()

    # dt_now = datetime.now()
    # folder_name = dt_now.strftime('%Y%m%d/')
    # csv_file_name = os.listdir(
    #     'C:\\Users\\Public\\Programming\\Scraping\\akebono_crown\\csv\\' +
    #     folder_name)[0]
    # shutil.copy(
    #     'C:\\Users\\Public\\Programming\\Scraping\\akebono_crown\\csv\\' +
    #     folder_name +
    #     csv_file_name,
    #     'C:\\Users\\Public\\Programming\\Scraping\\akebono_crown\\アケボノクラウン.csv')

    # # #LF-->CRLF
    # # command = ["sed", "-i", "s/$/\r/g", "アケボノクラウン.csv"]
    # # proc = subprocess.Popen(command)
    # # res = proc.communicate()
    # # #sed -i 's/$/\r/g' csv/20211226/141958755329.csv

    # shutil.copyfile("アケボノクラウン.csv", "../common_functions/アケボノクラウン.csv")
    # sleep(1)

    # # dropbox_function.upload_dropbox('アケボノクラウン.csv', '送料適用前ファイル')

    # # status_sheet.end("アケボノクラウン新商品取得")
