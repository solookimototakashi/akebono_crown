import time
import csv
import create_df
import sys
import pandas as pd
sys.path.append('../common_functions')
# import status_sheet
# import chatwork_function        
import subprocess
import shutil

class ErrorHandlingClass:
    def __init__(self, df_columns, execution, process_no):
        # 中断ログの読込
        self.output_filename = f"./output_{process_no}.txt"
        self.item_list_name = f"./DEVELOPMENT/item_list_{process_no}.csv"
        self.csv_filename = f"./interrupted_data_export_{process_no}.csv"
        self.csv_url_filename = f"./interrupted_url_export_{process_no}.csv"
        self.csv_filename_back = f"./interrupted_data_export_{process_no}_back.csv"
        self.csv_url_filename_back = f"./interrupted_url_export_{process_no}_back.csv"
        
        try:
            if execution == "True":
                # 再開
                print("中断ログ有:再開")                
                self.closed_manufacturers_list = self.read_data_csv(self.csv_filename)                
                self.closed_url_list = self.read_data_csv(self.csv_url_filename)
                self.item_info = create_df.create_df(self.output_filename, df_columns)
                self.item_list = create_df.create_df(self.item_list_name, ["URL"])
                self.first_write = False                    
                # self.closed_manufacturers_list = []
            else:
                # 新規
                print("中断ログ無:開始")
                self.create_interrupt_log()
                self.closed_manufacturers_list = []
                self.closed_url_list = []
                self.item_info = create_df.create_df(None, df_columns)
                self.item_list = []
                self.first_write = True
                self.save_state()
        except FileNotFoundError:
            self.closed_manufacturers_list = []
            print("No interrupt log found. Starting fresh...")

        self.start_time = None


    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # elapsed_time = time.time() - self.start_time
        # if elapsed_time > (18 * 60 * 60):
        #     print("Total timeout exceeded. Exiting...")
        #     self.create_interrupt_log()
        #     self.export_data_csv()  # データをCSV出力
        #     raise SystemExit

        if exc_type is not None:
            print("An error occurred!")
            self.create_interrupt_log()
            self.export_data_csv()  # データをCSV出力
            self.send_error_notification()
            raise SystemExit

        else:
            self.export_data_csv()  # データをCSV出力
            print("No error occurred.")        

    def send_error_notification(self):
        to_list = "[To:1285429]大島さん\n[To:5961183]橋本拓馬さん\n[To:2946724]沖本 卓士さん\n"
        # chatwork_function.postChatwork("254599573", "アケボノクラウン新商品取得スクレイピングエラー中断", to_list)
        # status_sheet.error("アケボノクラウン新商品取得")
        self.terminate_specific_chrome_driver()

    def terminate_specific_chrome_driver(self):
        # PowerShellスクリプトを実行するコマンド
        command = "powershell.exe -ExecutionPolicy Bypass -File all_kill.ps1"
        # subprocessを使ってPowerShellスクリプトを実行
        result = subprocess.run(command, capture_output=True, text=True)
        # 結果を出力
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

    def create_interrupt_log(self):
        # 中断ログの発行
        log_filename = "./interrupt_log.txt"

        with open(log_filename, 'w') as log_file:
            log_file.write("akebono execution interrupted")

        print(f"Interrupt log created: {log_filename}")

    def export_data_csv(self):
        # 中断時点の取得データ
        csv_filename = "./interrupted_data_export.csv"

        with open(csv_filename, 'w', newline='', encoding='cp932') as csv_file:
            writer = csv.writer(csv_file)
            for data in self.closed_manufacturers_list:
                writer.writerow([data])  # リストとして出力

        print(f"Data exported to CSV: {csv_filename}")

    def read_data_csv(self, csv_filename):
        data_list = []

        with open(csv_filename, 'r', newline='', encoding='cp932') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                data_list.extend(row)

        return data_list  
        
    def save_state(self):
        # closed_manufacturers_listとclosed_url_listをCSVに保存
        self.write_list_to_csv(self.closed_manufacturers_list, self.csv_filename)
        self.write_list_to_csv(self.closed_url_list, self.csv_url_filename)
        
        # item_infoをCSVに保存
        self.item_info.to_csv(self.output_filename, index=False)
        
        # item_listをCSVに保存
        self.write_list_to_csv(self.item_list, self.item_list_name)

    def write_list_to_csv(self, data_list, filename):
        # リストをCSVに保存するメソッド
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for item in data_list:
                writer.writerow([item])