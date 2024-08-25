import os
import re
import shutil
import pandas as pd
import sqlite3
import subprocess
import sys
import create_df 
from datetime import datetime
import unicodedata
import time

columns = [
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

df_columns = [
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

def adjust_data(item_df):
    """取得したデータを整える"""

    item_df.fillna('', inplace=True)
    try:
        # JANがない商品除去
        item_df.drop(
            index=item_df.loc[item_df['JAN'] == ''].index, inplace=True)

        # 改行など置換

        for col in item_df.columns:
            item_df[col] = item_df[col].apply(
                lambda x: str(x).replace(
                    '\n', '　').replace(
                    '\r', ''))

        # 調整
        item_df['JAN'] = item_df['JAN'].apply(
            lambda x: x.replace('\u3000', '').replace(' ', ''))
        item_df['JAN'] = item_df['JAN'].apply(
            lambda x: re.search(
                r'[0-9]+',
                x).group() if re.search(
                r'[0-9]+',
                x) is not None else '')
        item_df['JAN'] = item_df['JAN'].apply(
            lambda x: '0' * (13 - len(x)) + str(x))
        item_df['商品名'] = item_df['商品名'].apply(
            lambda x: re.sub(r'　$', '', str(x)))
        item_df['商品名'] = item_df['商品名'].apply(
            lambda x: re.sub(r' $', '', str(x)))
        item_df['商品名'] = item_df['商品名'].apply(
            lambda x: str(x).replace('－', '-'))
        item_df['商品名'] = item_df['商品名'].str.lstrip()
        item_df['税抜き定価'] = item_df['税抜き定価'].str.replace(',', '')
        item_df['税抜き仕入れ値'] = item_df['税抜き仕入れ値'].str.replace(
            ',', '')
        item_df['税抜き定価'] = item_df['税抜き定価'].apply(
            parse_price)
        item_df['税抜き仕入れ値'] = item_df['税抜き仕入れ値'].str.replace(
            ',', '').replace('-', -1)
        item_df['受注生産'] = item_df['商品名'].apply(
            lambda x: '受注生産' if '受注生産' in str(x) else '')
        item_df['取寄せ'] = item_df['在庫数'].apply(
            lambda x: '取寄' if str(x) == '取寄品' else '')

        def extract_number(value):
            match = re.search(r'[0-9]+', str(value))
            if match:
                return int(match.group())
            else:
                return 0
        item_df['発注単位'] = item_df['発注単位'].apply(extract_number)
        item_df['発注単位'] = item_df['発注単位'].apply(
            lambda x: '有' if int(x) >= 2 else '')
        item_df['在庫数'] = item_df['在庫数'].apply(
            lambda x: unicodedata.normalize('NFKC', str(x)))
        item_df['在庫数'] = item_df['在庫数'].apply(
            lambda x: int(
                re.search(
                    r'[0-9]+',
                    str(x)).group()) if re.search(
                r'[0-9]+',
                str(x)) is not None else 0)
        item_df['プロパー用在庫'] = item_df['在庫数']
        item_df['在庫数'] = item_df.apply(
            lambda x: 0 if x['取寄せ'] == '取寄' or x['発注単位'] == '有' or x['受注生産'] == '受注生産' else x['在庫数'],
            axis=1)
        item_df['プロパー用在庫'] = item_df.apply(
            lambda x: 0 if x['取寄せ'] == '取寄' or x['受注生産'] == '受注生産' else x['プロパー用在庫'], axis=1)
        item_df['プロパー用在庫'] = item_df.apply(
            lambda x: 999 if x['取寄せ'] == '取寄' and x['受注生産'] != '受注生産' else x['プロパー用在庫'], axis=1)
        item_df['画像URL'] = item_df['画像URL'].apply(
            lambda x: 'https://akebonocrown.co.jp{}'.format(x) if x != '' else '')
        item_df['画像URL'] = item_df['画像URL'].apply(
            lambda x: '' if ' ' in x or '　' in x or '	' in x else x)
        no_image = 'https://akebonocrown.co.jp/tryangle/shohin/gazo/.jpg'
        item_df['画像URL'] = item_df['画像URL'].apply(
            lambda x: '' if no_image in x else x)
        no_image = 'https://akebonocrown.co.jp/tryangle/shohin/gazo/0.jpg'
        item_df['画像URL'] = item_df['画像URL'].apply(
            lambda x: '' if no_image in x else x)
        # JANが重複している商品について処理を行う
        duplicated_jans = item_df['JAN'].duplicated(keep=False)
        # 同じJANを持つ商品のうち、全ての属性が同じ商品のインデックスを取得
        unique_indices = item_df[duplicated_jans].drop_duplicates(
            subset=['JAN', '商品名', '品番']).index
        # 重複している商品のインデックスを取得
        duplicated_indices = item_df[duplicated_jans].index.difference(
            unique_indices)

        # 重複している商品のうち、発注単位が存在しない商品のインデックスを取得
        unitless_indices = item_df.loc[duplicated_indices, '発注単位'].isnull(
        )

        if unitless_indices.any():
            # 重複している商品を削除（発注単位が存在しない商品を優先）
            item_df.drop(
                index=duplicated_indices[unitless_indices][1:], inplace=True)
            item_df.drop_duplicates(
                subset=['JAN'], keep='first', inplace=True)
        else:
            # 重複している商品を削除（1つのみ残す）
            item_df.drop(index=duplicated_indices[1:], inplace=True)

        # 追加の重複削除が必要な場合は繰り返し処理を行う
        while item_df['JAN'].duplicated().any():
            duplicated_jans = item_df['JAN'].duplicated(keep=False)
            duplicated_indices = item_df[duplicated_jans].index
            item_df.drop(index=duplicated_indices[1:], inplace=True)

        # 重複が解消されたかどうかを確認する
        duplicated_jans = item_df['JAN'].duplicated(keep=False)
        remaining_duplicates = item_df[duplicated_jans]
        # print(remaining_duplicates)
        # '税抜き定価'が'税抜き仕入れ値'より低い行を削除
        item_df = item_df[pd.to_numeric(item_df['税抜き定価'], errors='coerce') > pd.to_numeric(
            item_df['税抜き仕入れ値'], errors='coerce')]

        # インデックスリセット
        item_df = item_df.reset_index(drop=True)
    except Exception as e:
        if len(item_df) != 0:
            print('failed to adjust data.')
        else:
            print('data is empty.')        
            item_df = pd.DataFrame(columns=columns)

def parse_price(x):
    if x == '' or str(x) == '0' or 'OPEN' in str(
            x) or x == ' ' or x == '　' or x == 'ー' or x == '-' or x == '－':
        return 999999
    elif re.search(r'[0-9]+', str(x)) is not None:
        return int(re.search(r'[0-9]+', str(x)).group())
    else:
        return x

def output_csv(item_df):
    """
    取得結果をcsvに出力します.
    """
    try:
        print("output_csv start.")
        dt_now = datetime.now()
        folder_name = dt_now.strftime('%Y%m%d')
        # NR_csv出力
        if not os.path.exists(f'csv/{folder_name}'):
            os.makedirs(f'csv/{folder_name}')
            print("csv output folder created.")
        file_name = dt_now.strftime('%H%M%S%f') + '.csv'
        with open(f"csv/{folder_name}/{file_name}", mode="wt", newline="", encoding="shift-jis", errors="ignore") as f:
            item_df.to_csv(f, index=False)
        print("output csv end.")
    except Exception as e:
        print("output csv: " + str(e))

def finalize():
    """
    処理を終了します.
    """
    # 正常ログの発行
    log_filename = "./interrupt_log.txt"

    with open(log_filename, 'w') as log_file:
        log_file.write("akebono normal termination")

    print(f"Termination log created: {log_filename}")

def err_finalize():
    """
    処理を終了します.
    """
    # エラーログの発行
    log_filename = "./interrupt_log.txt"

    with open(log_filename, 'w') as log_file:
        log_file.write("akebono execution termination")

    print(f"Termination log created: {log_filename}")

def execute_processes(count, execution):
    python_executable = sys.executable  # 現在のPythonインタプリタのパスを取得
    script_path = 'main.py'  # 実行するスクリプトのパス
    processes = []
    failed_processes = []

    pid_file = 'pids.txt'

    # PIDファイルを初期化
    with open(pid_file, 'w') as f:
        f.write("")

    # サブプロセスを起動
    for i in range(count):
        p = subprocess.Popen([python_executable, script_path, str(i), str(count), str(execution)])
        processes.append((i, p))
        # PIDをファイルに書き込む
        with open(pid_file, 'a') as f:
            f.write(f"{p.pid}\n")

    # サブプロセスの終了を待つ
    for i, p in processes:
        try:
            # p.wait(timeout=300)  # タイムアウトを設定
            p.wait()
        except subprocess.TimeoutExpired:
            print(f"[ERROR] Process {i} timed out.")
            p.kill()
            failed_processes.append(i)
        else:
            if p.returncode != 0:
                print(f"[ERROR] Process {i} failed with return code {p.returncode}.")
                failed_processes.append(i)
            # 成功した場合、PIDファイルから該当のPIDを削除
            remove_pid_from_file(pid_file, p.pid)

    return failed_processes

def remove_pid_from_file(pid_file, pid):
    with open(pid_file, 'r') as f:
        lines = f.readlines()
    with open(pid_file, 'w') as f:
        for line in lines:
            if line.strip() != str(pid):
                f.write(line)

def run_in_parallel():
    try:
        execution = False
        log_filename = "./interrupt_log.txt"
        count = 2        
        with open(log_filename, 'r') as log_file:
            first_line = log_file.readline().strip()
            if first_line == "akebono execution interrupted":
                # 再開
                print("中断ログ有:再開")
                execution = True
            else:
                # 初回起動解除
                print("正常処理ログ有:初回起動")
                with open(log_filename, 'w') as log_file:
                    log_file.write("akebono execution interrupted")

        if not execution:
            for i in range(count):
                try:
                    # output.txtをoutput_backup.txtにバックアップ
                    shutil.copy2(f'./output_{i}.txt', f'./output_{i}_backup.txt')
                except Exception:
                    pass
                # 空のCSVファイルを作成してからデータを書き込む
                with open(f'./output_{i}.txt', 'w', encoding='cp932', errors='ignore') as f:
                    pass

        # すべてのプロセスが正常に終了するまで繰り返す
    # while True:
        failed_processes = execute_processes(count, execution)
        if not failed_processes:
            print("All processes completed successfully.")
            # break
        else:
            print(f"Retrying failed processes: {failed_processes}")
            time.sleep(5)  # 少し待機してから再試行

        # データフレームを格納するリスト
        all_dfs = []
        # データの読み込み
        for i in range(count):
            try:
                item_df = create_df.create_df(f"./output_{i}.txt", df_columns)
                all_dfs.append(item_df)
            except Exception as e:
                print(f"Error reading data: {e}")
                raise

        # すべてのデータフレームを縦に結合
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # データベースに保存
        db_conn = sqlite3.connect("scraping.db")
        combined_df.to_sql(
            'akebono',
            db_conn,
            if_exists='replace',
            index=None)
        db_conn.close()

        # データ調整、CSV出力、終了処理
        adjust_data(combined_df)
        combined_df = combined_df[columns]
        output_csv(combined_df)
        finalize()
    except:
        err_finalize

if __name__ == "__main__":
    run_in_parallel()
