import os
import re
import unicodedata
import pandas as pd
from datetime import datetime, timedelta

def read_data_txt(txt_filename):
    data_list = []
    with open(txt_filename, 'r', newline='', encoding='cp932', errors='ignore') as file:
        for line in file:
            try:
                row = line.strip().split(',')
                image_index = get_image_index(row)
                zeinuki = ""
                zeikomi = ""
                pos = 3
                if row[0] == "4902870776565":
                    print("")
                elif len(row) < 11:
                    print(row)
                    continue
                
                while True:
                    if zeinuki == "":
                        zeinuki = zeinuki + row[pos]
                        ii = 1
                        while True:
                            if check_money(row[pos+ii]):
                                if (image_index - 2) - (pos + ii) == 1:
                                    zeinuki_flag = False
                                    pos += ii
                                    break
                                else:
                                    if row[pos+ii-1] == "000":
                                        zeinuki_flag = False
                                        pos += ii
                                        break
                                    else:
                                        zeinuki += row[pos+ii]                               
                            else:
                                pos += ii
                                break
                            ii += 1
                    elif zeikomi == "":
                        zeikomi = zeikomi + row[pos]
                        ii = 1
                        while True:                            
                            if check_money(row[pos+ii]):
                                zeikomi += row[pos+ii]                                                        
                            if (image_index - 2) == (pos+ii):
                                zeikomi_flag = False
                                break
                            ii += 1
                    else:
                        break                                                           
                # 改行文字を削除してカンマで分割
                item_dict = {
                    'JAN': row[0],
                    '品番': row[1],
                    '商品名': row[2],
                    '税抜き定価': zeinuki,
                    '税抜き仕入れ値': zeikomi,
                    '発注単位': row[image_index - 2],
                    '在庫数': row[image_index - 1],
                    '画像URL': row[image_index],
                    '商品URL': row[image_index + 1],
                    'メーカー名': row[image_index + 2],
                    '注文番号': row[image_index + 3],                    
                }                
                data_list.append(item_dict)
            except Exception as e:
                print(e)
                continue
            
    return data_list

def get_image_index(row):
    for i in range(len(row)):    
        if ".jpg" in row[i]:
            return i
        
def check_money(s):
    # 文字列が数字のみで構成されているかどうかをチェック
    if "." in s:
        s = s.split(".")[0]
    if s.isdigit():
        # 文字列の長さが3であるかどうかをチェック
        return len(s) == 3
    else:
        return False

def adjust_data(item_df):
    """取得したデータを整える"""

    item_df.fillna('', inplace=True)
    try:
        # JANがない商品除去
        item_df.drop(index=item_df.loc[item_df['JAN'] == ''].index, inplace=True)
        
        # 改行など置換
        
        for col in item_df.columns:
            item_df[col] = item_df[col].apply(lambda x: str(x).replace('\n', '　').replace('\r', ''))
        
        # 調整
        item_df['JAN'] = item_df['JAN'].apply(lambda x: x.replace('\u3000', '').replace(' ', ''))
        item_df['JAN'] = item_df['JAN'].apply(lambda x: re.search(r'[0-9]+', x).group() if re.search(r'[0-9]+', x) != None else '')
        item_df['JAN'] = item_df['JAN'].apply(lambda x: '0'*(13-len(x))+ str(x))
        item_df['商品名'] = item_df['商品名'].apply(lambda x: re.sub(r'　$', '', str(x)))
        item_df['商品名'] = item_df['商品名'].apply(lambda x: re.sub(r' $', '', str(x)))
        item_df['商品名'] = item_df['商品名'].apply(lambda x: str(x).replace('－', '-'))
        item_df['商品名'] = item_df['商品名'].str.lstrip()
        item_df['税抜き定価'] = item_df['税抜き定価'].str.replace(',', '')
        item_df['税抜き仕入れ値'] = item_df['税抜き仕入れ値'].str.replace(',', '')
        item_df['税抜き定価'] = item_df['税抜き定価'].apply(parse_price)
        item_df['税抜き仕入れ値'] = item_df['税抜き仕入れ値'].str.replace(',', '').replace('-', -1)
        item_df['受注生産'] = item_df['商品名'].apply(lambda x: '受注生産' if '受注生産' in str(x) else '')
        item_df['取寄せ'] = item_df['在庫数'].apply(lambda x: '取寄' if str(x) == '取寄品' else '')
        def extract_number(value):
            match = re.search(r'[0-9]+', str(value))
            if match:
                return int(match.group())
            else:
                return 0
        item_df['発注単位'] = item_df['発注単位'].apply(extract_number)
        item_df['発注単位'] = item_df['発注単位'].apply(lambda x: '有' if int(x) >= 2 else '')
        item_df['在庫数'] = item_df['在庫数'].apply(lambda x: unicodedata.normalize('NFKC', str(x)))
        item_df['在庫数'] = item_df['在庫数'].apply(lambda x: int(re.search(r'[0-9]+', str(x)).group()) if re.search(r'[0-9]+', str(x)) != None else 0)
        item_df['プロパー用在庫'] = item_df['在庫数']
        item_df['在庫数'] = item_df.apply(lambda x: 0 if x['取寄せ'] == '取寄' or x['発注単位'] == '有' or x['受注生産'] == '受注生産' else x['在庫数'], axis=1)
        item_df['プロパー用在庫'] = item_df.apply(lambda x: 0 if x['取寄せ'] == '取寄' or x['受注生産'] == '受注生産' else x['プロパー用在庫'], axis=1)
        item_df['プロパー用在庫'] = item_df.apply(lambda x: 999 if x['取寄せ'] == '取寄' and x['受注生産'] != '受注生産' else x['プロパー用在庫'], axis=1)
        item_df['画像URL'] = item_df['画像URL'].apply(lambda x: 'https://akebonocrown.co.jp{}'.format(x) if x != '' else '')
        item_df['画像URL'] = item_df['画像URL'].apply(lambda x: '' if ' ' in x or '　' in x or '	' in x else x)
        no_image = 'https://akebonocrown.co.jp/tryangle/shohin/gazo/.jpg'
        item_df['画像URL'] = item_df['画像URL'].apply(lambda x: '' if no_image in x else x)
        no_image = 'https://akebonocrown.co.jp/tryangle/shohin/gazo/0.jpg'
        item_df['画像URL'] = item_df['画像URL'].apply(lambda x: '' if no_image in x else x)
        # JANが重複している商品について処理を行う
        duplicated_jans = item_df['JAN'].duplicated(keep=False)
        # 同じJANを持つ商品のうち、全ての属性が同じ商品のインデックスを取得
        unique_indices = item_df[duplicated_jans].drop_duplicates(subset=['JAN', '商品名', '品番']).index
        # 重複している商品のインデックスを取得
        duplicated_indices = item_df[duplicated_jans].index.difference(unique_indices)

        # 重複している商品のうち、発注単位が存在しない商品のインデックスを取得
        unitless_indices = item_df.loc[duplicated_indices, '発注単位'].isnull()

        if unitless_indices.any():
            # 重複している商品を削除（発注単位が存在しない商品を優先）
            item_df.drop(index=duplicated_indices[unitless_indices][1:], inplace=True)
            item_df.drop_duplicates(subset=['JAN'], keep='first', inplace=True)
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
        print(remaining_duplicates)

        # if len(item_df) != len(item_df['JAN'].unique()):
        #     dupli_jans = [jan for jan in item_df['JAN'].unique() if len(item_df.loc[item_df['JAN'] == jan]) > 1]
        #     for jan in dupli_jans:
        #         if len(item_df.loc[item_df['JAN'] == jan, '商品名'].unique()) == 1 and len(item_df.loc[item_df['JAN'] == jan, '品番'].unique()) == 1:
        #             item_df.drop(index=item_df.loc[item_df['JAN'] == jan].index[1:], inplace=True)
        #         else:
        #             item_df.drop(index=item_df.loc[item_df['JAN'] == jan].index, inplace=True)
        
        # 税抜き仕入れ値が0円の商品は除外
        # item_df.drop(index=item_df.loc[item_df['税抜き仕入れ値'] == 0].index, inplace=True)
        # '税抜き定価'が'税抜き仕入れ値'より低い行を削除
        item_df = item_df[pd.to_numeric(item_df['税抜き定価'], errors='coerce') > pd.to_numeric(item_df['税抜き仕入れ値'], errors='coerce')]

        # インデックスリセット
        item_df = item_df.reset_index(drop=True)
        return item_df
    except Exception as e:
        print(e)

def parse_price(x):
    if x == '' or str(x) == '0' or 'OPEN' in str(x) or x ==' ' or x =='　' or x =='ー' or x =='-' or x =='－':
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
        dt_now = datetime.now()
        folder_name = dt_now.strftime('%Y%m%d')
        # NR_csv出力
        if not os.path.exists(f'csv/{folder_name}'):
            os.makedirs(f'csv/{folder_name}')
        file_name = dt_now.strftime('%H%M%S%f') + '.csv'
        with open(f"csv/{folder_name}/{file_name}", mode="wt", newline="", encoding="shift-jis", errors="ignore") as f:
            item_df.to_csv(f, index=False)
    except Exception as e:
        print(e)

def create_df(txt_filename, df_columns):
    if txt_filename is None:
        item_df = pd.DataFrame([],columns=df_columns)
    elif ".csv" in txt_filename:
        item_df = pd.read_csv(txt_filename, encoding="cp932")
        item_df = [item[0] for item in item_df.values]
    else:
        df = read_data_txt(txt_filename)    
        item_df = pd.DataFrame(df,columns=df_columns)
    return item_df

if __name__ == "__main__":
    dir = r"C:\Users\Public\Programming\Scraping\akebono_crown"
    file_path = r"\20240214\214615183836.csv"
    file_path_2 = r"\20240214\アケボノクラウン0214.csv"    
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

    df = read_data_txt(r"C:\Users\Public\Programming\Scraping\akebono_crown\merge_output.txt")
    # df = read_data_txt(r"C:\Users\Public\Programming\Scraping\akebono_crown\output_test.txt")
    
    item_df = pd.DataFrame(df,columns=df_columns)
    item_df = adjust_data(item_df)
    item_df = item_df[columns]
    output_csv(item_df) 

    # # CSVファイルを読み込む
    # df1 = pd.read_csv(dir+file_path, encoding="cp932")
    # df2 = pd.read_csv(dir+file_path_2, encoding="cp932")

    # # # df1にあってdf2にないデータを抽出する
    # # diff_df = pd.concat([df1, df2]).drop_duplicates(keep=False)
    # # df2の「品番」がdf1に存在しないものを抽出
    # diff_df = df2[~df2['品番'].isin(df1['品番'])]

    # output_csv(diff_df)