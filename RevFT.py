#逆展開ファイルの整理
#K1.5：076S部品名から商品コードを取出す正規表現式追加
#K1.5.1 : 出力／df_revFT_Fで、列＝'最上位行'　削除を追加
#K1.5.2 : ADD subprocess.Popen
'''
商品コード2種類（4桁、或いは8桁）
・４桁商品コード「１２３４」／１：A-Z、２：0-9、A-Z、３～４：0-9
・８桁商品コード 「１２３４５６７８」／１：A-Z、２～３：0-9、A-Z、４：0-9、A-Z、*、５～８：0-9
・共通/商品コードの後 　(?=...)／空白：無し有り（複数）\s{0,}、１～３：0-9 、４：A-Z
短縮書き方
'[a-zA-Z]{1}[\w]{1}[\d]{2}(?=[\s]{0,}[\d]{3}[a-zA-Z]{1})|[a-zA-Z]{1}[\w]{2}[\w*]{1}[0-9]{4}(?=[\s]{0,}[0-9]{3}[a-zA-Z]{1})'
'''

import pandas as pd
import sys
import time
import re
import subprocess

def getShin(file1,file2):
    
    #1) Read  Excel file
    start = time.time()

    df_partsNo=pd.read_excel(file1,sheet_name="List")    #"調査部番リスト.xlsx"
    df_revFT=pd.read_excel(file2)                                       #"構成展開／逆展開.xlsx"

    proTime = time.time() - start
    print('---')
    print( 'Excel_file loading time : '+str(round(proTime))+' sec')
    print('---')

    #2) 逆展開不可品目を特定
    #2-1) df_revFT項目確認
    item=('No','XJ','ルート','拠点','レベル',
          '部番','REV','部品名','商品コード','QTY',
          'MB','SP','ALT','取引先コード','取引先名',
          '旧部番','ECO NO','CUT IN','SUBC','ST')
    columns=df_revFT.columns
    item_diff=set(item)-set(columns)  # 両集合差取得

    if len(item_diff) > 0:
        print("? ? ?  ")
        print("逆展開取得項目不足、設定修正、再実施！")
        print(item_diff)
        print("? ? ?  ")
        sys.exit()

    #2-2) df_revFT 空白行削除
    df_revFT.dropna(how='all',inplace=True)
    df_revFT.reset_index(inplace=True, drop=True)   #df_revFT index再設定

    #2-3) [df_partsNo , root]から共通しない要素とその個数を取得
    partsNo_diff=set(df_partsNo["調査対象品目"]) - set(df_revFT["ルート"])
    partsNo_diff_len=len(partsNo_diff)
    df_partsNo_diff=pd.DataFrame(list(partsNo_diff), columns=['X-FIT逆展開不可品目'])      #make dataframe

    if partsNo_diff_len > 0:
        print("逆展開不可品目数："+str(partsNo_diff_len))
    else:
        print("依頼部番 : 全数逆展開できた")

    #3) 商品CDを取得
    # 3-1) 前準備
    start = time.time()

    df_revFT=df_revFT.loc[:,['ルート','部番','部品名','商品コード']]

    df_shift_1=df_revFT.loc[:,['部番']].shift(1)      # 縦軸方向一行ずらす
    df_shift_1.columns = ['部番_s1']

    df_shift_2=df_revFT.loc[:,['部番','部品名']].shift(2)      # 縦軸方向二行ずらす
    df_shift_2.columns=['部番_s2','部品名_s2']

    df_rev_m = pd.concat([df_revFT, df_shift_1, df_shift_2],axis=1).fillna('NoData')       # 合体

    # 3-2) add new 7 columns
    # 3-2-0) 最上位行(DXJ行)判定「len（部番）＝3」
    df_rev_m['最上位行'] = df_rev_m["部番"].apply(len)
    df_rev_m1=df_rev_m[df_rev_m['最上位行']==3]                 # 最上位行＝３ の全項目を取得
    df_rev_m2=df_rev_m1.drop_duplicates()             # 重複除外
    df_rev_m2.reset_index(inplace=True, drop=True)   # df_rev index再設定
    df_rev = df_rev_m2.copy()               #参照による元ファイルを変更のwarnnin防止ため

    # 3-2-1) DXJ行商品CDなし判定
    def func_noShinCD(arr) :
        if arr['最上位行']==3 and arr["商品コード"] == 'NoData' :
            return "Non_ShinCD"
        else:
            return "NoData"
    df_rev['商品CD無しマーク'] = df_rev.apply(func_noShinCD , axis=1)

    # 3-2-2) DXJ行商品コードなし、DXJ手前部番を取り出す（⇒080Sを取得）
    def func_080S(arr) :
        if arr['商品CD無しマーク'] == "Non_ShinCD" :
            return arr['部番_s1']
        else:
            return "NoData"
    df_rev['080S部番'] = df_rev.apply(func_080S, axis=1)

    # 3-2-3) 080Sあれば、手前部番中に”076S”を取得
    def func_076S(arr) :
        if arr['080S部番'] == "NoData" :
            return "NoData"
        elif arr['部番_s2'][0:4:1]=="076S" :
            return arr['部番_s2']
        else:
            return "NoData"
    df_rev['076S部番'] = df_rev.apply(func_076S, axis=1)

    # 3-2-4) 076Sあれば、その部品名を取得
    def func_076S_name(arr) :
        if arr['076S部番'] == "NoData" :
            return "NoData"
        else :
            return arr['部品名_s2']
    df_rev['076S部品名'] = df_rev.apply(func_076S_name, axis=1)

    # 3-2-5) 076S 部品名あれば、その中の商品CDを取得「正規表現式」

    def func_ShinCD_076S(arr) :
        if arr['076S部品名'] == "NoData" :
            return "NoData"
        else:
            p=re.compile(r'[a-zA-Z]{1}[\w]{1}[\d]{2}(?=[\s]{0,}[\d]{3}[a-zA-Z]{1})|[a-zA-Z]{1}[\w]{2}[\w*]{1}[0-9]{4}(?=[\s]{0,}[0-9]{3}[a-zA-Z]{1})')
            ShinCD_076S_m = p.search(arr['076S部品名'])
            if ShinCD_076S_m == None :    #有効な商品コードなし
                return "NoData"
            else :
                return ShinCD_076S_m[0]

    df_rev['076S_商品CD'] = df_rev.apply(func_ShinCD_076S, axis=1)

    # 3-2-6) 商品CDを抽出
    def func_ShinCD(arr) :
        if arr['076S_商品CD'] !="NoData":
            return arr['076S_商品CD']
        else:
            return arr["商品コード"]
    df_rev['商品CD'] = df_rev.apply(func_ShinCD, axis=1)

    #４）整理
    df_m = df_rev.loc[:, ['ルート','部番','最上位行','080S部番','076S部番','076S部品名','商品CD']]        # 必要項目を抽出

    # df_m1=df_m[df_m['最上位行']==3]                 # 最上位行＝３ の全項目を取得

    df_revFT_t=df_m.drop_duplicates()             # 重複除外
    df_revFT_t.reset_index(inplace=True, drop=True)   # df_revFT index再設定
    df_revFT_F = df_revFT_t.copy()
    df_revFT_F.drop(columns = ['最上位行'], inplace = True)     #K1.5.1 追加

    # 商品CD列を抽出、整理
    df_ShinCD_F1 = df_revFT_F[df_revFT_F['商品CD'] != 'NoData']        #空白除外
    ShinCD_F=set(df_ShinCD_F1['商品CD'])                                            #重複除外
    df_ShinCD_F=pd.DataFrame(ShinCD_F, columns=['商品CD'])      #make dataframe

    #5) to_excel（複数シートへ書き込む）
    with pd.ExcelWriter("output_revFT.xlsx") as writer:
        df_ShinCD_F.to_excel(writer, sheet_name='商品CD')
        df_partsNo.to_excel(writer, sheet_name='PartsNo', index = False)
        if partsNo_diff_len > 0:
            df_partsNo_diff.to_excel(writer, sheet_name='X-FIT逆展開不可品目')
        df_revFT_F.to_excel(writer, sheet_name='df_rev_F')

    subprocess.Popen(['start', 'output_revFT.xlsx'], shell = True)

    proTime = time.time() - start
    print('~.~.~')
    print("逆展開より商品コード抽出完了")
    print( 'Data processing time : '+str(round(proTime))+' sec')
    print('~.~.~')

if __name__ == '__main__':
    file1 = r'C:\Users\jcj_c\OneDrive\Desktop\python\sampleData\調査部番リスト.xlsx'
    file2 = r'C:\Users\jcj_c\OneDrive\Desktop\python\sampleData\構成展開／逆展開.xlsx'
    getShin(file1,file2)
