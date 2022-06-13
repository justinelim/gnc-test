import pandas as pd
import glob
import os

path = "data/business_report/"

all_files = glob.glob(os.path.join(path, "*.csv"))

li = []

for filename in all_files:
    print('Processing ' + filename)
    df = pd.read_csv(filename, converters={'user_id': str}, encoding='utf-8')
    id_cols = [col for col in df.columns if ('user_id' in col) | ('hc_id' in col)]

    for colx in id_cols:
        df[colx] = df[colx].replace("	", "", regex=True)
        df[colx] = df[colx].apply(str) + "	"

    df.to_csv(filename, index=False, encoding='utf_8_sig')