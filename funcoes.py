import pandas as pd
import requests
from io import StringIO
import json

def get_csv_estoque(url):
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0"}
    response = requests.get(url, headers=headers)
    csv_data = response.text
    csv_data = StringIO(csv_data)
    df = pd.read_csv(csv_data, sep=';', encoding='utf-8')
    df['Qtd'] = df['Qtd'].astype(int)
    df = df.reset_index(drop=True, inplace=False)

    # Converter pandas dataframe para json
    df_json = df.to_json(orient='index')
    df_parsed = json.loads(df_json).values()
    df_to_df = pd.DataFrame.from_dict(df_parsed)
    df_to_js = df_to_df.to_json(orient='records')
    df_final = json.loads(df_to_js)
    return df

def get_csv_estoque_id(url):
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0"}
    response = requests.get(url, headers=headers)
    csv_data = response.text    
    csv_data = StringIO(csv_data)
    df = pd.read_csv(csv_data, sep=';', encoding='utf-8')
    df['Qtd'] = df['Qtd'].astype(int)
    df = df.reset_index(drop=True, inplace=False)

    # Converter pandas dataframe para json
    df_json = df.to_json(orient='index')
    df_parsed = json.loads(df_json).values()
    df_to_df = pd.DataFrame.from_dict(df_parsed)
    return df