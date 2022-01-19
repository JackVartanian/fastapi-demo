# main.py

from fastapi import FastAPI
import uvicorn
import pandas as pd
import json
import funcoes as funcs

# URLs
estoque_url = "https://jvphotos.com.br/public_html/wp-content/uploads/ETL/fEstoque.csv"
produtos_url = "https://jvphotos.com.br/public_html/wp-content/uploads/ETL/dProdutos.csv"
clientes_url = "https://jvphotos.com.br/public_html/wp-content/uploads/ETL/dClientes.csv"
vendas_url = "https://jvphotos.com.br/public_html/wp-content/uploads/ETL/fVendas.csv"

app = FastAPI()

@app.get("/", status_code=200)
async def read_root():
    return {"message": "Hello World"}


@app.get("/estoque", status_code=200)
async def get_estoque():
    df = funcs.get_csv_estoque(estoque_url)
    return df


@app.get("/estoque/{item_id}", status_code=200)
async def read_item(item_id):
    df = funcs.get_csv_estoque_id(estoque_url)
    df = df.loc[df['Cod__Prod'] == item_id]
    df_to_js = df.to_json(orient='records')
    df_final = json.loads(df_to_js)
    return df_final