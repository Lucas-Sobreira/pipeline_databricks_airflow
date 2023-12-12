# Databricks notebook source
# MAGIC %md 
# MAGIC ## Imports

# COMMAND ----------

import requests
import pyspark.sql.functions as fn

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Funções

# COMMAND ----------

def extraindo_dados(date, base="BRL"):

    url = f"https://api.apilayer.com/exchangerates_data/{date}&base={base}"

    headers= {
    "apikey": "dU3B3Ua5VT5zeQcTsknh23VsMyZ9lYyV"
    }

    parametros = {"base":base}

    response = requests.request("GET", url, headers=headers, params=parametros)

    if response.status_code != 200: 
        raise Exception("Não consegui extrair dados!!!")

    return response.json()

def dados_para_dataframe(dado_json):
    dados_tupla = [(moeda, float(taxa)) for moeda, taxa in dado_json["rates"].items()]
    return dados_tupla

def salvar_arquivo_parquet(cotacoes):
    # Definindo variaveis auxiliares
    ano, mes, dia = cotacoes["date"].split("-")
    caminho_completo = f"dbfs:/databricks-results/bronze/{ano}/{mes}/{dia}"
    
    # DataFrame construido
    dados = dados_para_dataframe(cotacoes)
    df_conversoes = spark.createDataFrame(dados, schema=['moeda', 'taxa'])
    df_conversoes = df_conversoes.withColumn("data", fn.lit(f"{ano}-{mes}-{dia}"))

    # Salvando em formato Parquet
    df_conversoes.write.format("parquet").mode("overwrite").save(caminho_completo)

    print("Os dados foram salvos em {}".format(caminho_completo))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Executando as funções

# COMMAND ----------

cotacoes = extraindo_dados("2023-01-01")
salvar_arquivo_parquet(cotacoes)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Verificando se deu certo

# COMMAND ----------

spark.read.format('parquet').load("dbfs:/databricks-results/bronze/2023/01/01").display()

# COMMAND ----------


