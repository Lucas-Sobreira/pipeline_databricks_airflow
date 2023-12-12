# Databricks notebook source
# MAGIC %md 
# MAGIC ## Realizando um teste

# COMMAND ----------

import requests

url = "https://api.apilayer.com/exchangerates_data/{date}?symbols={symbols}&base={base}"

payload = {}
headers= {
  "apikey": "dU3B3Ua5VT5zeQcTsknh23VsMyZ9lYyV"
}

response = requests.request("GET", url, headers=headers, data = payload)

status_code = response.status_code
result = response.text
