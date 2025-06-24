#!/usr/bin/env python3

from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()

query = """
SELECT *
FROM `bigquery-public-data.crypto_ethereum_classic.transactions`
WHERE block_number <= 1000000
"""

df = client.query(query).to_dataframe()
df.to_csv("transactions.csv", index=False)
print("Saved data to transactions.csv")

query = """
SELECT *
FROM `bigquery-public-data.crypto_ethereum_classic.traces`
WHERE block_number <= 1000000
"""

df = client.query(query).to_dataframe()
df.to_csv("traces.csv", index=False)
print("Saved data to traces.csv")
