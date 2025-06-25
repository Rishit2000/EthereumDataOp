# EthereumDataOp

This project explains how to export transaction and trace data for Ethereum using the BiqQuery Python Client. Thereafter, it shows how to simulate algorithm 1 of the [paper](https://saltaformaggio.ece.gatech.edu/publications/yao2024pulling.pdf) using SQL.

## Download Ethereum Transaction records in CSV 

```bash
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()

query = """
SELECT *
FROM `bigquery-public-data.crypto_ethereum_classic.transactions`
WHERE block_number <= 100000
"""

df = client.query(query).to_dataframe()
df.to_csv("transactions.csv", index=False)
print("Saved data to transactions.csv")
```


## Download Ethereum Traces records in CSV

```bash
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()

query = """
SELECT *
FROM `bigquery-public-data.crypto_ethereum_classic.traces`
WHERE block_number <= 100000
"""

df = client.query(query).to_dataframe()
df.to_csv("traces.csv", index=False)
print("Saved data to traces.csv")
```

## Create PostGreSQL tables using the CSV files

```bash
-- Assumed VARCHAR datatype for all columns for simplicity.
CREATE TABLE transactions (
  hash VARCHAR,
  nonce VARCHAR,
  transaction_index VARCHAR,
  from_address VARCHAR,
  to_address VARCHAR,
  value VARCHAR,
  gas VARCHAR,
  gas_price VARCHAR,
  input VARCHAR,
  receipt_cumulative_gas_used VARCHAR,
  receipt_gas_used VARCHAR,
  receipt_contract_address VARCHAR,
  receipt_root VARCHAR,
  receipt_status VARCHAR,
  block_timestamp VARCHAR,
  block_number VARCHAR,
  block_hash VARCHAR
);

CREATE TABLE traces (               
  transaction_hash VARCHAR,
  transaction_index VARCHAR,
  from_address VARCHAR,
  to_address VARCHAR,
  value VARCHAR,
  input VARCHAR,
  output VARCHAR,
  trace_type VARCHAR,                 
  call_type VARCHAR,                   
  reward_type VARCHAR,              
  gas VARCHAR,
  gas_used VARCHAR,
  subtraces VARCHAR,
  trace_address VARCHAR,
  error VARCHAR,
  status VARCHAR,
  block_timestamp VARCHAR,             
  block_number VARCHAR,
  block_hash VARCHAR,
  trace_id VARCHAR
);

-- psql commands to import CSV data to SQL tables.
\copy traces FROM '\path\traces.csv' WITH (FORMAT csv, HEADER true);
\copy transactions FROM '\path\transactions.csv' WITH (FORMAT csv, HEADER true);

```

## Run the following code to get all the associated contracts for given DCW

```bash
-- Mimic paper Algorithm 1 using Recursive Common Table Expression.
WITH RECURSIVE associated_contracts AS (
    -- Base case: fetch contracts deployed directly by the DCW.
    SELECT
        tr.to_address AS contract_address -- 40 char hexadecimal string for Ethereum as per paper Section 2.
    FROM
        transactions t
    JOIN
        traces tr
        /*
         * We join transaction table with traces table using transaction hash (64 char hexadecimal
         * string for Ethereum as per paper Section 2). There can be multiple traces referencing
         * one transaction hash. So, we assume there is another field like trace_id in the trace
         * table making a composite primary key: (transaction_hash, trace_id).
         */
        ON t.hash = tr.transaction_hash
    WHERE
        t.from_address = '0xb735bf53abc79525a4f585a004a620d08cc66b27' -- 40 char hexadecimal string for Ethereum as per paper Section 2.
        AND tr.trace_type IN ('create')
        AND tr.to_address IS NOT NULL

    UNION

    -- Recursive case: fetch contracts deployed indirectly by the DCW.
    SELECT
        tr.to_address AS contract_address
    FROM
        transactions t
    JOIN
        traces tr
        ON t.hash = tr.transaction_hash
    JOIN
        associated_contracts ac
        -- Get transactions initiated by already deployed contracts.
        ON t.from_address = ac.contract_address 
    WHERE
        tr.trace_type IN ('create')
        AND tr.to_address IS NOT NULL
)

-- Get all associated contracts.
SELECT contract_address FROM associated_contracts;
