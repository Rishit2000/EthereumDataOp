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
  block_hash VARCHAR
);

-- psql commands to import CSV data to SQL tables.
\copy traces FROM '\path\traces.csv' WITH (FORMAT csv, HEADER true);
\copy transactions FROM '\path\transactions.csv' WITH (FORMAT csv, HEADER true);
