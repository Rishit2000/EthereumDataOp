import psycopg2
import json
import gzip
from pathlib import Path
import os
from tqdm import tqdm
import multiprocessing as mp

# --- Paths to your data folders ---
traces_gz_folder = Path(os.environ.get("TRACES_GZ_FOLDER", "/tmp"))
txn_gz_folder = Path(os.environ.get("TXN_GZ_FOLDER", "/tmp"))
contracts_gz_folder = Path(os.environ.get("CONTRACTS_GZ_FOLDER", "/tmp"))

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname="your_db_name",
        user="your_db_user",
        password="your_db_password",
        host="your_db_host",
        port="your_db_port"
    )
    return conn

def create_tables_and_indexes():
    """Connects to the DB and creates tables and indexes if they don't exist."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Create tables.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contracts (
                address VARCHAR PRIMARY KEY,
                bytecode TEXT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                hash VARCHAR PRIMARY KEY,
                from_address VARCHAR,
                to_address VARCHAR,
                block_number BIGINT,
                raw_data TEXT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traces (
                transaction_hash VARCHAR,
                trace_type VARCHAR,
                from_address VARCHAR,
                to_address VARCHAR,
                raw_data TEXT
            );
        """)

        # Create indexes.
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_from_address ON transactions(LOWER(from_address));")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_to_address ON transactions(LOWER(to_address));")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_traces_transaction_hash ON traces(LOWER(transaction_hash));")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_traces_from_address ON traces(LOWER(from_address));")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_traces_to_address ON traces(LOWER(to_address));")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_traces_type_and_to_address ON traces(trace_type, LOWER(to_address));")

        conn.commit()
        cursor.close()
    except psycopg2.Error as e:
        print(f"Database error during creation: {e}")
    finally:
        if conn:
            conn.close()

def load_json_gz_file(file_path):
    """Generator to yield JSON objects from a gzipped file."""
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                yield json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

def process_transaction_file(gz_file):
    """Worker function to process a single transaction file and insert into DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    transactions = load_json_gz_file(gz_file)
    
    records_to_insert = [
        (txn.get('hash'), txn.get('from_address'), txn.get('to_address'), txn.get('block_number'), json.dumps(txn))
        for txn in transactions if txn.get('hash')
    ]

    if records_to_insert:
        insert_query = """
            INSERT INTO transactions (hash, from_address, to_address, block_number, raw_data)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (hash) DO NOTHING;
        """
        cursor.executemany(insert_query, records_to_insert)
        conn.commit()

    cursor.close()
    conn.close()

def process_trace_file(gz_file):
    """Worker function to process a single trace file and insert into DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    traces = load_json_gz_file(gz_file)
    
    records_to_insert = [
        (
            trace.get('transaction_hash'),
            trace.get('trace_type'),
            trace.get('from_address'),
            trace.get('to_address'),
            json.dumps(trace)
        )
        for trace in traces if trace.get('transaction_hash')
    ]
    
    if records_to_insert:
        insert_query = """
            INSERT INTO traces (transaction_hash, trace_type, from_address, to_address, raw_data)
            VALUES (%s, %s, %s, %s, %s);
        """
        cursor.executemany(insert_query, records_to_insert)
        conn.commit()

    cursor.close()
    conn.close()

def process_contract_file(gz_file):
    """Worker function to process a single contract file and insert into DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    contracts = load_json_gz_file(gz_file)
    
    records_to_insert = [
        (contract.get('address'), contract.get('bytecode'))
        for contract in contracts if contract.get('address')
    ]

    if records_to_insert:
        insert_query = """
            INSERT INTO contracts (address, bytecode)
            VALUES (%s, %s)
            ON CONFLICT (address) DO NOTHING;
        """
        cursor.executemany(insert_query, records_to_insert)
        conn.commit()

    cursor.close()
    conn.close()

def insert_data(name, worker_func, folder_path):
    """Generic function to run a parallel data ingestion process."""
    gz_files = list(folder_path.glob('*.gz'))
    if not gz_files:
        print(f"No .gz files found in {folder_path} for {name} ingestion. Skipping.")
        return

    with mp.Pool(processes=mp.cpu_count()) as pool:
        for _ in list(tqdm(pool.imap_unordered(worker_func, gz_files), total=len(gz_files), desc=f"Ingesting {name}")):
            pass

if __name__ == '__main__':
    # Set up the database.
    create_tables_and_indexes()

    # Run ingestions in parallel.
    insert_data("transactions", process_transaction_file, txn_gz_folder)
    insert_data("traces", process_trace_file, traces_gz_folder)
    insert_data("contracts", process_contract_file, contracts_gz_folder)