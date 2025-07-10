import psycopg2
from psycopg2.extras import RealDictCursor

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

def get_all_txns_given_hash_list(txn_hash_list):
    """ Get all raw transactions data from a list of transaction hashes."""
    if not txn_hash_list:
        return {}

    txn_hash_list = [h.lower() for h in txn_hash_list]
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = "SELECT raw_data FROM transactions WHERE LOWER(hash) = ANY(%s);"
    cursor.execute(query, (txn_hash_list,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # The raw_data column contains the original JSON object.
    return {item['raw_data']['hash'].lower(): item['raw_data'] for item in results}


def get_all_traces_given_txn_list(txn_hash_list):
    """Gets all traces associated with a list of transaction hashes."""
    if not txn_hash_list:
        return {}

    txn_hash_list = [h.lower() for h in txn_hash_list]
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = "SELECT raw_data FROM traces WHERE LOWER(transaction_hash) = ANY(%s);"
    cursor.execute(query, (txn_hash_list,))
    results = cursor.fetchall()

    cursor.close()
    conn.close()
    
    traces = {}
    for item in results:
        trace = item['raw_data']
        txn_hash = trace['transaction_hash'].lower()
        if txn_hash not in traces:
            traces[txn_hash] = []
        traces[txn_hash].append(trace)
        
    return traces


def get_all_txn_from_to_address_list(address_list):
    """Gets all transactions from and to a list of addresses."""
    if not address_list:
        return {}
        
    address_list = [addr.lower() for addr in address_list]
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT raw_data FROM transactions 
        WHERE LOWER(from_address) = ANY(%s) OR LOWER(to_address) = ANY(%s);
    """
    cursor.execute(query, (address_list, address_list))
    results = cursor.fetchall()

    cursor.close()
    conn.close()
    
    address_txns = {addr: {'from': [], 'to': []} for addr in address_list}
    for item in results:
        txn = item['raw_data']
        from_addr = txn.get('from_address', '').lower()
        to_addr = txn.get('to_address', '').lower()
        
        if from_addr in address_txns:
            address_txns[from_addr]['from'].append(txn)
        if to_addr in address_txns:
            address_txns[to_addr]['to'].append(txn)
            
    return address_txns
    

def get_all_traces_from_to_address_list(address_list):
    """Get all the traces from and to a list of addresses."""
    if not address_list:
        return {}

    address_list = [addr.lower() for addr in address_list]

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = """
        SELECT raw_data FROM traces
        WHERE LOWER(from_address) = ANY(%s) OR LOWER(to_address) = ANY(%s);
    """
    cursor.execute(query, (address_list, address_list))
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    address_traces = {addr: {'from': [], 'to': []} for addr in address_list}
    for item in results:
        trace = item['raw_data']
        from_addr = trace.get('from_address', '').lower()
        to_addr = trace.get('to_address', '').lower()

        if from_addr in address_traces:
            address_traces[from_addr]['from'].append(trace)
        if to_addr in address_traces:
            address_traces[to_addr]['to'].append(trace)

    return address_traces


def get_creation_txn_given_contract(contract_list):
    """Gets the creation transaction hash for a list of contracts."""
    if not contract_list:
        return {}
    
    contract_list = [c.lower() for c in contract_list]

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = """
        SELECT to_address, transaction_hash 
        FROM traces 
        WHERE trace_type = 'create' AND LOWER(to_address) = ANY(%s);
    """
    cursor.execute(query, (contract_list,))
    results = cursor.fetchall()

    cursor.close()
    conn.close()
    
    return {item['to_address'].lower(): item['transaction_hash'] for item in results}


def get_contracts_code(address_list):
    """Gets the bytecode for a list of contract addresses."""
    if not address_list:
        return {}

    address_list = [addr.lower() for addr in address_list]

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT address, bytecode FROM contracts WHERE LOWER(address) = ANY(%s);"
    cursor.execute(query, (address_list,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return {item['address'].lower(): item['bytecode'] for item in results}
