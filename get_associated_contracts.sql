-- Mimic paper Algorithm 1 using Recursive Common Table Expression.
WITH RECURSIVE associated_contracts AS (
    -- Base case: fetch contracts deployed directly by the DCW.
    SELECT
        tr.to_address AS contract_address -- 40 char hexadecimal string for Ethereum as per paper Section 2.
    FROM
        public.transactions t
    JOIN
        public.traces tr
        /*
         * We join transaction table with traces table using transaction hash (64 char hexadecimal
         * string for Ethereum as per paper Section 2). There can be multiple traces referencing
         * one transaction hash. So, we assume there is another field like trace_id in the trace
         * table making a composite primary key: (transaction_hash, trace_id).
         */
        ON t.hash = tr.transaction_hash
    WHERE
        t.from_address = '0x4b0c6f0297cee4b551b6fab3277067b64b238990' -- 40 char hexadecimal string for Ethereum as per paper Section 2.
        AND tr.trace_type IN ('create')
        AND tr.to_address IS NOT NULL

    UNION

    -- Recursive case: fetch contracts deployed indirectly by the DCW.
    SELECT
        tr.to_address AS contract_address
    FROM
        public.transactions t
    JOIN
        public.traces tr
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