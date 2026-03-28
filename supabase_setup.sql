-- ============================================================
-- Regulatory Reporting: Ledger Balance & Risk Tables
-- Run this in Supabase SQL Editor
-- ============================================================

-- Drop tables if they exist (idempotent setup)
DROP TABLE IF EXISTS risk_table;
DROP TABLE IF EXISTS ledger_balance;

-- ── Ledger Balance Table ──────────────────────────────────
CREATE TABLE ledger_balance (
    id               SERIAL PRIMARY KEY,
    account_id       VARCHAR(20)    NOT NULL,
    account_name     VARCHAR(100),
    counterparty_id  VARCHAR(20),
    counterparty_name VARCHAR(100),
    currency         VARCHAR(3),
    amount           NUMERIC(18,2),
    balance_date     DATE,
    product_type     VARCHAR(50),
    region           VARCHAR(50),
    legal_entity     VARCHAR(20),
    status           VARCHAR(20)    DEFAULT 'ACTIVE',
    created_at       TIMESTAMP      DEFAULT NOW()
);

-- ── Risk Table ────────────────────────────────────────────
CREATE TABLE risk_table (
    id               SERIAL PRIMARY KEY,
    account_id       VARCHAR(20)    NOT NULL,
    counterparty_id  VARCHAR(20),
    currency         VARCHAR(3),
    amount           NUMERIC(18,2),
    notional         NUMERIC(18,2),
    exposure         NUMERIC(18,2),
    risk_date        DATE,
    risk_type        VARCHAR(50),
    region           VARCHAR(50),
    legal_entity     VARCHAR(20),
    status           VARCHAR(20)    DEFAULT 'ACTIVE',
    created_at       TIMESTAMP      DEFAULT NOW()
);

-- ── Sample Ledger Data (15 rows) ──────────────────────────
INSERT INTO ledger_balance
    (account_id, account_name, counterparty_id, counterparty_name, currency, amount, balance_date, product_type, region, legal_entity)
VALUES
    ('ACC001', 'Alpha Corp',       'CP001', 'Beta Ltd',     'USD', 1000000.00, '2024-01-31', 'Loan',       'AMERICAS', 'LE_US'),
    ('ACC001', 'Alpha Corp',       'CP001', 'Beta Ltd',     'EUR',  500000.00, '2024-01-31', 'Bond',       'EMEA',     'LE_UK'),
    ('ACC002', 'Gamma Inc',        'CP002', 'Delta SA',     'USD', 2500000.00, '2024-01-31', 'Derivative', 'AMERICAS', 'LE_US'),
    ('ACC002', 'Gamma Inc',        'CP002', 'Delta SA',     'GBP',  750000.00, '2024-01-31', 'FX',         'EMEA',     'LE_UK'),
    ('ACC003', 'Epsilon LLC',      'CP003', 'Zeta Corp',    'USD', 1800000.00, '2024-01-31', 'Equity',     'AMERICAS', 'LE_US'),
    ('ACC003', 'Epsilon LLC',      'CP003', 'Zeta Corp',    'EUR',  920000.00, '2024-01-31', 'Loan',       'EMEA',     'LE_UK'),
    ('ACC004', 'Eta Partners',     'CP004', 'Theta Fund',   'USD', 3200000.00, '2024-01-31', 'Bond',       'APAC',     'LE_SG'),
    ('ACC004', 'Eta Partners',     'CP004', 'Theta Fund',   'JPY',15000000.00, '2024-01-31', 'Derivative', 'APAC',     'LE_SG'),
    ('ACC005', 'Iota Holdings',    'CP005', 'Kappa Group',  'USD',  670000.00, '2024-01-31', 'FX',         'AMERICAS', 'LE_US'),
    ('ACC005', 'Iota Holdings',    'CP005', 'Kappa Group',  'EUR',  430000.00, '2024-01-31', 'Equity',     'EMEA',     'LE_UK'),
    ('ACC006', 'Lambda Bank',      'CP006', 'Mu Finance',   'USD', 4500000.00, '2024-01-31', 'Loan',       'EMEA',     'LE_UK'),
    ('ACC007', 'Nu Capital',       'CP007', 'Xi Invest',    'USD',  890000.00, '2024-01-31', 'Bond',       'APAC',     'LE_SG'),
    ('ACC008', 'Omicron Funds',    'CP008', 'Pi Asset',     'GBP', 1200000.00, '2024-01-31', 'Derivative', 'EMEA',     'LE_UK'),
    ('ACC009', 'Rho Securities',   'CP009', 'Sigma Corp',   'USD', 2100000.00, '2024-01-31', 'FX',         'AMERICAS', 'LE_US'),
    ('ACC010', 'Tau Investments',  'CP010', 'Upsilon Ltd',  'EUR',  780000.00, '2024-01-31', 'Loan',       'EMEA',     'LE_UK');

-- ── Sample Risk Data (16 rows, with intentional discrepancies) ──
-- Discrepancies introduced:
--   ACC001/EUR  → risk amount 510000 vs ledger 500000  (AMOUNT BREAK)
--   ACC003/USD  → risk amount 1750000 vs ledger 1800000 (AMOUNT BREAK)
--   ACC004/JPY  → NOT in risk table                    (LEDGER ONLY break)
--   ACC009/USD  → NOT in risk table                    (LEDGER ONLY break)
--   ACC011/USD  → ONLY in risk table                   (RISK ONLY break)
INSERT INTO risk_table
    (account_id, counterparty_id, currency, amount, notional, exposure, risk_date, risk_type, region, legal_entity)
VALUES
    ('ACC001', 'CP001', 'USD', 1000000.00, 1200000.00,  950000.00, '2024-01-31', 'Credit Risk',      'AMERICAS', 'LE_US'),
    ('ACC001', 'CP001', 'EUR',  510000.00,  600000.00,  480000.00, '2024-01-31', 'Market Risk',      'EMEA',     'LE_UK'),  -- BREAK: 10000 more
    ('ACC002', 'CP002', 'USD', 2500000.00, 2800000.00, 2300000.00, '2024-01-31', 'Credit Risk',      'AMERICAS', 'LE_US'),
    ('ACC002', 'CP002', 'GBP',  750000.00,  850000.00,  700000.00, '2024-01-31', 'Market Risk',      'EMEA',     'LE_UK'),
    ('ACC003', 'CP003', 'USD', 1750000.00, 2000000.00, 1650000.00, '2024-01-31', 'Operational Risk', 'AMERICAS', 'LE_US'),  -- BREAK: 50000 less
    ('ACC003', 'CP003', 'EUR',  920000.00, 1050000.00,  870000.00, '2024-01-31', 'Market Risk',      'EMEA',     'LE_UK'),
    ('ACC004', 'CP004', 'USD', 3200000.00, 3500000.00, 3000000.00, '2024-01-31', 'Credit Risk',      'APAC',     'LE_SG'),
    -- ACC004/JPY intentionally omitted (LEDGER ONLY)
    ('ACC005', 'CP005', 'USD',  670000.00,  750000.00,  630000.00, '2024-01-31', 'Liquidity Risk',   'AMERICAS', 'LE_US'),
    ('ACC005', 'CP005', 'EUR',  430000.00,  500000.00,  410000.00, '2024-01-31', 'Market Risk',      'EMEA',     'LE_UK'),
    ('ACC006', 'CP006', 'USD', 4500000.00, 5000000.00, 4200000.00, '2024-01-31', 'Credit Risk',      'EMEA',     'LE_UK'),
    ('ACC007', 'CP007', 'USD',  890000.00, 1000000.00,  840000.00, '2024-01-31', 'Market Risk',      'APAC',     'LE_SG'),
    ('ACC008', 'CP008', 'GBP', 1200000.00, 1350000.00, 1100000.00, '2024-01-31', 'Credit Risk',      'EMEA',     'LE_UK'),
    -- ACC009/USD intentionally omitted (LEDGER ONLY)
    ('ACC010', 'CP010', 'EUR',  780000.00,  900000.00,  740000.00, '2024-01-31', 'Market Risk',      'EMEA',     'LE_UK'),
    ('ACC011', 'CP011', 'USD',  350000.00,  400000.00,  320000.00, '2024-01-31', 'Operational Risk', 'AMERICAS', 'LE_US');  -- RISK ONLY

-- Verify
SELECT 'ledger_balance' AS table_name, COUNT(*) AS row_count FROM ledger_balance
UNION ALL
SELECT 'risk_table',                   COUNT(*)               FROM risk_table;
