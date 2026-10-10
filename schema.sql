CREATE TABLE IF NOT EXISTS loan_records (
    unique_id BIGINT PRIMARY KEY,
    loan_default SMALLINT NOT NULL CHECK (loan_default IN (0, 1)),
    disbursed_amount NUMERIC(14,2) NOT NULL,
    asset_cost NUMERIC(14,2),
    ltv NUMERIC(10,3),
    branch_id INTEGER,
    manufacturer_id INTEGER,
    employment_type TEXT,
    disbursal_date DATE,
    state_id INTEGER,
    bureau_score NUMERIC(10,2),
    bureau_score_description TEXT,
    primary_overdue_accounts INTEGER,
    delinquent_accounts_last_6_months INTEGER,
    new_accounts_last_6_months INTEGER,
    no_of_inquiries INTEGER,
    credit_history_length TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_loan_records_disbursal_date ON loan_records(disbursal_date);
CREATE INDEX IF NOT EXISTS idx_loan_records_state_id ON loan_records(state_id);
CREATE INDEX IF NOT EXISTS idx_loan_records_branch_id ON loan_records(branch_id);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    source_url TEXT,
    source_rows INTEGER,
    loaded_rows INTEGER,
    rejected_rows INTEGER,
    error_message TEXT
);
