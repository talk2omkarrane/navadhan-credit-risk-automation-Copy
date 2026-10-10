-- Core portfolio KPIs
SELECT COUNT(*) AS total_loans,
       ROUND(COALESCE(SUM(disbursed_amount), 0), 2) AS total_disbursed_amount,
       ROUND(AVG(loan_default::numeric) * 100, 2) AS first_emi_default_rate_pct,
       ROUND(AVG(ltv)::numeric, 2) AS average_ltv
FROM loan_records;

-- State-level risk (only segments with at least 100 loans)
SELECT state_id, COUNT(*) AS total_loans,
       ROUND(SUM(disbursed_amount), 2) AS total_disbursed_amount,
       ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct
FROM loan_records
WHERE state_id IS NOT NULL
GROUP BY state_id
HAVING COUNT(*) >= 100
ORDER BY default_rate_pct DESC, total_loans DESC;

-- Branch-level risk (only segments with at least 100 loans)
SELECT branch_id, COUNT(*) AS total_loans,
       ROUND(SUM(disbursed_amount), 2) AS total_disbursed_amount,
       ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct
FROM loan_records
WHERE branch_id IS NOT NULL
GROUP BY branch_id
HAVING COUNT(*) >= 100
ORDER BY default_rate_pct DESC, total_loans DESC;

-- Employment segment analysis
SELECT COALESCE(employment_type, 'Unknown') AS employment_type,
       COUNT(*) AS total_loans,
       ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct
FROM loan_records
GROUP BY COALESCE(employment_type, 'Unknown')
ORDER BY default_rate_pct DESC;

-- Monthly portfolio trend
SELECT DATE_TRUNC('month', disbursal_date)::date AS disbursal_month,
       COUNT(*) AS total_loans,
       ROUND(SUM(disbursed_amount), 2) AS total_disbursed_amount,
       ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct
FROM loan_records
WHERE disbursal_date IS NOT NULL
GROUP BY 1
ORDER BY 1;
