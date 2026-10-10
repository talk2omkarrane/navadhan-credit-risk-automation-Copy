"""Automated loan portfolio ETL and reporting demo.

Source: public L&T/Analytics Vidhya first-EMI vehicle-loan-default dataset.
This is historical demo data; it is not a live feed from Navadhan or any lender.
"""
from __future__ import annotations

import io
import logging
import os
import smtplib
import sys
import time
import urllib.request
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports"
SQL_DIR = ROOT / "sql"
DATA_URL = "https://raw.githubusercontent.com/AIduate/Vehicle-Loan-Default-Prediction/main/train.csv"

SOURCE_COLUMNS = [
    "UniqueID", "loan_default", "disbursed_amount", "asset_cost", "ltv",
    "branch_id", "manufacturer_id", "Employment.Type", "DisbursalDate",
    "State_ID", "PERFORM_CNS.SCORE", "PERFORM_CNS.SCORE.DESCRIPTION",
    "PRI.OVERDUE.ACCTS", "DELINQUENT.ACCTS.IN.LAST.SIX.MONTHS",
    "NEW.ACCTS.IN.LAST.SIX.MONTHS", "NO.OF_INQUIRIES", "CREDIT.HISTORY.LENGTH",
]
RENAME = {
    "UniqueID": "unique_id",
    "loan_default": "loan_default",
    "disbursed_amount": "disbursed_amount",
    "asset_cost": "asset_cost",
    "ltv": "ltv",
    "branch_id": "branch_id",
    "manufacturer_id": "manufacturer_id",
    "Employment.Type": "employment_type",
    "DisbursalDate": "disbursal_date",
    "State_ID": "state_id",
    "PERFORM_CNS.SCORE": "bureau_score",
    "PERFORM_CNS.SCORE.DESCRIPTION": "bureau_score_description",
    "PRI.OVERDUE.ACCTS": "primary_overdue_accounts",
    "DELINQUENT.ACCTS.IN.LAST.SIX.MONTHS": "delinquent_accounts_last_6_months",
    "NEW.ACCTS.IN.LAST.SIX.MONTHS": "new_accounts_last_6_months",
    "NO.OF_INQUIRIES": "no_of_inquiries",
    "CREDIT.HISTORY.LENGTH": "credit_history_length",
}
COLUMNS = list(RENAME.values())
INTEGER_COLUMNS = [
    "unique_id", "loan_default", "branch_id", "manufacturer_id", "state_id",
    "primary_overdue_accounts", "delinquent_accounts_last_6_months",
    "new_accounts_last_6_months", "no_of_inquiries",
]
NUMERIC_COLUMNS = ["disbursed_amount", "asset_cost", "ltv", "bureau_score"]


def configure_logging() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    log_path = REPORT_DIR / "etl.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")],
        force=True,
    )


def download_source() -> pd.DataFrame:
    """Download source CSV with a timeout and small retry policy."""
    last_error = None
    for attempt in range(1, 4):
        try:
            logging.info("Downloading historical source data (attempt %s/3)", attempt)
            request = urllib.request.Request(DATA_URL, headers={"User-Agent": "NavadhanCreditRiskDemo/1.0"})
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = response.read()
            df = pd.read_csv(io.BytesIO(payload), usecols=SOURCE_COLUMNS, low_memory=False)
            logging.info("Downloaded %s source rows", len(df))
            return df
        except Exception as exc:
            last_error = exc
            logging.warning("Source download/parse attempt failed: %s", exc)
            if attempt < 3:
                time.sleep(attempt * 3)
    raise RuntimeError(f"Could not download the public source dataset: {last_error}")


def clean_data(raw: pd.DataFrame):
    """Standardise fields and return curated data plus validation summary."""
    original_rows = len(raw)
    df = raw.rename(columns=RENAME).copy()

    for col in INTEGER_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["employment_type"] = df["employment_type"].astype("string").str.strip()
    df["employment_type"] = df["employment_type"].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    df["bureau_score_description"] = df["bureau_score_description"].astype("string").str.strip()
    df["credit_history_length"] = df["credit_history_length"].astype("string").str.strip()
    df["disbursal_date"] = pd.to_datetime(df["disbursal_date"], errors="coerce", dayfirst=True).dt.date

    invalid_id_target = df["unique_id"].isna() | df["loan_default"].isna() | ~df["loan_default"].isin([0, 1])
    invalid_id_target_count = int(invalid_id_target.sum())
    df = df.loc[~invalid_id_target].copy()

    duplicate_count = int(df["unique_id"].duplicated(keep="last").sum())
    df = df.drop_duplicates(subset=["unique_id"], keep="last").copy()

    invalid_amount = df["disbursed_amount"].isna() | (df["disbursed_amount"] <= 0)
    invalid_amount_count = int(invalid_amount.sum())
    df = df.loc[~invalid_amount].copy()

    # Do not impute business fields for this descriptive analysis. Keep optional fields null and report them.
    df = df[COLUMNS].copy()
    quality_rows = [
        {"check_name": "source_rows", "failed_rows": 0, "detail": f"Rows read from source: {original_rows}", "status": "INFO"},
        {"check_name": "missing_or_invalid_unique_id_or_target", "failed_rows": invalid_id_target_count, "detail": "Rows removed because ID/target is missing or target is not 0/1", "status": "PASS" if invalid_id_target_count == 0 else "REVIEW"},
        {"check_name": "duplicate_unique_id", "failed_rows": duplicate_count, "detail": "Duplicate IDs removed; latest source row retained", "status": "PASS" if duplicate_count == 0 else "REVIEW"},
        {"check_name": "missing_or_nonpositive_disbursed_amount", "failed_rows": invalid_amount_count, "detail": "Rows removed because disbursed amount is missing or <= 0", "status": "PASS" if invalid_amount_count == 0 else "REVIEW"},
        {"check_name": "curated_rows", "failed_rows": 0, "detail": f"Rows retained for analysis: {len(df)}", "status": "INFO"},
    ]
    for col in COLUMNS:
        missing = int(df[col].isna().sum())
        quality_rows.append({
            "check_name": f"missing_values_{col}", "failed_rows": missing,
            "detail": f"Missing values in optional/analytical field '{col}'", "status": "REVIEW" if missing else "PASS",
        })
    quality = pd.DataFrame(quality_rows)
    counts = {
        "source_rows": original_rows,
        "invalid_id_target_rows": invalid_id_target_count,
        "duplicate_rows_removed": duplicate_count,
        "invalid_amount_rows": invalid_amount_count,
        "curated_rows": len(df),
    }
    return df, quality, counts


def execute_sql_file(conn, filename: str) -> None:
    text = (SQL_DIR / filename).read_text(encoding="utf-8")
    # These project SQL files use simple DDL/query statements with semicolon delimiters.
    for statement in text.split(";"):
        statement = statement.strip()
        if statement and not statement.startswith("--"):
            with conn.cursor() as cur:
                cur.execute(statement)


def fetch_df(conn, query: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query)
        names = [desc.name for desc in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=names)


def load_data(conn, df: pd.DataFrame) -> None:
    """COPY into a staging table, then upsert by unique ID to make reruns safe."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS loan_records_stage")
        cur.execute("CREATE TEMP TABLE loan_records_stage (LIKE loan_records) ON COMMIT DROP")
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep=r"\N", date_format="%Y-%m-%d")
        buffer.seek(0)
        column_list = ", ".join(COLUMNS)
        copy_sql = f"COPY loan_records_stage ({column_list}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
        with cur.copy(copy_sql) as copy:
            copy.write(buffer.read())
        update_cols = [c for c in COLUMNS if c != "unique_id"]
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        cur.execute(
            f"INSERT INTO loan_records ({column_list}) SELECT {column_list} FROM loan_records_stage "
            f"ON CONFLICT (unique_id) DO UPDATE SET {updates}"
        )
        cur.execute("SELECT COUNT(*) FROM loan_records")
        total_after = cur.fetchone()[0]
    logging.info("Upsert completed. Total records in loan_records: %s", total_after)


def build_reports(conn, quality: pd.DataFrame, counts: dict) -> dict[str, pd.DataFrame]:
    queries = {
        "overall_kpis": """
            SELECT COUNT(*) AS total_loans,
                   ROUND(COALESCE(SUM(disbursed_amount), 0), 2) AS total_disbursed_amount,
                   ROUND(AVG(loan_default::numeric) * 100, 2) AS first_emi_default_rate_pct,
                   ROUND(AVG(ltv)::numeric, 2) AS average_ltv
            FROM loan_records
        """,
        "state_risk": """
            SELECT state_id, COUNT(*) AS total_loans,
                   ROUND(SUM(disbursed_amount), 2) AS total_disbursed_amount,
                   ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct
            FROM loan_records WHERE state_id IS NOT NULL
            GROUP BY state_id HAVING COUNT(*) >= 100
            ORDER BY default_rate_pct DESC, total_loans DESC
        """,
        "branch_risk": """
            SELECT branch_id, COUNT(*) AS total_loans,
                   ROUND(SUM(disbursed_amount), 2) AS total_disbursed_amount,
                   ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct
            FROM loan_records WHERE branch_id IS NOT NULL
            GROUP BY branch_id HAVING COUNT(*) >= 100
            ORDER BY default_rate_pct DESC, total_loans DESC
        """,
        "employment_risk": """
            SELECT COALESCE(employment_type, 'Unknown') AS employment_type,
                   COUNT(*) AS total_loans,
                   ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct,
                   ROUND(SUM(disbursed_amount), 2) AS total_disbursed_amount
            FROM loan_records GROUP BY COALESCE(employment_type, 'Unknown')
            ORDER BY default_rate_pct DESC
        """,
        "bureau_risk": """
            SELECT CASE WHEN bureau_score IS NULL OR bureau_score = 0 THEN 'No/zero bureau score'
                        WHEN bureau_score < 300 THEN 'Below 300'
                        WHEN bureau_score < 600 THEN '300-599'
                        WHEN bureau_score < 750 THEN '600-749'
                        ELSE '750+' END AS bureau_score_band,
                   COUNT(*) AS total_loans,
                   ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct
            FROM loan_records GROUP BY 1 ORDER BY 1
        """,
        "monthly_trend": """
            SELECT DATE_TRUNC('month', disbursal_date)::date AS disbursal_month,
                   COUNT(*) AS total_loans,
                   ROUND(SUM(disbursed_amount), 2) AS total_disbursed_amount,
                   ROUND(AVG(loan_default::numeric) * 100, 2) AS default_rate_pct
            FROM loan_records WHERE disbursal_date IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """,
    }
    outputs = {name: fetch_df(conn, query) for name, query in queries.items()}
    outputs["data_quality"] = quality
    outputs["run_summary"] = pd.DataFrame([{
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(), **counts
    }])

    # Use a transparent threshold for investigation, not automatic lending decisions.
    base_rows = outputs["overall_kpis"].to_dict(orient="records")
    base_rate = float(base_rows[0]["first_emi_default_rate_pct"] or 0) if base_rows else 0.0
    candidates = []
    for segment_name, key_name in [("state", "state_risk"), ("branch", "branch_risk")]:
        segment_df = outputs[key_name]
        if not segment_df.empty:
            flagged = segment_df[segment_df["default_rate_pct"].astype(float) >= base_rate + 2.0].copy()
            if not flagged.empty:
                flagged.insert(0, "segment_type", segment_name)
                candidates.append(flagged.rename(columns={"state_id": "segment_id", "branch_id": "segment_id"}))
    outputs["risk_watchlist"] = pd.concat(candidates, ignore_index=True) if candidates else pd.DataFrame(columns=["segment_type", "segment_id", "total_loans", "total_disbursed_amount", "default_rate_pct"])

    for name, df in outputs.items():
        df.to_csv(REPORT_DIR / f"{name}.csv", index=False)
    write_html_report(outputs, counts, base_rate)
    return outputs


def write_html_report(outputs: dict[str, pd.DataFrame], counts: dict, base_rate: float) -> None:
    overall = outputs["overall_kpis"].iloc[0].to_dict() if not outputs["overall_kpis"].empty else {}
    kpi_html = "".join(
        f"<li><b>{label}:</b> {value}</li>" for label, value in [
            ("Loans analysed", overall.get("total_loans", "N/A")),
            ("Total amount disbursed", overall.get("total_disbursed_amount", "N/A")),
            ("First-EMI default rate (%)", overall.get("first_emi_default_rate_pct", "N/A")),
            ("Average loan-to-value (%)", overall.get("average_ltv", "N/A")),
        ]
    )
    state = outputs["state_risk"].head(10).to_html(index=False, border=0, classes="data")
    branch = outputs["branch_risk"].head(10).to_html(index=False, border=0, classes="data")
    watch = outputs["risk_watchlist"].head(25).to_html(index=False, border=0, classes="data")
    html = f"""<!doctype html><html><head><meta charset='utf-8'><title>Loan Portfolio Risk Report</title>
    <style>body{{font-family:Arial,sans-serif;max-width:1100px;margin:32px auto;color:#1f2937;line-height:1.5}}h1,h2{{color:#12304a}}table{{border-collapse:collapse;width:100%;font-size:13px}}th,td{{padding:7px;border-bottom:1px solid #ddd;text-align:left}}th{{background:#edf2f7}}.note{{background:#f3f4f6;padding:12px;border-radius:6px}}</style></head><body>
    <h1>Automated Loan Portfolio Risk Report</h1><p>Generated (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p class='note'><b>Dataset limitation:</b> this is a public historical first-EMI default dataset used as a portfolio analytics demonstration. It is not a live data feed or internal Navadhan data. Segment-level differences are descriptive and do not establish causation.</p>
    <h2>Executive summary</h2><ul>{kpi_html}</ul>
    <h2>Data-quality and pipeline summary</h2><p>Source rows: {counts['source_rows']}; curated rows: {counts['curated_rows']}; duplicate IDs removed: {counts['duplicate_rows_removed']}; rows removed for invalid ID/target: {counts['invalid_id_target_rows']}; rows removed for invalid disbursed amount: {counts['invalid_amount_rows']}.</p>
    <h2>Highest-default state segments (minimum 100 records)</h2>{state}
    <h2>Highest-default branch segments (minimum 100 records)</h2>{branch}
    <h2>Investigation queue</h2><p>Segments shown here have at least 100 records and a first-EMI default rate at least 2 percentage points above the portfolio rate ({base_rate:.2f}%). This is an investigation threshold, not an automatic credit decision or validated policy.</p>{watch}
    <h2>Recommended business actions</h2><ol><li>Validate unusual branch/state trends with credit and operations teams before changing policy.</li><li>Review data-quality exceptions and reconcile source versus curated row counts.</li><li>Compare segment rates alongside loan volume; investigate patterns rather than interpreting correlation as causation.</li></ol>
    </body></html>"""
    (REPORT_DIR / "daily_report.html").write_text(html, encoding="utf-8")


def send_email_if_configured() -> None:
    host = os.getenv("SMTP_HOST", "").strip()
    user = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    to_addr = os.getenv("REPORT_TO", "").strip()
    if not all([host, user, password, to_addr]):
        logging.info("Email not sent: SMTP secrets are not fully configured; report files are still generated.")
        return
    port = int(os.getenv("SMTP_PORT") or "587")
    from_addr = os.getenv("REPORT_FROM", "").strip() or user
    message = EmailMessage()
    message["Subject"] = "Automated Loan Portfolio Risk Report"
    message["From"] = from_addr
    message["To"] = to_addr
    message.set_content("The scheduled loan portfolio analytics run completed. The HTML report and CSV outputs are attached.")
    html_path = REPORT_DIR / "daily_report.html"
    message.add_attachment(html_path.read_bytes(), maintype="text", subtype="html", filename=html_path.name)
    for filename in ["overall_kpis.csv", "state_risk.csv", "branch_risk.csv", "data_quality.csv", "risk_watchlist.csv", "run_summary.csv"]:
        path = REPORT_DIR / filename
        if path.exists():
            message.add_attachment(path.read_bytes(), maintype="text", subtype="csv", filename=path.name)
    logging.info("Sending email report to configured recipient")
    with smtplib.SMTP(host, port, timeout=30) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(message)
    logging.info("Email report sent")


def main() -> int:
    load_dotenv(ROOT / ".env")
    configure_logging()
    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn:
        logging.error("DATABASE_URL is missing. Add the Neon connection string as GitHub Actions secret NEON_DATABASE_URL.")
        return 2

    run_id = None
    conn = None
    started = datetime.now(timezone.utc)
    try:
        raw = download_source()
        cleaned, quality, counts = clean_data(raw)
        logging.info("Data quality summary: %s", counts)

        conn = psycopg.connect(dsn, connect_timeout=20)
        execute_sql_file(conn, "schema.sql")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_runs (status, source_url, source_rows) VALUES (%s, %s, %s) RETURNING run_id",
                ("RUNNING", DATA_URL, counts["source_rows"]),
            )
            run_id = cur.fetchone()[0]
        conn.commit()

        load_data(conn, cleaned)
        conn.commit()
        outputs = build_reports(conn, quality, counts)
        conn.commit()
        send_email_if_configured()

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_runs SET status=%s, ended_at=NOW(), source_rows=%s, loaded_rows=%s, rejected_rows=%s, error_message=NULL WHERE run_id=%s",
                ("SUCCESS", counts["source_rows"], counts["curated_rows"], counts["source_rows"] - counts["curated_rows"], run_id),
            )
        conn.commit()
        logging.info("PIPELINE SUCCEEDED. Outputs written to %s", REPORT_DIR)
        return 0
    except Exception as exc:
        logging.exception("PIPELINE FAILED: %s", exc)
        if conn is not None:
            try:
                conn.rollback()
                if run_id is not None:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE pipeline_runs SET status=%s, ended_at=NOW(), error_message=%s WHERE run_id=%s",
                            ("FAILED", str(exc)[:2000], run_id),
                        )
                    conn.commit()
            except Exception:
                logging.exception("Could not update pipeline run status in database")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
