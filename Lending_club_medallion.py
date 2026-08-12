# ============================================================================
# medallion_pipeline.py  —  Lakeflow Declarative Pipeline (Lending Club)
#
#   bronze_lendingclub   STREAMING TABLE   raw, Auto Loader, all STRING
#   silver_lendingclub   MATERIALIZED VIEW typed, cleaned, de-duplicated
#   gold_*               MATERIALIZED VIEWS risk & KPI marts
#
# Publishes to the pipeline's target catalog/schema (fin_risk.medallion),
# whose managed location is on S3 — so every table's Delta files land in S3.
# 'dlt' is the compatibility import name under Lakeflow.
# ============================================================================
import dlt
from pyspark.sql import functions as F

RAW = spark.conf.get("fin_risk.raw_bucket")  # e.g. s3://your-bucket/raw


def default_rate():
    """100 * defaults / RESOLVED loans. Null (not div-by-zero) if none resolved."""
    return F.round(
        100.0 * F.sum("is_default") / F.when(F.sum("is_resolved") > 0, F.sum("is_resolved")),
        2,
    ).alias("default_rate_pct")


# ============================================================================
# BRONZE — faithful raw landing (only the footer junk row is dropped)
# ============================================================================
@dlt.table(
    name="bronze_lendingclub",
    comment="Raw Lending Club loans via Auto Loader. All columns STRING.",
    table_properties={"quality": "bronze"},
)
def bronze_lendingclub():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        # The critical parse: desc/emp_title/title hold commas, quotes AND
        # newlines; without these ~2.26M loans shatter into ~30M fragments.
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{RAW}/lendingclub/")
        .where(F.col("id").rlike("^[0-9]+$"))  # drop the 'Total amount funded...' footer
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ============================================================================
# SILVER — typed, cleaned, de-duplicated, with derived risk fields
# ============================================================================
@dlt.table(
    name="silver_lendingclub",
    comment="Typed & cleaned loans. Origination-time features vs outcome fields.",
    table_properties={"quality": "silver"},
)
# Hard drops: rows missing core fields, or with non-positive amounts, are invalid.
@dlt.expect_all_or_drop({
    "core_not_null": "loan_amnt IS NOT NULL AND grade IS NOT NULL AND loan_status IS NOT NULL",
    "positive_amount": "loan_amnt > 0",
})
# Soft warns: flagged in the event log, rows kept.
@dlt.expect("plausible_rate", "int_rate BETWEEN 0 AND 100")
@dlt.expect("plausible_dti", "dti BETWEEN 0 AND 120")
def silver_lendingclub():
    b = dlt.read("bronze_lendingclub")

    def pct(c):
        return F.regexp_replace(F.col(c), "%", "").cast("double")

    s = (
        b
        # ----- identifiers & dates -----
        .withColumn("loan_id", F.col("id").cast("long"))
        .withColumn("issue_date", F.to_date("issue_d", "MMM-yyyy"))
        .withColumn("earliest_cr_date", F.to_date("earliest_cr_line", "MMM-yyyy"))
        # ----- ORIGINATION-TIME FEATURES (safe for scoring) -----
        .withColumn("loan_amnt", F.col("loan_amnt").cast("double"))
        .withColumn("funded_amnt", F.col("funded_amnt").cast("double"))
        .withColumn("term_months", F.regexp_extract("term", r"(\d+)", 1).cast("int"))
        .withColumn("int_rate", pct("int_rate"))
        .withColumn("installment", F.col("installment").cast("double"))
        .withColumn(
            "emp_length_years",
            F.when(F.col("emp_length").rlike(r"10\+"), F.lit(10))
            .when(F.col("emp_length").rlike("< 1"), F.lit(0))
            .otherwise(F.regexp_extract("emp_length", r"(\d+)", 1).cast("int")),
        )
        .withColumn("annual_inc", F.col("annual_inc").cast("double"))
        .withColumn("dti", F.col("dti").cast("double"))
        .withColumn("fico_low", F.col("fico_range_low").cast("int"))
        .withColumn("fico_high", F.col("fico_range_high").cast("int"))
        .withColumn("fico_avg", (F.col("fico_range_low").cast("double") + F.col("fico_range_high").cast("double")) / 2)
        .withColumn("revol_util", pct("revol_util"))
        .withColumn("revol_bal", F.col("revol_bal").cast("double"))
        .withColumn("open_acc", F.col("open_acc").cast("int"))
        .withColumn("total_acc", F.col("total_acc").cast("int"))
        .withColumn("delinq_2yrs", F.col("delinq_2yrs").cast("int"))
        .withColumn("inq_last_6mths", F.col("inq_last_6mths").cast("int"))
        .withColumn("pub_rec", F.col("pub_rec").cast("int"))
        .withColumn("pub_rec_bankruptcies", F.col("pub_rec_bankruptcies").cast("int"))
        .withColumn("mort_acc", F.col("mort_acc").cast("int"))
        .withColumn("tot_cur_bal", F.col("tot_cur_bal").cast("double"))
        # ----- OUTCOME / POST-ORIGINATION (analytics ONLY) -----
        .withColumn("out_prncp", F.col("out_prncp").cast("double"))
        .withColumn("total_pymnt", F.col("total_pymnt").cast("double"))
        .withColumn("total_rec_prncp", F.col("total_rec_prncp").cast("double"))
        .withColumn("total_rec_int", F.col("total_rec_int").cast("double"))
        .withColumn("recoveries", F.col("recoveries").cast("double"))
        .withColumn("last_pymnt_amnt", F.col("last_pymnt_amnt").cast("double"))
        # ----- DERIVED RISK / ECONOMICS -----
        .withColumn("credit_age_years", F.round(F.months_between("issue_date", "earliest_cr_date") / 12, 1))
        .withColumn(
            "installment_to_income",
            F.when(F.col("annual_inc") > 0, F.round(F.col("installment") * 12 / F.col("annual_inc"), 4)),
        )
        .withColumn("is_resolved", F.when(F.lower("loan_status").rlike("fully paid|charged off|default"), 1).otherwise(0))
        .withColumn("is_default", F.when(F.lower("loan_status").rlike("charged off|default"), 1).otherwise(0))
        .withColumn("net_cash", F.col("total_pymnt") - F.col("funded_amnt"))
        .withColumn(
            "charge_off_loss",
            F.when(
                F.col("is_default") == 1,
                F.greatest(F.col("funded_amnt") - F.col("total_rec_prncp") - F.col("recoveries"), F.lit(0.0)),
            ).otherwise(F.lit(0.0)),
        )
    )

    keep = [
        "loan_id", "issue_date", "earliest_cr_date", "credit_age_years",
        "loan_amnt", "funded_amnt", "term_months", "int_rate", "installment",
        "grade", "sub_grade", "emp_length_years", "emp_title", "home_ownership",
        "annual_inc", "verification_status", "purpose", "title", "addr_state", "zip_code",
        "dti", "fico_low", "fico_high", "fico_avg", "revol_util", "revol_bal",
        "open_acc", "total_acc", "delinq_2yrs", "inq_last_6mths", "pub_rec",
        "pub_rec_bankruptcies", "mort_acc", "tot_cur_bal", "application_type",
        "installment_to_income",
        "loan_status", "is_resolved", "is_default",
        "out_prncp", "total_pymnt", "total_rec_prncp", "total_rec_int", "recoveries",
        "last_pymnt_amnt", "net_cash", "charge_off_loss",
        "_ingested_at", "_source_file",
    ]
    # De-dup on loan_id (guards against overlapping concatenated source files).
    return s.select(*keep).dropDuplicates(["loan_id"])


# ============================================================================
# GOLD — risk & KPI marts (materialized views)
# ============================================================================
@dlt.table(name="gold_portfolio_kpis", comment="Headline portfolio health (one row).")
def gold_portfolio_kpis():
    return dlt.read("silver_lendingclub").agg(
        F.count("*").alias("total_loans"),
        F.round(F.sum("funded_amnt") / 1e9, 2).alias("total_funded_bn"),
        F.round(F.avg("loan_amnt"), 0).alias("avg_loan_amnt"),
        F.round(F.sum(F.col("funded_amnt") * F.col("int_rate")) / F.sum("funded_amnt"), 2).alias("wavg_int_rate_pct"),
        F.round(F.avg("fico_avg"), 0).alias("avg_fico"),
        F.round(F.avg("dti"), 1).alias("avg_dti"),
        default_rate(),
        F.round(F.sum("charge_off_loss") / 1e6, 1).alias("charge_off_loss_mn"),
        F.round(100.0 * F.sum("net_cash") / F.sum("funded_amnt"), 2).alias("realized_return_pct"),
    )


@dlt.table(name="gold_risk_by_grade", comment="Risk-return frontier by grade.")
def gold_risk_by_grade():
    return (
        dlt.read("silver_lendingclub")
        .where(F.col("grade").rlike("^[A-G]$"))
        .groupBy("grade")
        .agg(
            F.count("*").alias("loans"),
            F.round(F.sum("funded_amnt") / 1e6, 1).alias("funded_mn"),
            F.round(F.avg("int_rate"), 2).alias("avg_int_rate_pct"),
            F.round(F.avg("fico_avg"), 0).alias("avg_fico"),
            default_rate(),
            F.round(100.0 * F.sum("charge_off_loss") / F.sum("funded_amnt"), 2).alias("loss_rate_pct"),
            F.round(100.0 * F.sum("net_cash") / F.sum("funded_amnt"), 2).alias("realized_return_pct"),
        )
        .orderBy("grade")
    )


@dlt.table(name="gold_vintage", comment="Cohort performance by origination year.")
def gold_vintage():
    return (
        dlt.read("silver_lendingclub")
        .where(F.col("issue_date").isNotNull())
        .groupBy(F.year("issue_date").alias("vintage_year"))
        .agg(
            F.count("*").alias("loans"),
            F.round(F.sum("funded_amnt") / 1e6, 1).alias("funded_mn"),
            F.round(F.avg("int_rate"), 2).alias("avg_int_rate_pct"),
            default_rate(),
        )
        .orderBy("vintage_year")
    )


@dlt.table(name="gold_origination_trend", comment="Monthly volume & yield.")
def gold_origination_trend():
    return (
        dlt.read("silver_lendingclub")
        .where(F.col("issue_date").isNotNull())
        .groupBy(F.date_trunc("month", F.col("issue_date")).alias("issue_month"))
        .agg(
            F.count("*").alias("loans"),
            F.round(F.sum("funded_amnt") / 1e6, 2).alias("funded_mn"),
            F.round(F.avg("int_rate"), 2).alias("avg_int_rate_pct"),
        )
        .orderBy("issue_month")
    )


@dlt.table(name="gold_geo_risk", comment="Concentration & risk by state.")
def gold_geo_risk():
    return (
        dlt.read("silver_lendingclub")
        .where(F.col("addr_state").isNotNull())
        .groupBy(F.col("addr_state").alias("state"))
        .agg(
            F.count("*").alias("loans"),
            F.round(F.sum("funded_amnt") / 1e6, 1).alias("funded_mn"),
            F.round(F.avg("int_rate"), 2).alias("avg_int_rate_pct"),
            default_rate(),
        )
        .orderBy(F.desc("loans"))
    )


@dlt.table(name="gold_purpose_risk", comment="Risk by loan purpose.")
def gold_purpose_risk():
    return (
        dlt.read("silver_lendingclub")
        .where(F.col("purpose").isNotNull())
        .groupBy("purpose")
        .agg(
            F.count("*").alias("loans"),
            F.round(F.sum("funded_amnt") / 1e6, 1).alias("funded_mn"),
            F.round(F.avg("int_rate"), 2).alias("avg_int_rate_pct"),
            default_rate(),
        )
        .orderBy(F.desc("loans"))
    )


@dlt.table(name="gold_fico_band_risk", comment="Does FICO rank-order default?")
def gold_fico_band_risk():
    band = (
        F.when(F.col("fico_avg") < 660, "1: <660 (subprime)")
        .when(F.col("fico_avg") < 700, "2: 660-699")
        .when(F.col("fico_avg") < 740, "3: 700-739")
        .when(F.col("fico_avg") < 780, "4: 740-779")
        .otherwise("5: 780+")
    )
    return (
        dlt.read("silver_lendingclub")
        .where(F.col("fico_avg").isNotNull())
        .groupBy(band.alias("fico_band"))
        .agg(
            F.count("*").alias("loans"),
            F.round(F.avg("int_rate"), 2).alias("avg_int_rate_pct"),
            default_rate(),
        )
        .orderBy("fico_band")
    )


@dlt.table(name="gold_dti_band_risk", comment="Affordability (DTI) vs default.")
def gold_dti_band_risk():
    band = (
        F.when(F.col("dti") < 10, "1: <10")
        .when(F.col("dti") < 20, "2: 10-20")
        .when(F.col("dti") < 30, "3: 20-30")
        .when(F.col("dti") < 40, "4: 30-40")
        .otherwise("5: 40+")
    )
    return (
        dlt.read("silver_lendingclub")
        .where((F.col("dti").isNotNull()) & (F.col("dti") >= 0))
        .groupBy(band.alias("dti_band"))
        .agg(
            F.count("*").alias("loans"),
            default_rate(),
        )
        .orderBy("dti_band")
    )