# cfpb-lendingclub

# Credit Complaints Lakehouse

A Databricks medallion pipeline built on two public consumer credit datasets: the **CFPB Consumer Complaint Database** and **Lending Club accepted loans**.

The final output is a **state-by-month dataset** that compares consumer complaint pressure with Lending Club default rates.

Built with **Databricks, PySpark, Delta Lake, Unity Catalog, Auto Loader, Lakeflow Declarative Pipelines, Python, and SQL**.

## Why I built it this way

The two datasets do not have a common borrower or loan identifier.

CFPB complaints are tied to companies and complaint categories, while Lending Club records are individual loans. Trying to create a row-level match using state, date, or loan characteristics would mean inventing a relationship that does not exist.

Instead, I aggregate both datasets to the same grain:

```text
state × month
```

From there I calculate:

```text
complaints_per_1k_loans = complaints / loans * 1000
```

and compare that with Lending Club default rates.

I also remove state-month segments with fewer than **100 resolved loans** because very small groups produce unstable default rates.

The result is meant to show a population-level association, not a borrower-level relationship or causal effect.

## Architecture

```text
CFPB Bulk Data ─┐
                ├─> Bronze ─> Silver ─┐
CFPB API ───────┘                     │
                                      ├─> Gold State × Month Panel
Lending Club ─────> Bronze ─> Silver ─┘
```

## Bronze

Bronze keeps the source data close to its original form and adds:

```text
_ingested_at
_source_file
```

One of the biggest issues on the Lending Club side was CSV parsing.

Fields such as `desc`, `emp_title`, and `title` contain commas, quotes, and embedded newlines. Without multiline CSV parsing, roughly **2.26M loans turned into around 30M broken rows**.

That made row-count validation an important part of ingestion.

The file also contains a footer row with a funding summary. Instead of dropping the last row blindly, I keep only records where the loan `id` is numeric.

The Lending Club bronze table uses **Auto Loader** with schema evolution enabled so new source columns can be detected instead of silently ignored.

## Silver

Silver handles cleaning, typing, deduplication, and feature creation.

Some derived fields include:

```text
fico_avg
credit_age_years
is_resolved
is_default
```

For data quality, I separate rules into two types.

Hard failures are dropped using `expect_all_or_drop`.

Suspicious but usable records are kept and logged. For example:

```text
int_rate BETWEEN 0 AND 100
dti BETWEEN 0 AND 120
```

I did not want every unusual value to disappear automatically because that could silently change downstream rate calculations.

## Gold

The main gold tables are:

```text
gold_lc_default_by_state_month
gold_cfpb_complaints_by_state_month
joined state-month panel
```

Default rate is calculated only over **resolved loans**:

```sql
100 * SUM(is_default) / NULLIF(SUM(is_resolved), 0)
```

An active loan is not a successful loan yet, so counting active loans as non-defaults would make recent periods look artificially safer.

CFPB complaints are limited to categories that have the closest overlap with Lending Club lending:

```text
personal_loan
credit_card
debt_collection
```

## CFPB Historical Backfill

The CFPB bulk file I used did not contain the full history I needed, so older complaints are pulled from the CFPB search API.

Because deep paging is limited, the backfill runs **month by month**.

Each month is written separately to:

```text
/Volumes/workspace/src/cfpb/history
```

This also makes the backfill resumable. If one month fails, I can retry that month instead of restarting the entire historical load.

## Repository

| File                                 | Purpose                                                 |
| ------------------------------------ | ------------------------------------------------------- |
| `Data_extraction.ipynb`              | CFPB bulk data ingestion                                |
| `Lending_club_data_extraction.ipynb` | Lending Club ingestion                                  |
| `CFPB_medallion_architecture.ipynb`  | CFPB bronze, silver, gold, API backfill and final panel |
| `Lending_club_medallion.py`          | Lending Club Lakeflow pipeline                          |
| `Lending_club_transformation.py`     | Lending Club transformations                            |
| `extract_libraries.ipynb`            | Dependency setup                                        |

## Running the project

Requires:

* Databricks with Unity Catalog
* `/Volumes/workspace/src/`
* S3 storage for the Lakeflow pipeline
* Databricks secret scopes for credentials

Credentials are read from secret scopes rather than stored in code.

```python
AWS_ACCESS_KEY_ID = dbutils.secrets.get(
    scope="aws-keys",
    key="AWS_ACCESS_KEY_ID"
)
```

Run in this order:

```text
extract_libraries
→ CFPB extraction
→ Lending Club extraction
→ CFPB medallion notebook
→ Lending Club Lakeflow pipeline
```

## Limitations

This project does **not** claim that complaints cause defaults.

Both metrics may move because of other things happening in a state, such as economic conditions or lending activity.

The `100 resolved loans` threshold is also a design choice rather than a statistically proven cutoff.

Finally, the Lending Club public dataset ends in **2018 Q4**, so this is a historical credit analysis rather than a current risk-monitoring system.

## Data Sources

* [CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/)
* Lending Club accepted loans, 2007–2018 Q4
