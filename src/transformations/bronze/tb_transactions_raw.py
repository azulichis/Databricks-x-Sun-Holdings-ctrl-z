"""Bronze layer: raw ingestion of Taco Bueno daily transaction-level-detail CSVs.

Source: /Volumes/tacobueno/discounting/csvfiles/ (TB_Daily_TLD_MM-DD-YYYY-m.csv).

The source schema evolved over time (columns are ALWAYS appended at the end, the
first 19 positions are identical across all versions):
  * 19 columns: transactions up to 2025-06-28
  * 21 columns (+ Report Group, Tender Type): 2025-06-29 to 2026-06-30
  * 22 columns (+ Total Rounding): from ~2026-07-01

The schema uses the RAW header names (columns are resolved BY NAME thanks to
rescuedDataColumn), so the 19/21/22-column versions align automatically: a
column absent from a file's header (e.g. 'Tender Type' in 19-column files) is
read as NULL, and any unexpected extra column goes to _rescued_data. The
output projection renames everything to snake_case because Delta rejects
raw header names (spaces, '#'):
  store_id <- "Store #", date_raw <- "Date", total <- "Total",
  check_number <- "Check Number", time_card <- "Time Card",
  begin_time <- "Begin Time", end_time <- "End Time",
  total_discount <- "Total Discount", order_voided <- "Order Voided",
  tax <- "Tax", quantity <- "Quantity",
  transaction_type <- "Transaction Type", menu_item <- "Menu Item",
  menu_item_ext_ref <- "Menu Item Ext Ref", pop <- "Pop",
  pos_code <- "POS Code", menu_price <- "Menu Price",
  item_discount <- "Item Discount", item_voided <- "Item Voided",
  report_group <- "Report Group", tender_type <- "Tender Type",
  total_rounding <- "Total Rounding".
24 files are header-only (empty) and are ingested without failure (0 rows).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SOURCE_PATH = "/Volumes/tacobueno/discounting/csvfiles/"


BRONZE_SCHEMA = StructType(
    [
        StructField("Store #", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Total", StringType(), True),
        StructField("Check Number", StringType(), True),
        StructField("Time Card", StringType(), True),
        StructField("Begin Time", StringType(), True),
        StructField("End Time", StringType(), True),
        StructField("Total Discount", StringType(), True),
        StructField("Order Voided", StringType(), True),
        StructField("Tax", StringType(), True),
        StructField("Quantity", StringType(), True),
        StructField("Transaction Type", StringType(), True),
        StructField("Menu Item", StringType(), True),
        StructField("Menu Item Ext Ref", StringType(), True),
        StructField("Pop", StringType(), True),
        StructField("POS Code", StringType(), True),
        StructField("Menu Price", StringType(), True),
        StructField("Item Discount", StringType(), True),
        StructField("Item Voided", StringType(), True),
        StructField("Report Group", StringType(), True),
        StructField("Tender Type", StringType(), True),
        StructField("Total Rounding", StringType(), True),
        StructField("_rescued_data", StringType(), True),
    ]
)


@dp.table(
    name="tacobueno.discounting_bronze.tb_transactions_raw",
    comment=(
        "Raw line-level Taco Bueno transactions from daily CSVs (all STRING, "
        "positional 22-column schema across 3 schema versions), plus source file "
        "and ingestion timestamp metadata."
    ),
)
def tb_transactions_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("enforceSchema", "false")
        .schema(BRONZE_SCHEMA)
        .option("rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
        .select(
            F.col("`Store #`").alias("store_id"),
            F.col("`Date`").alias("date_raw"),
            F.col("`Total`").alias("total"),
            F.col("`Check Number`").alias("check_number"),
            F.col("`Time Card`").alias("time_card"),
            F.col("`Begin Time`").alias("begin_time"),
            F.col("`End Time`").alias("end_time"),
            F.col("`Total Discount`").alias("total_discount"),
            F.col("`Order Voided`").alias("order_voided"),
            F.col("`Tax`").alias("tax"),
            F.col("`Quantity`").alias("quantity"),
            F.col("`Transaction Type`").alias("transaction_type"),
            F.col("`Menu Item`").alias("menu_item"),
            F.col("`Menu Item Ext Ref`").alias("menu_item_ext_ref"),
            F.col("`Pop`").alias("pop"),
            F.col("`POS Code`").alias("pos_code"),
            F.col("`Menu Price`").alias("menu_price"),
            F.col("`Item Discount`").alias("item_discount"),
            F.col("`Item Voided`").alias("item_voided"),
            F.col("`Report Group`").alias("report_group"),
            F.col("`Tender Type`").alias("tender_type"),
            F.col("`Total Rounding`").alias("total_rounding"),
            F.col("_rescued_data"),
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
