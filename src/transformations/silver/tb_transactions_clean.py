"""Silver layer: typed, cleaned and harmonized Taco Bueno transactions (CURRENT POS structure).

The POS changed between 2025-11 and 2026-01 (stores migrated gradually). The structure in
force from 2026-02 onwards is the one that stays, so every row, old or new, is normalized
to it here. The raw legacy values stay available (`source_quantity`, `pos_regime`) so any
conversion can be traced and undone.

Current structure (target):
  * one row per UNIT sold (quantity = 1);
  * channel = Drive Thru / Dine-In / Carry Out / Online / Online: <partner> / Mobile;
  * check `Total` = sum of lines - discount + tax;
  * discounts live at the check level (`check_discount`), plus negative-price "(Coupon)" lines;
  * `begin_time` carries AM/PM.

Normalizations applied to legacy rows (pos_regime = 'legacy'):
  * legacy lines with quantity 0 (about 19% of legacy lines) are combo components and modifiers: they
    are never charged on their own (the check Total matches the sum of price x quantity with them at 0).
    The current structure lists them as one-unit rows with no charge, so they become quantity 1 with
    gross_amount 0 and are flagged `is_component_line` (menu_price keeps the list price for reference);
  * lines with quantity 2..MAX_EXPLODE_QTY are expanded to one row per unit; the line-level
    the line-level discount is split evenly across the units (sums of gross and discount are unchanged).
    Lines above MAX_EXPLODE_QTY are bulk orders: kept as one row and flagged `is_bulk_line`;
  * `check_total` restates the legacy `Total` (which had no tax and no discount) as
    gross - discount + tax (the raw `Total` is not kept; bronze still has it);
  * legacy channel codes (To Go, Inside, 3) have no reliable equivalent in the current
    taxonomy (store-level correlations 0.68 / 0.06 / -0.07), so they are labelled
    'Legacy (sin mapeo)' instead of being guessed.

Business flags (computed once here, reused by gold): is_coupon_line, is_donation, is_void_line,
is_component_line, is_addon_line, is_sale_line, is_bulk_line, hour_24, channel_group, pos_regime.
`is_coupon_line` means any discount line (negative price with Coupon / OrderReduction / Bogo in its
pos_code); `promo_type` and `promo_code` give the mechanism and the promotion identifier.

Redundant raw columns are not carried: `total` (replaced by check_total), `order_voided` / `item_voided`
(replaced by the *_flag columns) and `item_discount` (identical to discount_amount). The check-level
discount is `check_discount`; the line-level one is `discount_amount`.

Data quality: ALL expectations of the project live in this table. Rows that cannot be keyed
(no date, store or check number) are dropped; everything else is kept and monitored, because
the anomalies are informative (e.g. coupon lines carry negative prices on purpose).

Known data anomalies (kept, monitored by warn-level expectations):
  * Free items where Item Discount > gross (e.g. 'Chicken - Side').
  * Pop = '3' rows (legacy code, meaning unknown).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Everything from 2025-01-01 onwards. Set to "2026-02-01" to keep only the current structure.
SILVER_START_DATE = "2025-01-01"

# The 'Report Group' column only exists in source files from this date on.
REPORT_GROUP_ERA_START = "2025-06-29"

# Legacy (pre-migration) channel codes.
LEGACY_POPS = ("To Go", "Inside", "3")

# Lines with 2..MAX_EXPLODE_QTY units are expanded to one row per unit; larger ones are bulk orders.
MAX_EXPLODE_QTY = 50

_TIME_PAT = r"^\s*(\d{1,2}):(\d{2})\s*([AaPp])[Mm]?\s*$"  

# Discount lines are NEGATIVE-price lines whose pos_code names the promotion mechanism
_DISCOUNT_CODE = r"(?i)(coupon|orderreduction|bogo)"

# Add-ons / extras: priced lines with no menu_item, identified only by pos_code 
_ADDON_CODE = r"(?i)^(XT |\*?ADD )"

# ── Data-quality expectations ───────────────────────
# Hard rules: a row without these cannot be keyed or dated, so it is dropped.
HARD_RULES = {
    "valid_txn_date": "txn_date IS NOT NULL",
    "valid_store_id": "store_id IS NOT NULL",
    "valid_check_number": "check_number IS NOT NULL",
}

# Soft rules: the row is kept and the violation is counted in the pipeline UI.
SOFT_RULES = {
    "txn_date_not_in_future": "txn_date IS NULL OR txn_date <= current_date()",
    "known_transaction_type": "transaction_type IN ('Regular Sale', 'Discount', 'Void')",
    "known_channel": "channel_group NOT IN ('Unmapped', 'Unknown')",
    "parseable_begin_time": "hour_24 IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL AND quantity >= 1",
    "single_unit_rows": "quantity = 1 OR is_bulk_line",
    "price_not_negative_unless_coupon": "menu_price IS NULL OR menu_price >= 0 OR is_coupon_line",
    "promo_code_present": "NOT is_coupon_line OR promo_code IS NOT NULL",
    "discounts_not_negative": (
        "(discount_amount IS NULL OR discount_amount >= 0) "
        "AND (check_discount IS NULL OR check_discount >= 0)"
    ),
    "discount_within_gross": (
        "is_coupon_line OR discount_amount IS NULL OR gross_amount IS NULL "
        "OR discount_amount <= gross_amount + 0.01"
    ),
    "tax_not_negative": "tax IS NULL OR tax >= 0",
    "check_total_not_negative": "check_total IS NULL OR check_total >= 0",
    "void_lines_have_no_value": "transaction_type <> 'Void' OR gross_amount IS NULL OR gross_amount = 0",
    "menu_item_present_on_sales": "NOT is_sale_line OR is_addon_line OR menu_item IS NOT NULL",
    "components_only_in_legacy": "NOT is_component_line OR pos_regime = 'legacy'",
    "pos_code_present": "pos_code IS NOT NULL",
}


def report_group_map():
    """Majority menu_item -> report_group mapping from the era that has both.

    Built as a static (batch) read of bronze, so it is joined to the streaming
    bronze read as a stream-static join (dimension-table pattern). Only 4
    breakfast items map to 2 different groups (~2K rows of 65M), so the
    majority group per item is used. Items with no mapping (discontinued
    products) stay NULL downstream.
    """
    counts = (
        spark.read.table("tacobueno.discounting_bronze.tb_transactions_raw")
        .where(
            F.col("report_group").isNotNull()
            & F.col("menu_item").isNotNull()
            & (_parse_date(F.col("date_raw")) >= F.to_date(F.lit(REPORT_GROUP_ERA_START)))
        )
        .groupBy("menu_item", "report_group")
        .count()
    )
    return (
        counts.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("menu_item").orderBy(F.col("count").desc())
            ),
        )
        .where(F.col("rn") == 1)
        .select(
            F.col("menu_item").alias("map_menu_item"),
            F.col("report_group").alias("mapped_report_group"),
        )
    )


def _parse_date(c):
    """Parse 'M/d/yyyy' or 'yyyy-MM-dd' without raising on the other format (safe with ANSI mode on)."""
    us = F.regexp_extract(c, r"^\s*(\d{1,2}/\d{1,2}/\d{4})", 1)
    iso = F.regexp_extract(c, r"^\s*(\d{4}-\d{2}-\d{2})", 1)
    return F.when(us != "", F.to_date(us, "M/d/yyyy")).when(iso != "", F.to_date(iso, "yyyy-MM-dd"))


def _num(name, dtype):
    """String -> number; an unparseable value becomes NULL (and is caught by the expectations).

    Integer columns go through DOUBLE first as a defensive measure: try_cast('1.0' AS INT) is NULL.
    (A check on the raw files showed every quantity already parses as an integer.)
    """
    if dtype in ("INT", "BIGINT"):
        return F.expr(f"try_cast(try_cast(`{name}` AS DOUBLE) AS {dtype})")
    return F.expr(f"try_cast(`{name}` AS {dtype})")


def _hour_24():
    """Hour of day 0-23 from begin_time ("10:47 AM" -> 10, "10:47 PM" -> 22); NULL if unparseable."""
    ok = F.col("begin_time").rlike(_TIME_PAT)
    h12 = F.when(ok, F.regexp_extract("begin_time", _TIME_PAT, 1).cast("int"))
    pm = F.when(ok, F.upper(F.regexp_extract("begin_time", _TIME_PAT, 3)) == "P")
    return F.when(ok & (h12 >= 1) & (h12 <= 12), (h12 % 12) + F.when(pm, 12).otherwise(0))


def transform(bronze, rg_map):
    """Pure transformation (works on streaming or batch DataFrames); `rg_map` = report_group_map()."""
    pop = F.col("pop")

    # 1) Typing, snake_case names, date parsing and start-date filter.
    typed = bronze.select(
        _num("store_id", "INT").alias("store_id"),
        _parse_date(F.col("date_raw")).alias("txn_date"),
        _num("total", "DOUBLE").alias("total_raw"),
        _num("check_number", "BIGINT").alias("check_number"),
        F.col("time_card").alias("time_card"),
        F.col("begin_time").alias("begin_time"),
        F.col("end_time").alias("end_time"),
        _num("total_discount", "DOUBLE").alias("check_discount"),
        _num("order_voided", "DOUBLE").alias("order_voided"),
        _num("tax", "DOUBLE").alias("tax"),
        _num("quantity", "INT").alias("source_quantity"),
        F.col("transaction_type").alias("transaction_type"),
        F.col("menu_item").alias("menu_item"),
        F.col("menu_item_ext_ref").alias("menu_item_ext_ref"),
        F.col("pop").alias("pop"),
        F.col("pos_code").alias("pos_code"),
        _num("menu_price", "DOUBLE").alias("menu_price"),
        _num("item_discount", "DOUBLE").alias("source_item_discount"),
        _num("item_voided", "DOUBLE").alias("item_voided"),
        F.col("report_group").alias("report_group"),
        F.col("tender_type").alias("tender_type"),
        _num("total_rounding", "DOUBLE").alias("total_rounding"),
        F.col("source_file"),
        F.col("ingested_at"),
    ).filter(F.col("txn_date").isNull() | (F.col("txn_date") >= F.to_date(F.lit(SILVER_START_DATE))))

    # 2) One row per unit (current structure). item_discount is a LINE total: split it across units.
    expandable = (F.col("source_quantity") > 1) & (F.col("source_quantity") <= MAX_EXPLODE_QTY)
    units = (
        typed.withColumn(
            "unit_index",
            F.explode(
                F.when(expandable, F.sequence(F.lit(1), F.col("source_quantity"))).otherwise(F.array(F.lit(1)))
            ),
        )
        .withColumn("is_component_line", F.coalesce(F.col("source_quantity") == 0, F.lit(False)))
        .withColumn(
            "quantity",
            F.when(expandable, F.lit(1))
            .when(F.col("is_component_line"), F.lit(1))      
            .otherwise(F.col("source_quantity")),
        )
        .withColumn("discount_amount", F.col("source_item_discount") / F.when(expandable, F.col("source_quantity")).otherwise(F.lit(1)))
        .drop("source_item_discount")
    )

    # 3) Measures, regime, channel and business flags.
    enriched = (
        units.withColumn(
            "gross_amount",
            F.when(F.col("is_component_line"), F.lit(0.0)).otherwise(F.col("menu_price") * F.col("quantity")),
        )
        .withColumn("order_voided_flag", F.col("order_voided") > 0)
        .withColumn("item_voided_flag", F.col("item_voided") > 0)
        .withColumn(
            "pos_regime",
            F.when(pop.isin(*LEGACY_POPS), "legacy").when(pop.isNull(), "unknown").otherwise("current"),
        )
        .withColumn(
            "channel_group",
            F.when(pop == "Drive Thru", "Drive Thru")
            .when(pop == "Dine-In", "Dine-In")
            .when(pop == "Carry Out", "Carry Out")
            .when(pop.isin("Online", "Mobile"), "Online (Own)")     
            .when(pop.startswith("Online: "), "Third-Party Delivery")
            .when(pop.isin(*LEGACY_POPS), "Legacy (sin mapeo)")
            .when(pop.isNull(), "Unknown")
            .otherwise("Unmapped"),
        )
        .withColumn("hour_24", _hour_24())
        .withColumn(
            "is_coupon_line",
            F.coalesce((F.col("menu_price") < 0) & F.col("pos_code").rlike(_DISCOUNT_CODE), F.lit(False)),
        )
        .withColumn(
            "promo_type",
            F.when(
                F.col("is_coupon_line"),
                F.when(F.col("pos_code").rlike(r"(?i)bogo"), "Bogo")
                .when(F.col("pos_code").rlike(r"(?i)orderreduction"), "OrderReduction")
                .otherwise("Coupon"),
            ),
        )
        .withColumn(
            "promo_code",
            F.when(
                F.col("is_coupon_line"),
                F.when(
                    F.regexp_extract("pos_code", r"(?i)^(.*)\((?:coupon|orderreduction|bogo)\)\s*$", 1) != "",
                    F.regexp_extract("pos_code", r"(?i)^(.*)\((?:coupon|orderreduction|bogo)\)\s*$", 1),
                )
                .when(
                    F.regexp_extract("pos_code", r"(?i)^(?:coupon|orderreduction|bogo)-(.*)$", 1) != "",
                    F.regexp_extract("pos_code", r"(?i)^(?:coupon|orderreduction|bogo)-(.*)$", 1),
                )
                .otherwise(F.col("pos_code")),
            ),
        )
        .withColumn(
            "is_addon_line",
            F.coalesce(F.col("pos_code").rlike(_ADDON_CODE) | F.col("menu_item").rlike(r"(?i)^\s*extra "), F.lit(False)),
        )
        .withColumn("is_donation", F.coalesce(F.col("menu_item").rlike(r"(?i)^\s*donation"), F.lit(False)))
        .withColumn(
            "is_void_line",
            F.coalesce(F.col("order_voided_flag"), F.lit(False))
            | F.coalesce(F.col("item_voided_flag"), F.lit(False))
            | F.coalesce(F.col("transaction_type") == "Void", F.lit(False)),
        )
        .withColumn("is_bulk_line", F.coalesce(F.col("source_quantity") > MAX_EXPLODE_QTY, F.lit(False)))
        .withColumn(
            "is_sale_line",
            F.coalesce(F.col("menu_price") > 0, F.lit(False))
            & ~F.col("is_coupon_line")
            & ~F.col("is_void_line")
            & ~F.col("is_donation")
            & ~F.col("is_component_line"),
        )
        .withColumn(
            "check_total",
            F.when(
                F.col("pos_regime") == "legacy",
                F.col("total_raw") - F.coalesce(F.col("check_discount"), F.lit(0.0)) + F.coalesce(F.col("tax"), F.lit(0.0)),
            ).otherwise(F.col("total_raw")),
        )
        .drop("total_raw", "order_voided", "item_voided")
    )

    # 4) Stream-static join: backfill report_group where the source file predates the column.
    return (
        enriched.join(
            F.broadcast(rg_map),
            F.col("menu_item") == F.col("map_menu_item"),
            "left",
        )
        .withColumn(
            "report_group",
            F.coalesce(F.col("report_group"), F.col("mapped_report_group")),
        )
        .drop("map_menu_item", "mapped_report_group")
        .withColumn(
            "discount_rate",
            F.when(F.col("gross_amount") > 0, F.col("discount_amount") / F.col("gross_amount")),
        )
        .withColumn("month_year", F.date_format(F.col("txn_date"), "yyyy-MM"))
        .withColumn("uploaded_on", F.current_timestamp())
    )


@dp.table(
    name="tacobueno.discounting_silver.tb_transactions_clean",
    comment=(
        "Typed, cleaned line-level Taco Bueno transactions from 2025-01-01 onwards, normalized to the "
        "current POS structure (one row per unit, current channel taxonomy, check_total restated as "
        "gross - discount + tax), with business flags (coupon, donation, void, sale line, bulk) and "
        "all project data-quality expectations."
    ),
    cluster_by=["txn_date", "store_id"],
)
@dp.expect_all_or_drop(HARD_RULES)
@dp.expect_all(SOFT_RULES)
def tb_transactions_clean():
    bronze = spark.readStream.table("tacobueno.discounting_bronze.tb_transactions_raw")
    return transform(bronze, report_group_map())