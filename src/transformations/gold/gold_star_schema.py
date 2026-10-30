"""
Gold layer - star schema (dimensions and facts).

Reads the HARMONIZED silver table (tb_transactions_clean): one row per unit, current channel
taxonomy, business flags and the Total restated, so this file only models (keys, grains,
aggregations). All data-quality expectations live in silver; nothing is validated here.

Referential integrity holds by construction: every key in a fact is built with the same helper
as the matching dimension, from the same silver table, and nulls map to an explicit "Unknown"
member. The facts carry no expectations on purpose: a materialized view with expectations is
always fully refreshed, which would recompute ~90M rows on every run.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "tacobueno"
STAR_SCHEMA = "discounting_dim"           
SILVER = "tacobueno.discounting_silver.tb_transactions_clean"
CALENDAR_START = "2025-01-01"            
FUTURE_MONTHS = 12                       

MONTHS_ES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
             "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


def _fq(table: str) -> str:
    return f"{CATALOG}.{STAR_SCHEMA}.{table}"


# Key expressions 
# Dimensions and facts both build their keys from these helpers, so the two sides cannot drift apart. 
def date_key():
    return F.date_format("txn_date", "yyyyMMdd").cast("int")


def store_key():
    return F.coalesce(F.col("store_id"), F.lit(-1))


def item_key():
    return F.xxhash64(F.coalesce(F.col("pos_code"), F.lit("Unknown")))


def channel_key():
    return F.xxhash64(F.coalesce(F.col("pop"), F.lit("Unknown")))


def txn_type_key():
    return F.xxhash64(F.coalesce(F.col("transaction_type"), F.lit("Unknown")))


def check_key():
    return F.xxhash64(store_key(), F.col("txn_date"), F.col("check_number"))


def _flag(col):
    """Boolean column -> 1/0 integer for MAX aggregations."""
    return F.col(col).cast("int")


# DIMENSIONS 

# Stateless + idempotent: the full range is recomputed on every run, so the calendar grows by itself and can never have gaps.
@dp.materialized_view(
    name=_fq("dim_date"),
    comment="Calendar dimension, one row per day, from CALENDAR_START to the end "
            "of the month FUTURE_MONTHS ahead of today. PK: date_key (yyyymmdd).",
)
def dim_date():
    days = spark.sql(f"""
        SELECT explode(sequence(
            DATE'{CALENDAR_START}',
            last_day(add_months(current_date(), {FUTURE_MONTHS})),
            INTERVAL 1 DAY)) AS full_date
    """)
    es = F.array(*[F.lit(m) for m in MONTHS_ES])
    tercio = (F.floor((F.month("full_date") - 1) / 4) + 1).cast("int")
    return days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.concat(F.lit("Q"), F.quarter("full_date")).alias("quarter_name"),
        F.month("full_date").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.date_format("full_date", "MMM").alias("month_name_short"),
        F.element_at(es, F.month("full_date")).alias("month_name_es"),
        F.date_format("full_date", "yyyy-MM").alias("month_year"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.expr("weekday(full_date) + 1").alias("day_of_week_iso"), 
        F.date_format("full_date", "EEEE").alias("day_of_week_name"),
        F.date_format("full_date", "E").alias("day_of_week_short"),
        F.dayofyear("full_date").alias("day_of_year"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        tercio.alias("tercio"),
        F.concat(F.lit("T"), tercio).alias("tercio_name"),
    )


@dp.materialized_view(
    name=_fq("dim_month"),
    comment="Month dimension derived from dim_date. PK: month_year (YYYY-MM).",
)
def dim_month():
    return (
        spark.read.table(_fq("dim_date"))
        .groupBy("month_year", "year", "month_number", "month_name",
                 "month_name_short", "month_name_es", "quarter",
                 "quarter_name", "tercio", "tercio_name")
        .agg(F.count("*").alias("days_in_month"))
    )


@dp.materialized_view(
    name=_fq("dim_store"),
    comment="Store dimension. PK: store_key (= POS store id). -1 = Unknown.",
)
def dim_store():
    stores = (
        spark.read.table(SILVER)
        .where(F.col("store_id").isNotNull())
        .select(store_key().alias("store_key"))
        .distinct()
        .withColumn("store_name", F.concat(F.lit("Store #"), F.col("store_key")))
    )
    unknown = spark.createDataFrame([(-1, "Unknown")], "store_key INT, store_name STRING")
    return stores.unionByName(unknown)


# `pop` describes the ORDER, not the item. The channel_group classification comes from silver.
@dp.materialized_view(
    name=_fq("dim_channel"),
    comment="Order channel (point of purchase) and its group, classified in silver. "
            "PK: channel_key (hash of pop; NULL pop = 'Unknown').",
)
def dim_channel():
    return (
        spark.read.table(SILVER)
        .select(F.coalesce(F.col("pop"), F.lit("Unknown")).alias("pop"), "channel_group")
        .distinct()
        .withColumn("channel_key", F.xxhash64("pop"))
        .select("channel_key", "pop", "channel_group")
    )


# Grain: one row per POS code.
@dp.materialized_view(
    name=_fq("dim_item"),
    comment="Menu item dimension. Grain: pos_code. PK: item_key (hash of pos_code). "
            "is_coupon (any discount line: Coupon, OrderReduction, Bogo), is_donation, is_addon, promo_type and promo_code come from silver.",
)
def dim_item():
    s = spark.read.table(SILVER).withColumn(
        "pos_code", F.coalesce(F.col("pos_code"), F.lit("Unknown"))
    )
    return (
        s.groupBy("pos_code")
        .agg(
            F.max("menu_item").alias("menu_item"),
            F.max("report_group").alias("report_group"),
            F.countDistinct("menu_item").alias("n_names_seen"),  
            (F.max(_flag("is_coupon_line")) == 1).alias("is_coupon"),
            (F.max(_flag("is_donation")) == 1).alias("is_donation"),
            (F.max(_flag("is_addon_line")) == 1).alias("is_addon"),
            F.max("promo_type").alias("promo_type"),       
            F.max("promo_code").alias("promo_code"),   
        )
        .withColumn("item_key", F.xxhash64("pos_code"))
        .select("item_key", "pos_code", "menu_item", "report_group", "n_names_seen",
                "is_coupon", "is_donation", "is_addon", "promo_type", "promo_code")
    )


# POS labels seen in silver: 'Regular Sale', 'Discount', 'Void'. Anything new falls into 'Other' so it is visible instead of silently ignored.
@dp.materialized_view(
    name=_fq("dim_transaction_type"),
    comment="POS transaction type with grouping flags (is_discount, is_void). "
            "PK: transaction_type_key (hash of the label).",
)
def dim_transaction_type():
    t = F.col("transaction_type")
    return (
        spark.read.table(SILVER)
        .select(F.coalesce(F.col("transaction_type"), F.lit("Unknown")).alias("transaction_type"))
        .distinct()
        .withColumn("transaction_type_key", F.xxhash64("transaction_type"))
        .withColumn(
            "transaction_group",
            F.when(t == "Regular Sale", "Sale")
             .when(t == "Discount", "Discount")
             .when(t == "Void", "Void")
             .otherwise("Other"),
        )
        .withColumn("is_discount", t == "Discount")
        .withColumn("is_void", t == "Void")
        .select("transaction_type_key", "transaction_type", "transaction_group",
                "is_discount", "is_void")
    )


# FACTS 
# Grain: one row per unit sold. Narrow on purpose so the Power BI model stays small. Voided lines are KEPT with a flag.
@dp.materialized_view(
    name=_fq("fact_transaction_line"),
    comment="Fact, grain = one row per unit sold (bulk orders above the silver threshold keep their "
            "quantity, is_bulk_line). FKs: date_key, store_key, item_key, channel_key, "
            "transaction_type_key, check_key (-> fact_check). Units sold = SUM(quantity) WHERE "
            "is_sale_line (price > 0, no coupon, void or donation). Check-level amounts (total, "
            "total_discount) live in fact_check and must not be summed here.",
    cluster_by=["date_key", "store_key"],
)
def fact_transaction_line():
    return spark.read.table(SILVER).select(
        date_key().alias("date_key"),
        store_key().alias("store_key"),
        item_key().alias("item_key"),
        channel_key().alias("channel_key"),
        txn_type_key().alias("transaction_type_key"),
        check_key().alias("check_key"),
        F.col("check_number"),                      
        F.col("quantity"),
        F.col("menu_price"),                         
        F.col("gross_amount"),                      
        F.col("discount_amount"),                    
        (F.col("gross_amount") - F.col("discount_amount")).alias("net_amount"),
        F.col("discount_rate"),
        F.col("is_void_line").alias("is_voided"),
        F.col("is_sale_line"),
        F.col("is_coupon_line"),
        F.col("is_donation"),
        F.col("is_bulk_line"),
        F.col("is_component_line"),                  
        F.col("is_addon_line"),                      
        F.when(F.col("is_coupon_line"), -F.col("gross_amount")).alias("coupon_amount"),  
        F.col("hour_24"),
        F.col("pos_regime"),
    )


# Grain: one row per check. Check-level fields repeat on every line in the source, so they are collapsed with MAX, never summed. AOV, coupon share and basket size are computed from this table.
@dp.materialized_view(
    name=_fq("fact_check"),
    comment="Fact, grain = check (store + date + check number). PK: check_key. "
            "FKs: date_key, store_key, channel_key. hour_24 = hour of day (0-23). "
            "Use net_sales for AOV: pre-tax, after discounts, without donations, comparable across "
            "POS regimes. promo_program buckets the discount.",
    cluster_by=["date_key", "store_key"],
)
def fact_check():
    s = spark.read.table(SILVER)
    sellable = ~F.col("is_coupon_line") & ~F.col("is_donation")
    agg = (
        s.groupBy("store_id", "txn_date", "check_number")
        .agg(
            F.max("check_total").alias("check_total"),
            F.min("check_total").alias("_total_min"),
            F.max("check_discount").alias("check_discount"),
            F.max("tax").alias("tax"),
            F.max("total_rounding").alias("total_rounding"),
            F.sum(F.when(sellable, F.col("quantity"))).alias("items_qty"),            
            F.sum(F.when(F.col("is_sale_line"), F.col("quantity"))).alias("priced_units"),
            F.sum(F.when(sellable, F.lit(1)).otherwise(F.lit(0))).alias("line_count"),
            F.sum(F.when(~F.col("is_void_line"), F.col("gross_amount"))).alias("gross_amount"),  
            F.sum(F.when(F.col("is_donation") & ~F.col("is_void_line"), F.col("gross_amount"))).alias("donation_amount"),
            F.sum("discount_amount").alias("item_discount"),
            F.sum(F.when(F.col("is_coupon_line"), -F.col("gross_amount"))).alias("coupon_amount"),
            F.max(_flag("is_coupon_line")).alias("_coupon_line"),
            F.max(_flag("is_void_line")).alias("_any_void"),
            F.max((F.col("transaction_type") == "Discount").cast("int")).alias("_disc_line"),
            F.max((F.col("transaction_type") == "Void").cast("int")).alias("_void_line"),
            F.max("pop").alias("pop"),
            F.max("pos_regime").alias("pos_regime"),
            F.max("tender_type").alias("tender_type"),
            F.max("begin_time").alias("begin_time"),
            F.max("end_time").alias("end_time"),
            F.max("hour_24").alias("hour_24"),
        )
    )

    # Merchandise gross = valid lines minus donations, which are pass-through money.
    disc = F.coalesce(F.col("check_discount"), F.lit(0.0))
    merch_gross = F.col("gross_amount") - F.coalesce(F.col("donation_amount"), F.lit(0.0))
    rate = F.when(merch_gross > 0, disc / merch_gross)
    discount_total = disc + F.coalesce(F.col("coupon_amount"), F.lit(0.0))
    has_disc = disc > 0
    coupon = F.coalesce(F.col("_coupon_line") == 1, F.lit(False))
    promo = has_disc | (F.coalesce(F.col("item_discount"), F.lit(0.0)) > 0) | coupon
    # Anchors seen in both POS regimes: 10%, 20%, 50% and 100% (free). 
    program = (
        F.when(has_disc & (rate >= 0.99), "FREE")
         .when(has_disc & (F.abs(rate - 0.10) <= 0.01), "P10")
         .when(has_disc & (F.abs(rate - 0.20) <= 0.01), "P20")
         .when(has_disc & (F.abs(rate - 0.50) <= 0.01), "P50")
         .when(coupon, "COUPON_LINE")
         .when(promo, "PARTIAL_OR_OTHER")
         .otherwise("NONE")
    )

    return agg.select(
        check_key().alias("check_key"),
        date_key().alias("date_key"),
        store_key().alias("store_key"),
        channel_key().alias("channel_key"),
        F.col("check_number"),
        "check_total", "check_discount", "tax", "total_rounding",
        F.col("pos_regime"),
        F.col("gross_amount"),                       
        F.col("donation_amount"),
        (merch_gross - disc).alias("net_sales"),
        rate.alias("discount_rate"),
        program.alias("promo_program"),
        "items_qty", "priced_units", "line_count", "item_discount", "coupon_amount",
        discount_total.alias("discount_total"),      
        F.coalesce(F.col("_total_min") != F.col("check_total"), F.lit(False)).alias("has_inconsistent_total"),
        (disc > 0).alias("has_order_discount"),
        (F.coalesce(F.col("item_discount"), F.lit(0.0)) > 0).alias("has_item_discount"),
        promo.alias("is_promo_check"),
        coupon.alias("has_coupon_line"),
        F.coalesce(F.col("_disc_line") == 1, F.lit(False)).alias("has_discount_line"),
        F.coalesce(F.col("_any_void") == 1, F.lit(False)).alias("has_void"),
        F.coalesce(F.col("_void_line") == 1, F.lit(False)).alias("has_void_line"),
        "tender_type", "begin_time", "end_time", "hour_24",
    )