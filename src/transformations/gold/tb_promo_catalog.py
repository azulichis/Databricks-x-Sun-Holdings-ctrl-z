"""Gold layer: promotion catalog from the discount lines of the current POS structure.

Each discount line (negative price) carries its mechanism and an identifier in the pos_code:
'001095(OrderReduction)', '001018(Coupon)', and from 2026-07-19 'Coupon-640427727', 'Bogo-640432130',
'OrderReduction-640428058'. One row per (promo_type, promo_code) with its dates, duration, usage and amount.
Check-level discounts (the 10% / 20% / 50% / free programs) have no identifier and are not in this table.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

STAR = "tacobueno.discounting_dim"


@dp.materialized_view(
    name="tacobueno.discounting_gold.tb_promo_catalog",
    comment=(
        "Promotion catalog from discount lines: type, code, first and last day, active days, span, uses, "
        "checks, stores and discount amount."
    ),
)
def tb_promo_catalog():
    lines = spark.read.table(f"{STAR}.fact_transaction_line").where(F.col("is_coupon_line") & ~F.col("is_voided"))
    items = spark.read.table(f"{STAR}.dim_item").select("item_key", "promo_type", "promo_code", "pos_code")
    days = spark.read.table(f"{STAR}.dim_date").select("date_key", "full_date")
    return (
        lines.join(items, "item_key")
        .join(days, "date_key")
        .groupBy("promo_type", "promo_code")
        .agg(
            F.max("pos_code").alias("pos_code"),
            F.min("full_date").alias("first_day"),
            F.max("full_date").alias("last_day"),
            F.countDistinct("full_date").alias("active_days"),
            F.count("*").alias("uses"),
            F.countDistinct("check_key").alias("checks"),
            F.countDistinct("store_key").alias("stores"),
            F.sum("coupon_amount").alias("amount_total"),
            F.avg("coupon_amount").alias("avg_amount"),
        )
        .withColumn("span_days", F.datediff(F.col("last_day"), F.col("first_day")) + 1)
    )
