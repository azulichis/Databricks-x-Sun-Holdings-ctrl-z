"""Gold layer: monthly sales and discount summary, built on the star schema.

Everything comes from fact_check, so the measures are comparable across the POS migration
(2025-11 .. 2026-01): net_sales is pre-tax, after discounts, without donations or voided lines, and
discount_total adds the check-level discount and the discount lines (Coupon / OrderReduction / Bogo).
pct_current_regime shows how much of each month already runs on the current POS structure.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

STAR = "tacobueno.discounting_dim"


@dp.materialized_view(
    name="tacobueno.discounting_gold.tb_monthly_sales_summary",
    comment=(
        "Monthly checks, net sales, AOV, units per check, promo and free-check shares, discount cost and "
        "share of checks on the current POS structure. Comparable across the POS migration."
    ),
)
def tb_monthly_sales_summary():
    checks = spark.read.table(f"{STAR}.fact_check")
    months = spark.read.table(f"{STAR}.dim_date").select("date_key", "month_year")
    free = F.col("promo_program") == "FREE"
    return (
        checks.join(months, "date_key")
        .groupBy("month_year")
        .agg(
            F.count("*").alias("checks"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("priced_units").alias("priced_units"),
            F.sum("donation_amount").alias("donation_amount"),
            F.sum(F.col("is_promo_check").cast("int")).alias("promo_checks"),
            F.sum(free.cast("int")).alias("free_checks"),
            F.sum("discount_total").alias("discount_total"),
            F.sum(F.when(free, F.col("discount_total"))).alias("discount_free_checks"),
            F.avg((F.col("pos_regime") == "current").cast("int")).alias("pct_current_regime"),
        )
        .withColumn("aov", F.col("net_sales") / F.col("checks"))
        .withColumn("units_per_check", F.col("priced_units") / F.col("checks"))
        .withColumn("pct_promo_checks", F.col("promo_checks") / F.col("checks"))
        .withColumn("pct_free_checks", F.col("free_checks") / F.col("checks"))
        .withColumn(
            "discount_pct_of_sales",
            F.col("discount_total") / (F.col("net_sales") + F.col("discount_total")),
        )
        .orderBy("month_year")
    )