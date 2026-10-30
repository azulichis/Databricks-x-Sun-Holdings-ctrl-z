"""Gold layer: where the discount money goes, by POS structure and promotion program.

Replaces tb_promo_period_comparison, which compared "with promotions" (2025) against "without
promotions" (2026). That contrast measured the POS change (item-level discounts stopped being recorded),
not the promotions: check-level discounts kept running at 6-8% of checks.

Programs come from fact_check.promo_program: FREE (100% off), P10, P20, P50, COUPON_LINE (a discount
line without a check-level anchor), PARTIAL_OR_OTHER and NONE. Cost is at list price (discount_total).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

STAR = "tacobueno.discounting_dim"


@dp.materialized_view(
    name="tacobueno.discounting_gold.tb_promo_program_summary",
    comment=(
        "Checks, share of checks, net sales, average ticket, units, discount cost and share of the discount "
        "cost by promotion program, split by POS structure (legacy / current)."
    ),
)
def tb_promo_program_summary():
    base = (
        spark.read.table(f"{STAR}.fact_check")
        .groupBy("pos_regime", "promo_program")
        .agg(
            F.count("*").alias("checks"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("priced_units").alias("priced_units"),
            F.sum("discount_total").alias("discount_total"),
        )
    )
    by_regime = Window.partitionBy("pos_regime")
    return (
        base.withColumn("pct_checks", F.col("checks") / F.sum("checks").over(by_regime))
        .withColumn("pct_of_discount_cost", F.col("discount_total") / F.sum("discount_total").over(by_regime))
        .withColumn("avg_net_sales", F.col("net_sales") / F.col("checks"))
        .withColumn("avg_priced_units", F.col("priced_units") / F.col("checks"))
        .withColumn("avg_discount_per_check", F.col("discount_total") / F.col("checks"))
    )
