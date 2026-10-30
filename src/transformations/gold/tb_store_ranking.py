"""Gold layer: store ranking by net sales, discounting and free checks.

Net sales are comparable across the POS migration. The free-check rate is shown for the current POS
structure only (free_per_1000_current), because its level changed with the migration and stores moved
at different dates; current_checks tells how much data each rate rests on.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

STAR = "tacobueno.discounting_dim"


@dp.materialized_view(
    name="tacobueno.discounting_gold.tb_store_ranking",
    comment=(
        "Store ranking by net sales with checks, AOV, discount cost, promo share and free checks per 1,000 "
        "checks on the current POS structure."
    ),
)
def tb_store_ranking():
    checks = spark.read.table(f"{STAR}.fact_check")
    stores = spark.read.table(f"{STAR}.dim_store")
    free = F.col("promo_program") == "FREE"
    current = F.col("pos_regime") == "current"
    agg = (
        checks.groupBy("store_key")
        .agg(
            F.count("*").alias("checks"),
            F.countDistinct("date_key").alias("active_days"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("discount_total").alias("discount_total"),
            F.sum(F.col("is_promo_check").cast("int")).alias("promo_checks"),
            F.sum((free & current).cast("int")).alias("free_checks_current"),
            F.sum(current.cast("int")).alias("current_checks"),
            F.sum(F.when(free & current, F.col("discount_total"))).alias("free_discount_current"),
        )
        .withColumn("aov", F.col("net_sales") / F.col("checks"))
        .withColumn("discount_pct_of_sales", F.col("discount_total") / (F.col("net_sales") + F.col("discount_total")))
        .withColumn("pct_promo_checks", F.col("promo_checks") / F.col("checks"))
        .withColumn(
            "free_per_1000_current",
            F.when(F.col("current_checks") > 0, 1000.0 * F.col("free_checks_current") / F.col("current_checks")),
        )
    )
    return (
        agg.join(stores, "store_key", "left")
        .withColumn("net_sales_rank", F.rank().over(Window.orderBy(F.col("net_sales").desc())))
        .withColumn("free_rate_rank", F.rank().over(Window.orderBy(F.col("free_per_1000_current").desc_nulls_last())))
    )
