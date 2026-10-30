"""Gold layer: item-level discounts of the LEGACY POS structure, by menu item and report group.

Item-level discounts only exist in the legacy structure (before the 2025-11..2026-01 migration). In the
current structure discounts live at the check level or as discount lines: see tb_promo_program_summary
and tb_promo_catalog. Units are units sold (is_sale_line); the discount is the line discount split per unit.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

STAR = "tacobueno.discounting_dim"


@dp.materialized_view(
    name="tacobueno.discounting_gold.tb_item_discount_summary",
    comment=(
        "Legacy POS only: units, gross sales, item-level discount dollars, discount rate and discounted units "
        "by menu item and report group."
    ),
    cluster_by=["report_group", "menu_item"],
)
def tb_item_discount_summary():
    lines = (
        spark.read.table(f"{STAR}.fact_transaction_line")
        .where((F.col("pos_regime") == "legacy") & F.col("is_sale_line") & ~F.col("is_voided"))
    )
    items = spark.read.table(f"{STAR}.dim_item").select("item_key", "menu_item", "report_group")
    return (
        lines.join(items, "item_key")
        .groupBy("report_group", "menu_item")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount").alias("gross_sales"),
            F.sum("discount_amount").alias("discount_amount"),
            F.sum(F.when(F.col("discount_amount") > 0, F.col("quantity"))).alias("discounted_units"),
        )
        .withColumn("net_sales", F.col("gross_sales") - F.col("discount_amount"))
        .withColumn(
            "discount_rate",
            F.when(F.col("gross_sales") > 0, F.col("discount_amount") / F.col("gross_sales")),
        )
        .filter(F.col("gross_sales").isNotNull())
    )
