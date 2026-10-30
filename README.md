# Databricks-x-Sun-Holdings-ctrl-z

A data pipeline with a **medallion** architecture (bronze → silver → gold) that ingests Taco Bueno's daily transactions from POS CSV files, normalizes them to the current POS format, and publishes them into a **star schema** plus analytical views for discounts and promotions.

- **Pipeline**: `Taco Bueno Descuentos - Medallion`
- **Catalog**: `tacobueno`
- **Output schemas**: `discounting_bronze` (ingestion), `discounting_silver` (cleaning), `discounting_dim` (star schema), `discounting_gold` (analytics)
- **Compute**: serverless + Photon, `current` channel

## Bronze — `tb_transactions_raw`

**Tabla**: `tacobueno.discounting_bronze.tb_transactions_raw` (streaming table)

Ingesta *raw* de los CSV transaccionales diarios (`TB_Daily_TLD_MM-DD-YYYY-m.csv`) con Auto Loader (`cloudFiles`):

- **Evolución de esquema controlada por nombre**: el POS tuvo 3 versiones de archivo (19 columnas hasta 2025-06-28, 21 desde 2025-06-29, 22 desde ~2026-07-01). Se declara el esquema de 22 columnas con los nombres crudos del header y `enforceSchema: false` + `rescuedDataColumn`, de modo que las versiones se alinean solas: la columna ausente en un archivo se lee como NULL y las inesperadas caen en `_rescued_data`.
- Todo como `STRING` (fidelidad raw); el mapeo a `snake_case` (`Store #` → `store_id`, etc.) ocurre en la proyección porque Delta no acepta los nombres crudos.
- Metadatos de linaje: `source_file` (`_metadata.file_path`) e `ingested_at`.
- Los 24 archivos vacíos (solo header) se ingieren sin error (0 filas).

## Silver: `tb_transactions_clean`

**Table**: `tacobueno.discounting_silver.tb_transactions_clean`

Types, cleans, and **harmonizes** transactions to the current POS structure (the migration happened between 2025-11 and 2026-01; the structure from 2026-02 onward is the one that remains):

- **Grain: one row per unit sold** (`quantity = 1`). Legacy lines with `quantity = 0` (combo components and modifiers, ~19% of legacy lines) are converted into 1-unit, no-charge rows and flagged `is_component_line`; lines with quantity 2–50 are exploded into one row per unit, with the line discount split evenly; more than 50 units are bulk orders (`is_bulk_line`, kept as a single row).
- **Totals restatement**: the legacy `Total` (excluding tax and discount) is re-expressed as `check_total = gross − discount + tax`. The original values remain traceable via `pos_regime` (`legacy` / `current`).
- **Channels**: current taxonomy of Drive Thru / Dine-In / Carry Out / Online / Mobile. Legacy codes (`To Go`, `Inside`, `3`) have no reliable mapping → `Legacy (sin mapeo)` (i.e., "Legacy (unmapped)").
- **Business flags** (computed here and reused in gold): `is_coupon_line`, `is_donation`, `is_void_line`, `is_component_line`, `is_addon_line`, `is_sale_line`, `is_bulk_line`, plus `hour_24`, `channel_group`, `pos_regime`, `promo_type`, and `promo_code`.
- **Data quality**: ALL of the project's expectations live here. Hard rules (drop): date, store, and check number present. Soft rules (warn, rows are kept): ~15 rules (future dates, negative discounts, voids with value, etc.), because the anomalies are informative (e.g., coupon lines carry a negative price on purpose).
- Redundant raw columns are not carried over (`total`, `order_voided`, `item_voided`, `item_discount`).

## Gold: Star schema (`discounting_dim`)

Defined in `gold_star_schema.py`. It reads only the harmonized silver table; fact and dimension keys are built with the **same helpers**, so referential integrity holds by construction (nulls map to the `Unknown` member).

**Dimensions**:

| Table | Key | Contents |
| --- | --- | --- |
| `dim_date` | `date_key` (yyyymmdd) | Calendar from 2025-01-01 → +12 months: year, quarter, month (EN/ES), third of month, ISO weekday, `is_weekend` |
| `dim_month` | `month_year` (YYYY-MM) | Monthly aggregate derived from `dim_date` |
| `dim_store` | `store_key` | POS stores (`-1` = Unknown) |
| `dim_channel` | `channel_key` | Order channel (`pop`) and its group, classified in silver |
| `dim_item` | `item_key` | Grain: `pos_code`; includes `is_coupon`, `is_donation`, `is_addon`, `promo_type`, `promo_code` |
| `dim_transaction_type` | hash | `Regular Sale` / `Discount` / `Void` with flags `is_discount`, `is_void` |

**Facts** (materialized views, deliberately narrow, with no expectations to avoid a full refresh of ~90M rows on every run):

- `fact_transaction_line`: grain is one row per unit sold; FKs to the 6 dimensions; keeps voided lines with a flag.
- `fact_check`: grain is one check (order); check-level measures: `net_sales` (pre-tax, after discounts, excluding donations and voids), `discount_total` (check discount + discount lines), `promo_program` (`FREE`, `P10`, `P20`, `P50`, `COUPON_LINE`, `PARTIAL_OR_OTHER`, `NONE`), `priced_units`, `donation_amount`, `pos_regime`.

## Gold: Analytical views (`discounting_gold`)

| Table | What it answers |
| --- | --- |
| `tb_monthly_sales_summary` | Monthly summary comparable across POS regimes: checks, `net_sales`, AOV, units/check, % of checks with a promo and with FREE, discount cost, and `pct_current_regime` (how much of the month already runs on the current POS) |
| `tb_promo_program_summary` | Where the discount money goes: checks, % of checks, net sales, average ticket, and cost per promotion program, split legacy/current |
| `tb_promo_catalog` | Catalog of promotions from the current POS's **discount lines** (`Coupon`, `OrderReduction`, `Bogo` with their code): first/last day, active days, uses, checks, stores, and amount. Check-level discounts (10/20/50%/FREE) have no identifier and do not appear here |
| `tb_item_discount_summary` | Item-level discounts from the **legacy** POS only (in the current one, the discount lives at check level or as a line): units, gross sales, discount dollars, and rate by menu item and report group |
| `tb_store_ranking` | Store ranking: checks, active days, net sales, AOV, discount cost, % of promo checks, and free checks per 1,000 (current regime only, since its level changed with the migration) |

> Note: `tb_item_discount_summary` and `tb_promo_catalog` are complementary: they cover the two POS eras (legacy item-level vs. current discount lines).

## Deployment and execution (DAB)

```bash
databricks bundle validate --target dev     
databricks bundle deploy --target dev       
databricks bundle run taco_bueno_descuentos_medallion  
```

A run recalculates all layers in dependency order: bronze → silver → star schema → analytical views.