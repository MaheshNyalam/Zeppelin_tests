%pyspark

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql import Row
import datetime
from datetime import date

#from fin_utils import *

%pyspark

# Read sales details (source) table name
sales_details_db_name = "dds_prod"
sales_details_table_name = "dds_prod.invoice_cshdet"

# Read sales agg table storage location (target)
sales_agg_sales_transaction_daily_s3path = "s3://aap-dpdm-prod/dpdm.db/agg_sales_transactions_daily"

# Read sales agg table storage location (target)
sales_agg_sales_store_sku_daily_db_name = "dpdm_prod"
sales_agg_sales_store_sku_daily_table_name = "dpdm_prod.agg_sales_store_sku_daily"

sales_agg_sales_store_sku_daily_s3path = "s3://aap-dpdm-prod/dpdm.db/agg_sales_store_sku_daily"

sku_rolling_sales_db_name = "dpdm_prod"
sku_rolling_sales_table_name = "dpdm_prod.sku_rolling_sales"

sales_agg_sales_by_sku_s3path = "s3://aap-dpdm-prod/dpdm.db/sku_rolling_sales"

# Read store_master (master table) table
store_master_db_name = "dds_prod"
store_master_table_name = "dds_prod.store_master"

# Read sku_master (master table) table
sku_master_db_name = "dds_prod"
sku_master_table_name = "dds_prod.sku_master"

# Read calendar hierarchy table
cal_hierarchy_db_name = "dds_prod"
cal_hierarchy_table_name = "dds_prod.cal_hierarchy"

# Read cluster_definition (master) table
clus_def_db_name = "dds_prod"
clus_def_table_name = "dds_prod.cluster_definition"

# Read sku_base_product_grp (master) table
sku_prodgrp_db_name = "dpdm_prod"
sku_prodgrp_table_name = "dpdm_prod.sku_base_product_group"

# Read sku_data (master) table
sku_data_db_name = "dpdm_prod"
sku_data_table_name = "dpdm_prod.sku_data"

# Read sku_part_type (master) table
sku_part_type_db_name ="dds_prod"
sku_part_type_table_name = "dds_prod.sku_part_type"

# Read agg sales storage location (target)
agg_sales_transactions_daily_db_name = "dpdm_prod"
agg_sales_transactions_daily_table_name = "dpdm_prod.agg_sales_transactions_daily"

# Read store sku rolling sales (source) table name
store_sku_rolling_sales_db_name = "dpdm_prod"
store_sku_rolling_sales_table_name = "dpdm_prod.store_sku_rolling_sales"

store_sku_rolling_sales_s3path = "s3://aap-dpdm-prod/dpdm.db/cq_store_sku_rolling_sales"

store_sales_by_period_db_name = "dpdm_prod"
store_sales_by_period_table_name = "dpdm_prod.store_sales_by_period"

store_sales_by_period_s3path = "s3://aap-dpdm-prod/dpdm.db/store_sales_by_period"

daily_sales_store_sku = "dds_prod.daily_sales"

ad_promo_monthly_sales = "dpdm_prod.ad_promo_monthly_sales_by_bpg"

ad_promo_monthly_sales_by_store_sku = "dpdm_prod.ad_promo_sales_by_str_sku"

agg_sales_ad_promo_monthly_sku_s3path = "s3://aap-dpdm-prod/dpdm.db/ad_promo_monthly_sales_merch"
agg_sales_ad_promo_monthly_store_sku_s3path = "s3://aap-dpdm-prod/dpdm.db/ad_promo_monthly_sales_by_store_sku"

agg_sales_daily_store_sku_s3path = "s3://aap-data-exploration/tmp_daily_sales/prod"

agg_sales_weekly_store_sku_s3path = "s3://aap-data-exploration/tmp_weekly_sales/prod"

agg_sales_weekly_store_sku_by_bpg_s3path = "s3://aap-dpdm-dev/dpdm.db/cq_weekly_sales_by_bpg"

agg_sales_weekly_store_sku = "dds_prod.weekly_sales"

agg_sales_weekly_sku_s3path = "s3://aap-data-exploration/tmp_weekly_sales_by_sku/prod"

job_metrics_db_name = "dpdm_prod"
job_metrics_table_name = "dpdm_prod.dp_job_metrics"

%pyspark

pc_data = [
    Row(sequence_id=1, cal_year=2000, cal_period=1, start_date=date(2000, 1, 2), end_date=date(2000, 1, 29),days_in_period=28)
    , Row(sequence_id=2, cal_year=2000, cal_period=2, start_date=date(2000, 1, 30), end_date=date(2000, 2, 26),days_in_period=28)
    , Row(sequence_id=3, cal_year=2000, cal_period=3, start_date=date(2000, 2, 27), end_date=date(2000, 3, 25), days_in_period=28)
    , Row(sequence_id=4, cal_year=2000, cal_period=4, start_date=date(2000, 3, 26), end_date=date(2000, 4, 22), days_in_period=28)
    , Row(sequence_id=5, cal_year=2000, cal_period=5, start_date=date(2000, 4, 23), end_date=date(2000, 5, 20), days_in_period=28)
    , Row(sequence_id=6, cal_year=2000, cal_period=6, start_date=date(2000, 5, 21), end_date=date(2000, 6, 17), days_in_period=28)
    , Row(sequence_id=7, cal_year=2000, cal_period=7, start_date=date(2000, 6, 18), end_date=date(2000, 7, 15), days_in_period=28)
    , Row(sequence_id=8, cal_year=2000, cal_period=8, start_date=date(2000, 7, 16), end_date=date(2000, 8, 12), days_in_period=28)
    , Row(sequence_id=9, cal_year=2000, cal_period=9, start_date=date(2000, 8, 13), end_date=date(2000, 9, 9), days_in_period=28)
    , Row(sequence_id=10, cal_year=2000, cal_period=10, start_date=date(2000, 9, 10), end_date=date(2000, 10, 7), days_in_period=28)
    , Row(sequence_id=11, cal_year=2000, cal_period=11, start_date=date(2000, 10, 8), end_date=date(2000, 11, 4), days_in_period=28)
    , Row(sequence_id=12, cal_year=2000, cal_period=12, start_date=date(2000, 11, 5), end_date=date(2000, 12, 2), days_in_period=28)
    , Row(sequence_id=13, cal_year=2000, cal_period=13, start_date=date(2000, 12, 3), end_date=date(2000, 12, 30), days_in_period=28)
    , Row(sequence_id=14, cal_year=2001, cal_period=1, start_date=date(2000, 12, 31), end_date=date(2001, 1, 27), days_in_period=28)
    , Row(sequence_id=15, cal_year=2001, cal_period=2, start_date=date(2001, 1, 28), end_date=date(2001, 2, 24), days_in_period=28)
    , Row(sequence_id=16, cal_year=2001, cal_period=3, start_date=date(2001, 2, 25), end_date=date(2001, 3, 24), days_in_period=28)
    , Row(sequence_id=17, cal_year=2001, cal_period=4, start_date=date(2001, 3, 25), end_date=date(2001, 4, 21), days_in_period=28)
    , Row(sequence_id=18, cal_year=2001, cal_period=5, start_date=date(2001, 4, 22), end_date=date(2001, 5, 19), days_in_period=28)
    , Row(sequence_id=19, cal_year=2001, cal_period=6, start_date=date(2001, 5, 20), end_date=date(2001, 6, 16), days_in_period=28)
    , Row(sequence_id=20, cal_year=2001, cal_period=7, start_date=date(2001, 6, 17), end_date=date(2001, 7, 14), days_in_period=28)
    , Row(sequence_id=21, cal_year=2001, cal_period=8, start_date=date(2001, 7, 15), end_date=date(2001, 8, 11), days_in_period=28)
    , Row(sequence_id=22, cal_year=2001, cal_period=9, start_date=date(2001, 8, 12), end_date=date(2001, 9, 8), days_in_period=28)
    , Row(sequence_id=23, cal_year=2001, cal_period=10, start_date=date(2001, 9, 9), end_date=date(2001, 10, 6), days_in_period=28)
    , Row(sequence_id=24, cal_year=2001, cal_period=11, start_date=date(2001, 10, 7), end_date=date(2001, 11, 3), days_in_period=28)
    , Row(sequence_id=25, cal_year=2001, cal_period=12, start_date=date(2001, 11, 4), end_date=date(2001, 12, 1), days_in_period=28)
    , Row(sequence_id=26, cal_year=2001, cal_period=13, start_date=date(2001, 12, 2), end_date=date(2001, 12, 29), days_in_period=28)
    , Row(sequence_id=27, cal_year=2002, cal_period=1, start_date=date(2001, 12, 30), end_date=date(2002, 1, 26), days_in_period=28)
    , Row(sequence_id=28, cal_year=2002, cal_period=2, start_date=date(2002, 1, 27), end_date=date(2002, 2, 23), days_in_period=28)
    , Row(sequence_id=29, cal_year=2002, cal_period=3, start_date=date(2002, 2, 24), end_date=date(2002, 3, 23), days_in_period=28)
    , Row(sequence_id=30, cal_year=2002, cal_period=4, start_date=date(2002, 3, 24), end_date=date(2002, 4, 20), days_in_period=28)
    , Row(sequence_id=31, cal_year=2002, cal_period=5, start_date=date(2002, 4, 21), end_date=date(2002, 5, 18), days_in_period=28)
    , Row(sequence_id=32, cal_year=2002, cal_period=6, start_date=date(2002, 5, 19), end_date=date(2002, 6, 15), days_in_period=28)
    , Row(sequence_id=33, cal_year=2002, cal_period=7, start_date=date(2002, 6, 16), end_date=date(2002, 7, 13), days_in_period=28)
    , Row(sequence_id=34, cal_year=2002, cal_period=8, start_date=date(2002, 7, 14), end_date=date(2002, 8, 10), days_in_period=28)
    , Row(sequence_id=35, cal_year=2002, cal_period=9, start_date=date(2002, 8, 11), end_date=date(2002, 9, 7), days_in_period=28)
    , Row(sequence_id=36, cal_year=2002, cal_period=10, start_date=date(2002, 9, 8), end_date=date(2002, 10, 5), days_in_period=28)
    , Row(sequence_id=37, cal_year=2002, cal_period=11, start_date=date(2002, 10, 6), end_date=date(2002, 11, 2), days_in_period=28)
    , Row(sequence_id=38, cal_year=2002, cal_period=12, start_date=date(2002, 11, 3), end_date=date(2002, 11, 30), days_in_period=28)
    , Row(sequence_id=39, cal_year=2002, cal_period=13, start_date=date(2002, 12, 1), end_date=date(2002, 12, 28), days_in_period=28)
    , Row(sequence_id=40, cal_year=2003, cal_period=1, start_date=date(2002, 12, 29), end_date=date(2003, 1, 25), days_in_period=28)
    , Row(sequence_id=41, cal_year=2003, cal_period=2, start_date=date(2003, 1, 26), end_date=date(2003, 2, 22), days_in_period=28)
    , Row(sequence_id=42, cal_year=2003, cal_period=3, start_date=date(2003, 2, 23), end_date=date(2003, 3, 22), days_in_period=28)
    , Row(sequence_id=43, cal_year=2003, cal_period=4, start_date=date(2003, 3, 23), end_date=date(2003, 4, 19), days_in_period=28)
    , Row(sequence_id=44, cal_year=2003, cal_period=5, start_date=date(2003, 4, 20), end_date=date(2003, 5, 17), days_in_period=28)
    , Row(sequence_id=45, cal_year=2003, cal_period=6, start_date=date(2003, 5, 18), end_date=date(2003, 6, 14), days_in_period=28)
    , Row(sequence_id=46, cal_year=2003, cal_period=7, start_date=date(2003, 6, 15), end_date=date(2003, 7, 12), days_in_period=28)
    , Row(sequence_id=47, cal_year=2003, cal_period=8, start_date=date(2003, 7, 13), end_date=date(2003, 8, 9), days_in_period=28)
    , Row(sequence_id=48, cal_year=2003, cal_period=9, start_date=date(2003, 8, 10), end_date=date(2003, 9, 6), days_in_period=28)
    , Row(sequence_id=49, cal_year=2003, cal_period=10, start_date=date(2003, 9, 7), end_date=date(2003, 10, 4), days_in_period=28)
    , Row(sequence_id=50, cal_year=2003, cal_period=11, start_date=date(2003, 10, 5), end_date=date(2003, 11, 1), days_in_period=28)
    , Row(sequence_id=51, cal_year=2003, cal_period=12, start_date=date(2003, 11, 2), end_date=date(2003, 11, 29), days_in_period=28)
    , Row(sequence_id=52, cal_year=2003, cal_period=13, start_date=date(2003, 11, 30), end_date=date(2004, 1, 3), days_in_period=35)
    , Row(sequence_id=53, cal_year=2004, cal_period=1, start_date=date(2004, 1, 4), end_date=date(2004, 1, 31), days_in_period=28)
    , Row(sequence_id=54, cal_year=2004, cal_period=2, start_date=date(2004, 2, 1), end_date=date(2004, 2, 28), days_in_period=28)
    , Row(sequence_id=55, cal_year=2004, cal_period=3, start_date=date(2004, 2, 29), end_date=date(2004, 3, 27), days_in_period=28)
    , Row(sequence_id=56, cal_year=2004, cal_period=4, start_date=date(2004, 3, 28), end_date=date(2004, 4, 24), days_in_period=28)
    , Row(sequence_id=57, cal_year=2004, cal_period=5, start_date=date(2004, 4, 25), end_date=date(2004, 5, 22), days_in_period=28)
    , Row(sequence_id=58, cal_year=2004, cal_period=6, start_date=date(2004, 5, 23), end_date=date(2004, 6, 19), days_in_period=28)
    , Row(sequence_id=59, cal_year=2004, cal_period=7, start_date=date(2004, 6, 20), end_date=date(2004, 7, 17), days_in_period=28)
    , Row(sequence_id=60, cal_year=2004, cal_period=8, start_date=date(2004, 7, 18), end_date=date(2004, 8, 14), days_in_period=28)
    , Row(sequence_id=61, cal_year=2004, cal_period=9, start_date=date(2004, 8, 15), end_date=date(2004, 9, 11), days_in_period=28)
    , Row(sequence_id=62, cal_year=2004, cal_period=10, start_date=date(2004, 9, 12), end_date=date(2004, 10, 9), days_in_period=28)
    , Row(sequence_id=63, cal_year=2004, cal_period=11, start_date=date(2004, 10, 10), end_date=date(2004, 11, 6), days_in_period=28)
    , Row(sequence_id=64, cal_year=2004, cal_period=12, start_date=date(2004, 11, 7), end_date=date(2004, 12, 4), days_in_period=28)
    , Row(sequence_id=65, cal_year=2004, cal_period=13, start_date=date(2004, 12, 5), end_date=date(2005, 1, 1), days_in_period=28)
    , Row(sequence_id=66, cal_year=2005, cal_period=1, start_date=date(2005, 1, 2), end_date=date(2005, 1, 29), days_in_period=28)
    , Row(sequence_id=67, cal_year=2005, cal_period=2, start_date=date(2005, 1, 30), end_date=date(2005, 2, 26), days_in_period=28)
    , Row(sequence_id=68, cal_year=2005, cal_period=3, start_date=date(2005, 2, 27), end_date=date(2005, 3, 26), days_in_period=28)
    , Row(sequence_id=69, cal_year=2005, cal_period=4, start_date=date(2005, 3, 27), end_date=date(2005, 4, 23), days_in_period=28)
    , Row(sequence_id=70, cal_year=2005, cal_period=5, start_date=date(2005, 4, 24), end_date=date(2005, 5, 21), days_in_period=28)
    , Row(sequence_id=71, cal_year=2005, cal_period=6, start_date=date(2005, 5, 22), end_date=date(2005, 6, 18), days_in_period=28)
    , Row(sequence_id=72, cal_year=2005, cal_period=7, start_date=date(2005, 6, 19), end_date=date(2005, 7, 16), days_in_period=28)
    , Row(sequence_id=73, cal_year=2005, cal_period=8, start_date=date(2005, 7, 17), end_date=date(2005, 8, 13), days_in_period=28)
    , Row(sequence_id=74, cal_year=2005, cal_period=9, start_date=date(2005, 8, 14), end_date=date(2005, 9, 10), days_in_period=28)
    , Row(sequence_id=75, cal_year=2005, cal_period=10, start_date=date(2005, 9, 11), end_date=date(2005, 10, 8), days_in_period=28)
    , Row(sequence_id=76, cal_year=2005, cal_period=11, start_date=date(2005, 10, 9), end_date=date(2005, 11, 5), days_in_period=28)
    , Row(sequence_id=77, cal_year=2005, cal_period=12, start_date=date(2005, 11, 6), end_date=date(2005, 12, 3), days_in_period=28)
    , Row(sequence_id=78, cal_year=2005, cal_period=13, start_date=date(2005, 12, 4), end_date=date(2005, 12, 31), days_in_period=28)
    , Row(sequence_id=79, cal_year=2006, cal_period=1, start_date=date(2006, 1, 1), end_date=date(2006, 1, 28), days_in_period=28)
    , Row(sequence_id=80, cal_year=2006, cal_period=2, start_date=date(2006, 1, 29), end_date=date(2006, 2, 25), days_in_period=28)
    , Row(sequence_id=81, cal_year=2006, cal_period=3, start_date=date(2006, 2, 26), end_date=date(2006, 3, 25), days_in_period=28)
    , Row(sequence_id=82, cal_year=2006, cal_period=4, start_date=date(2006, 3, 26), end_date=date(2006, 4, 22), days_in_period=28)
    , Row(sequence_id=83, cal_year=2006, cal_period=5, start_date=date(2006, 4, 23), end_date=date(2006, 5, 20), days_in_period=28)
    , Row(sequence_id=84, cal_year=2006, cal_period=6, start_date=date(2006, 5, 21), end_date=date(2006, 6, 17), days_in_period=28)
    , Row(sequence_id=85, cal_year=2006, cal_period=7, start_date=date(2006, 6, 18), end_date=date(2006, 7, 15), days_in_period=28)
    , Row(sequence_id=86, cal_year=2006, cal_period=8, start_date=date(2006, 7, 16), end_date=date(2006, 8, 12), days_in_period=28)
    , Row(sequence_id=87, cal_year=2006, cal_period=9, start_date=date(2006, 8, 13), end_date=date(2006, 9, 9), days_in_period=28)
    , Row(sequence_id=88, cal_year=2006, cal_period=10, start_date=date(2006, 9, 10), end_date=date(2006, 10, 7), days_in_period=28)
    , Row(sequence_id=89, cal_year=2006, cal_period=11, start_date=date(2006, 10, 8), end_date=date(2006, 11, 4), days_in_period=28)
    , Row(sequence_id=90, cal_year=2006, cal_period=12, start_date=date(2006, 11, 5), end_date=date(2006, 12, 2), days_in_period=28)
    , Row(sequence_id=91, cal_year=2006, cal_period=13, start_date=date(2006, 12, 3), end_date=date(2006, 12, 30), days_in_period=28)
    , Row(sequence_id=92, cal_year=2007, cal_period=1, start_date=date(2006, 12, 31), end_date=date(2007, 1, 27), days_in_period=28)
    , Row(sequence_id=93, cal_year=2007, cal_period=2, start_date=date(2007, 1, 28), end_date=date(2007, 2, 24), days_in_period=28)
    , Row(sequence_id=94, cal_year=2007, cal_period=3, start_date=date(2007, 2, 25), end_date=date(2007, 3, 24), days_in_period=28)
    , Row(sequence_id=95, cal_year=2007, cal_period=4, start_date=date(2007, 3, 25), end_date=date(2007, 4, 21), days_in_period=28)
    , Row(sequence_id=96, cal_year=2007, cal_period=5, start_date=date(2007, 4, 22), end_date=date(2007, 5, 19), days_in_period=28)
    , Row(sequence_id=97, cal_year=2007, cal_period=6, start_date=date(2007, 5, 20), end_date=date(2007, 6, 16), days_in_period=28)
    , Row(sequence_id=98, cal_year=2007, cal_period=7, start_date=date(2007, 6, 17), end_date=date(2007, 7, 14), days_in_period=28)
    , Row(sequence_id=99, cal_year=2007, cal_period=8, start_date=date(2007, 7, 15), end_date=date(2007, 8, 11), days_in_period=28)
    , Row(sequence_id=100, cal_year=2007, cal_period=9, start_date=date(2007, 8, 12), end_date=date(2007, 9, 8), days_in_period=28)
    , Row(sequence_id=101, cal_year=2007, cal_period=10, start_date=date(2007, 9, 9), end_date=date(2007, 10, 6), days_in_period=28)
    , Row(sequence_id=102, cal_year=2007, cal_period=11, start_date=date(2007, 10, 7), end_date=date(2007, 11, 3), days_in_period=28)
    , Row(sequence_id=103, cal_year=2007, cal_period=12, start_date=date(2007, 11, 4), end_date=date(2007, 12, 1), days_in_period=28)
    , Row(sequence_id=104, cal_year=2007, cal_period=13, start_date=date(2007, 12, 2), end_date=date(2007, 12, 29), days_in_period=28)
    , Row(sequence_id=105, cal_year=2008, cal_period=1, start_date=date(2007, 12, 30), end_date=date(2008, 1, 26), days_in_period=28)
    , Row(sequence_id=106, cal_year=2008, cal_period=2, start_date=date(2008, 1, 27), end_date=date(2008, 2, 23), days_in_period=28)
    , Row(sequence_id=107, cal_year=2008, cal_period=3, start_date=date(2008, 2, 24), end_date=date(2008, 3, 22), days_in_period=28)
    , Row(sequence_id=108, cal_year=2008, cal_period=4, start_date=date(2008, 3, 23), end_date=date(2008, 4, 19), days_in_period=28)
    , Row(sequence_id=109, cal_year=2008, cal_period=5, start_date=date(2008, 4, 20), end_date=date(2008, 5, 17), days_in_period=28)
    , Row(sequence_id=110, cal_year=2008, cal_period=6, start_date=date(2008, 5, 18), end_date=date(2008, 6, 14), days_in_period=28)
    , Row(sequence_id=111, cal_year=2008, cal_period=7, start_date=date(2008, 6, 15), end_date=date(2008, 7, 12), days_in_period=28)
    , Row(sequence_id=112, cal_year=2008, cal_period=8, start_date=date(2008, 7, 13), end_date=date(2008, 8, 9), days_in_period=28)
    , Row(sequence_id=113, cal_year=2008, cal_period=9, start_date=date(2008, 8, 10), end_date=date(2008, 9, 6), days_in_period=28)
    , Row(sequence_id=114, cal_year=2008, cal_period=10, start_date=date(2008, 9, 7), end_date=date(2008, 10, 4), days_in_period=28)
    , Row(sequence_id=115, cal_year=2008, cal_period=11, start_date=date(2008, 10, 5), end_date=date(2008, 11, 1), days_in_period=28)
    , Row(sequence_id=116, cal_year=2008, cal_period=12, start_date=date(2008, 11, 2), end_date=date(2008, 11, 29), days_in_period=28)
    , Row(sequence_id=117, cal_year=2008, cal_period=13, start_date=date(2008, 11, 30), end_date=date(2009, 1, 3), days_in_period=35)
    , Row(sequence_id=118, cal_year=2009, cal_period=1, start_date=date(2009, 1, 4), end_date=date(2009, 1, 31), days_in_period=28)
    , Row(sequence_id=119, cal_year=2009, cal_period=2, start_date=date(2009, 2, 1), end_date=date(2009, 2, 28), days_in_period=28)
    , Row(sequence_id=120, cal_year=2009, cal_period=3, start_date=date(2009, 3, 1), end_date=date(2009, 3, 28), days_in_period=28)
    , Row(sequence_id=121, cal_year=2009, cal_period=4, start_date=date(2009, 3, 29), end_date=date(2009, 4, 25), days_in_period=28)
    , Row(sequence_id=122, cal_year=2009, cal_period=5, start_date=date(2009, 4, 26), end_date=date(2009, 5, 23), days_in_period=28)
    , Row(sequence_id=123, cal_year=2009, cal_period=6, start_date=date(2009, 5, 24), end_date=date(2009, 6, 20), days_in_period=28)
    , Row(sequence_id=124, cal_year=2009, cal_period=7, start_date=date(2009, 6, 21), end_date=date(2009, 7, 18), days_in_period=28)
    , Row(sequence_id=125, cal_year=2009, cal_period=8, start_date=date(2009, 7, 19), end_date=date(2009, 8, 15), days_in_period=28)
    , Row(sequence_id=126, cal_year=2009, cal_period=9, start_date=date(2009, 8, 16), end_date=date(2009, 9, 12), days_in_period=28)
    , Row(sequence_id=127, cal_year=2009, cal_period=10, start_date=date(2009, 9, 13), end_date=date(2009, 10, 10), days_in_period=28)
    , Row(sequence_id=128, cal_year=2009, cal_period=11, start_date=date(2009, 10, 11), end_date=date(2009, 11, 7), days_in_period=28)
    , Row(sequence_id=129, cal_year=2009, cal_period=12, start_date=date(2009, 11, 8), end_date=date(2009, 12, 5), days_in_period=28)
    , Row(sequence_id=130, cal_year=2009, cal_period=13, start_date=date(2009, 12, 6), end_date=date(2010, 1, 2), days_in_period=28)
    , Row(sequence_id=131, cal_year=2010, cal_period=1, start_date=date(2010, 1, 3), end_date=date(2010, 1, 30), days_in_period=28)
    , Row(sequence_id=132, cal_year=2010, cal_period=2, start_date=date(2010, 1, 31), end_date=date(2010, 2, 27), days_in_period=28)
    , Row(sequence_id=133, cal_year=2010, cal_period=3, start_date=date(2010, 2, 28), end_date=date(2010, 3, 27), days_in_period=28)
    , Row(sequence_id=134, cal_year=2010, cal_period=4, start_date=date(2010, 3, 28), end_date=date(2010, 4, 24), days_in_period=28)
    , Row(sequence_id=135, cal_year=2010, cal_period=5, start_date=date(2010, 4, 25), end_date=date(2010, 5, 22), days_in_period=28)
    , Row(sequence_id=136, cal_year=2010, cal_period=6, start_date=date(2010, 5, 23), end_date=date(2010, 6, 19), days_in_period=28)
    , Row(sequence_id=137, cal_year=2010, cal_period=7, start_date=date(2010, 6, 20), end_date=date(2010, 7, 17), days_in_period=28)
    , Row(sequence_id=138, cal_year=2010, cal_period=8, start_date=date(2010, 7, 18), end_date=date(2010, 8, 14), days_in_period=28)
    , Row(sequence_id=139, cal_year=2010, cal_period=9, start_date=date(2010, 8, 15), end_date=date(2010, 9, 11), days_in_period=28)
    , Row(sequence_id=140, cal_year=2010, cal_period=10, start_date=date(2010, 9, 12), end_date=date(2010, 10, 9), days_in_period=28)
    , Row(sequence_id=141, cal_year=2010, cal_period=11, start_date=date(2010, 10, 10), end_date=date(2010, 11, 6), days_in_period=28)
    , Row(sequence_id=142, cal_year=2010, cal_period=12, start_date=date(2010, 11, 7), end_date=date(2010, 12, 4), days_in_period=28)
    , Row(sequence_id=143, cal_year=2010, cal_period=13, start_date=date(2010, 12, 5), end_date=date(2011, 1, 1), days_in_period=28)
    , Row(sequence_id=144, cal_year=2011, cal_period=1, start_date=date(2011, 1, 2), end_date=date(2011, 1, 29), days_in_period=28)
    , Row(sequence_id=145, cal_year=2011, cal_period=2, start_date=date(2011, 1, 30), end_date=date(2011, 2, 26), days_in_period=28)
    , Row(sequence_id=146, cal_year=2011, cal_period=3, start_date=date(2011, 2, 27), end_date=date(2011, 3, 26), days_in_period=28)
    , Row(sequence_id=147, cal_year=2011, cal_period=4, start_date=date(2011, 3, 27), end_date=date(2011, 4, 23), days_in_period=28)
    , Row(sequence_id=148, cal_year=2011, cal_period=5, start_date=date(2011, 4, 24), end_date=date(2011, 5, 21), days_in_period=28)
    , Row(sequence_id=149, cal_year=2011, cal_period=6, start_date=date(2011, 5, 22), end_date=date(2011, 6, 18), days_in_period=28)
    , Row(sequence_id=150, cal_year=2011, cal_period=7, start_date=date(2011, 6, 19), end_date=date(2011, 7, 16), days_in_period=28)
    , Row(sequence_id=151, cal_year=2011, cal_period=8, start_date=date(2011, 7, 17), end_date=date(2011, 8, 13), days_in_period=28)
    , Row(sequence_id=152, cal_year=2011, cal_period=9, start_date=date(2011, 8, 14), end_date=date(2011, 9, 10), days_in_period=28)
    , Row(sequence_id=153, cal_year=2011, cal_period=10, start_date=date(2011, 9, 11), end_date=date(2011, 10, 8), days_in_period=28)
    , Row(sequence_id=154, cal_year=2011, cal_period=11, start_date=date(2011, 10, 9), end_date=date(2011, 11, 5), days_in_period=28)
    , Row(sequence_id=155, cal_year=2011, cal_period=12, start_date=date(2011, 11, 6), end_date=date(2011, 12, 3), days_in_period=28)
    , Row(sequence_id=156, cal_year=2011, cal_period=13, start_date=date(2011, 12, 4), end_date=date(2011, 12, 31), days_in_period=28)
    , Row(sequence_id=157, cal_year=2012, cal_period=1, start_date=date(2012, 1, 1), end_date=date(2012, 1, 28), days_in_period=28)
    , Row(sequence_id=158, cal_year=2012, cal_period=2, start_date=date(2012, 1, 29), end_date=date(2012, 2, 25), days_in_period=28)
    , Row(sequence_id=159, cal_year=2012, cal_period=3, start_date=date(2012, 2, 26), end_date=date(2012, 3, 24), days_in_period=28)
    , Row(sequence_id=160, cal_year=2012, cal_period=4, start_date=date(2012, 3, 25), end_date=date(2012, 4, 21), days_in_period=28)
    , Row(sequence_id=161, cal_year=2012, cal_period=5, start_date=date(2012, 4, 22), end_date=date(2012, 5, 19), days_in_period=28)
    , Row(sequence_id=162, cal_year=2012, cal_period=6, start_date=date(2012, 5, 20), end_date=date(2012, 6, 16), days_in_period=28)
    , Row(sequence_id=163, cal_year=2012, cal_period=7, start_date=date(2012, 6, 17), end_date=date(2012, 7, 14), days_in_period=28)
    , Row(sequence_id=164, cal_year=2012, cal_period=8, start_date=date(2012, 7, 15), end_date=date(2012, 8, 11), days_in_period=28)
    , Row(sequence_id=165, cal_year=2012, cal_period=9, start_date=date(2012, 8, 12), end_date=date(2012, 9, 8), days_in_period=28)
    , Row(sequence_id=166, cal_year=2012, cal_period=10, start_date=date(2012, 9, 9), end_date=date(2012, 10, 6), days_in_period=28)
    , Row(sequence_id=167, cal_year=2012, cal_period=11, start_date=date(2012, 10, 7), end_date=date(2012, 11, 3), days_in_period=28)
    , Row(sequence_id=168, cal_year=2012, cal_period=12, start_date=date(2012, 11, 4), end_date=date(2012, 12, 1), days_in_period=28)
    , Row(sequence_id=169, cal_year=2012, cal_period=13, start_date=date(2012, 12, 2), end_date=date(2012, 12, 29), days_in_period=28)
    , Row(sequence_id=170, cal_year=2013, cal_period=1, start_date=date(2012, 12, 30), end_date=date(2013, 1, 26), days_in_period=28)
    , Row(sequence_id=171, cal_year=2013, cal_period=2, start_date=date(2013, 1, 27), end_date=date(2013, 2, 23), days_in_period=28)
    , Row(sequence_id=172, cal_year=2013, cal_period=3, start_date=date(2013, 2, 24), end_date=date(2013, 3, 23), days_in_period=28)
    , Row(sequence_id=173, cal_year=2013, cal_period=4, start_date=date(2013, 3, 24), end_date=date(2013, 4, 20), days_in_period=28)
    , Row(sequence_id=174, cal_year=2013, cal_period=5, start_date=date(2013, 4, 21), end_date=date(2013, 5, 18), days_in_period=28)
    , Row(sequence_id=175, cal_year=2013, cal_period=6, start_date=date(2013, 5, 19), end_date=date(2013, 6, 15), days_in_period=28)
    , Row(sequence_id=176, cal_year=2013, cal_period=7, start_date=date(2013, 6, 16), end_date=date(2013, 7, 13), days_in_period=28)
    , Row(sequence_id=177, cal_year=2013, cal_period=8, start_date=date(2013, 7, 14), end_date=date(2013, 8, 10), days_in_period=28)
    , Row(sequence_id=178, cal_year=2013, cal_period=9, start_date=date(2013, 8, 11), end_date=date(2013, 9, 7), days_in_period=28)
    , Row(sequence_id=179, cal_year=2013, cal_period=10, start_date=date(2013, 9, 8), end_date=date(2013, 10, 5), days_in_period=28)
    , Row(sequence_id=180, cal_year=2013, cal_period=11, start_date=date(2013, 10, 6), end_date=date(2013, 11, 2), days_in_period=28)
    , Row(sequence_id=181, cal_year=2013, cal_period=12, start_date=date(2013, 11, 3), end_date=date(2013, 11, 30), days_in_period=28)
    , Row(sequence_id=182, cal_year=2013, cal_period=13, start_date=date(2013, 12, 1), end_date=date(2013, 12, 28), days_in_period=28)
    , Row(sequence_id=183, cal_year=2014, cal_period=1, start_date=date(2013, 12, 29), end_date=date(2014, 1, 25), days_in_period=28)
    , Row(sequence_id=184, cal_year=2014, cal_period=2, start_date=date(2014, 1, 26), end_date=date(2014, 2, 22), days_in_period=28)
    , Row(sequence_id=185, cal_year=2014, cal_period=3, start_date=date(2014, 2, 23), end_date=date(2014, 3, 22), days_in_period=28)
    , Row(sequence_id=186, cal_year=2014, cal_period=4, start_date=date(2014, 3, 23), end_date=date(2014, 4, 19), days_in_period=28)
    , Row(sequence_id=187, cal_year=2014, cal_period=5, start_date=date(2014, 4, 20), end_date=date(2014, 5, 17), days_in_period=28)
    , Row(sequence_id=188, cal_year=2014, cal_period=6, start_date=date(2014, 5, 18), end_date=date(2014, 6, 14), days_in_period=28)
    , Row(sequence_id=189, cal_year=2014, cal_period=7, start_date=date(2014, 6, 15), end_date=date(2014, 7, 12), days_in_period=28)
    , Row(sequence_id=190, cal_year=2014, cal_period=8, start_date=date(2014, 7, 13), end_date=date(2014, 8, 9), days_in_period=28)
    , Row(sequence_id=191, cal_year=2014, cal_period=9, start_date=date(2014, 8, 10), end_date=date(2014, 9, 6), days_in_period=28)
    , Row(sequence_id=192, cal_year=2014, cal_period=10, start_date=date(2014, 9, 7), end_date=date(2014, 10, 4), days_in_period=28)
    , Row(sequence_id=193, cal_year=2014, cal_period=11, start_date=date(2014, 10, 5), end_date=date(2014, 11, 1), days_in_period=28)
    , Row(sequence_id=194, cal_year=2014, cal_period=12, start_date=date(2014, 11, 2), end_date=date(2014, 11, 29), days_in_period=28)
    , Row(sequence_id=195, cal_year=2014, cal_period=13, start_date=date(2014, 11, 30), end_date=date(2015, 1, 3), days_in_period=35)
    , Row(sequence_id=196, cal_year=2015, cal_period=1, start_date=date(2015, 1, 4), end_date=date(2015, 1, 31), days_in_period=28)
    , Row(sequence_id=197, cal_year=2015, cal_period=2, start_date=date(2015, 2, 1), end_date=date(2015, 2, 28), days_in_period=28)
    , Row(sequence_id=198, cal_year=2015, cal_period=3, start_date=date(2015, 3, 1), end_date=date(2015, 3, 28), days_in_period=28)
    , Row(sequence_id=199, cal_year=2015, cal_period=4, start_date=date(2015, 3, 29), end_date=date(2015, 4, 25), days_in_period=28)
    , Row(sequence_id=200, cal_year=2015, cal_period=5, start_date=date(2015, 4, 26), end_date=date(2015, 5, 23), days_in_period=28)
    , Row(sequence_id=201, cal_year=2015, cal_period=6, start_date=date(2015, 5, 24), end_date=date(2015, 6, 20), days_in_period=28)
    , Row(sequence_id=202, cal_year=2015, cal_period=7, start_date=date(2015, 6, 21), end_date=date(2015, 7, 18), days_in_period=28)
    , Row(sequence_id=203, cal_year=2015, cal_period=8, start_date=date(2015, 7, 19), end_date=date(2015, 8, 15), days_in_period=28)
    , Row(sequence_id=204, cal_year=2015, cal_period=9, start_date=date(2015, 8, 16), end_date=date(2015, 9, 12), days_in_period=28)
    , Row(sequence_id=205, cal_year=2015, cal_period=10, start_date=date(2015, 9, 13), end_date=date(2015, 10, 10), days_in_period=28)
    , Row(sequence_id=206, cal_year=2015, cal_period=11, start_date=date(2015, 10, 11), end_date=date(2015, 11, 7), days_in_period=28)
    , Row(sequence_id=207, cal_year=2015, cal_period=12, start_date=date(2015, 11, 8), end_date=date(2015, 12, 5), days_in_period=28)
    , Row(sequence_id=208, cal_year=2015, cal_period=13, start_date=date(2015, 12, 6), end_date=date(2016, 1, 2), days_in_period=28)
    , Row(sequence_id=209, cal_year=2016, cal_period=1, start_date=date(2016, 1, 3), end_date=date(2016, 1, 30), days_in_period=28)
    , Row(sequence_id=210, cal_year=2016, cal_period=2, start_date=date(2016, 1, 31), end_date=date(2016, 2, 27), days_in_period=28)
    , Row(sequence_id=211, cal_year=2016, cal_period=3, start_date=date(2016, 2, 28), end_date=date(2016, 3, 26), days_in_period=28)
    , Row(sequence_id=212, cal_year=2016, cal_period=4, start_date=date(2016, 3, 27), end_date=date(2016, 4, 23), days_in_period=28)
    , Row(sequence_id=213, cal_year=2016, cal_period=5, start_date=date(2016, 4, 24), end_date=date(2016, 5, 21), days_in_period=28)
    , Row(sequence_id=214, cal_year=2016, cal_period=6, start_date=date(2016, 5, 22), end_date=date(2016, 6, 18), days_in_period=28)
    , Row(sequence_id=215, cal_year=2016, cal_period=7, start_date=date(2016, 6, 19), end_date=date(2016, 7, 16), days_in_period=28)
    , Row(sequence_id=216, cal_year=2016, cal_period=8, start_date=date(2016, 7, 17), end_date=date(2016, 8, 13), days_in_period=28)
    , Row(sequence_id=217, cal_year=2016, cal_period=9, start_date=date(2016, 8, 14), end_date=date(2016, 9, 10), days_in_period=28)
    , Row(sequence_id=218, cal_year=2016, cal_period=10, start_date=date(2016, 9, 11), end_date=date(2016, 10, 8), days_in_period=28)
    , Row(sequence_id=219, cal_year=2016, cal_period=11, start_date=date(2016, 10, 9), end_date=date(2016, 11, 5), days_in_period=28)
    , Row(sequence_id=220, cal_year=2016, cal_period=12, start_date=date(2016, 11, 6), end_date=date(2016, 12, 3), days_in_period=28)
    , Row(sequence_id=221, cal_year=2016, cal_period=13, start_date=date(2016, 12, 4), end_date=date(2016, 12, 31), days_in_period=28)
    , Row(sequence_id=222, cal_year=2017, cal_period=1, start_date=date(2017, 1, 1), end_date=date(2017, 1, 28), days_in_period=28)
    , Row(sequence_id=223, cal_year=2017, cal_period=2, start_date=date(2017, 1, 29), end_date=date(2017, 2, 25), days_in_period=28)
    , Row(sequence_id=224, cal_year=2017, cal_period=3, start_date=date(2017, 2, 26), end_date=date(2017, 3, 25), days_in_period=28)
    , Row(sequence_id=225, cal_year=2017, cal_period=4, start_date=date(2017, 3, 26), end_date=date(2017, 4, 22), days_in_period=28)
    , Row(sequence_id=226, cal_year=2017, cal_period=5, start_date=date(2017, 4, 23), end_date=date(2017, 5, 20), days_in_period=28)
    , Row(sequence_id=227, cal_year=2017, cal_period=6, start_date=date(2017, 5, 21), end_date=date(2017, 6, 17), days_in_period=28)
    , Row(sequence_id=228, cal_year=2017, cal_period=7, start_date=date(2017, 6, 18), end_date=date(2017, 7, 15), days_in_period=28)
    , Row(sequence_id=229, cal_year=2017, cal_period=8, start_date=date(2017, 7, 16), end_date=date(2017, 8, 12), days_in_period=28)
    , Row(sequence_id=230, cal_year=2017, cal_period=9, start_date=date(2017, 8, 13), end_date=date(2017, 9, 9), days_in_period=28)
    , Row(sequence_id=231, cal_year=2017, cal_period=10, start_date=date(2017, 9, 10), end_date=date(2017, 10, 7), days_in_period=28)
    , Row(sequence_id=232, cal_year=2017, cal_period=11, start_date=date(2017, 10, 8), end_date=date(2017, 11, 4), days_in_period=28)
    , Row(sequence_id=233, cal_year=2017, cal_period=12, start_date=date(2017, 11, 5), end_date=date(2017, 12, 2), days_in_period=28)
    , Row(sequence_id=234, cal_year=2017, cal_period=13, start_date=date(2017, 12, 3), end_date=date(2017, 12, 30), days_in_period=28)
    , Row(sequence_id=235, cal_year=2018, cal_period=1, start_date=date(2017, 12, 31), end_date=date(2018, 1, 27), days_in_period=28)
    , Row(sequence_id=236, cal_year=2018, cal_period=2, start_date=date(2018, 1, 28), end_date=date(2018, 2, 24), days_in_period=28)
    , Row(sequence_id=237, cal_year=2018, cal_period=3, start_date=date(2018, 2, 25), end_date=date(2018, 3, 24), days_in_period=28)
    , Row(sequence_id=238, cal_year=2018, cal_period=4, start_date=date(2018, 3, 25), end_date=date(2018, 4, 21), days_in_period=28)
    , Row(sequence_id=239, cal_year=2018, cal_period=5, start_date=date(2018, 4, 22), end_date=date(2018, 5, 19), days_in_period=28)
    , Row(sequence_id=240, cal_year=2018, cal_period=6, start_date=date(2018, 5, 20), end_date=date(2018, 6, 16), days_in_period=28)
    , Row(sequence_id=241, cal_year=2018, cal_period=7, start_date=date(2018, 6, 17), end_date=date(2018, 7, 14), days_in_period=28)
    , Row(sequence_id=242, cal_year=2018, cal_period=8, start_date=date(2018, 7, 15), end_date=date(2018, 8, 11), days_in_period=28)
    , Row(sequence_id=243, cal_year=2018, cal_period=9, start_date=date(2018, 8, 12), end_date=date(2018, 9, 8), days_in_period=28)
    , Row(sequence_id=244, cal_year=2018, cal_period=10, start_date=date(2018, 9, 9), end_date=date(2018, 10, 6), days_in_period=28)
    , Row(sequence_id=245, cal_year=2018, cal_period=11, start_date=date(2018, 10, 7), end_date=date(2018, 11, 3), days_in_period=28)
    , Row(sequence_id=246, cal_year=2018, cal_period=12, start_date=date(2018, 11, 4), end_date=date(2018, 12, 1), days_in_period=28)
    , Row(sequence_id=247, cal_year=2018, cal_period=13, start_date=date(2018, 12, 2), end_date=date(2018, 12, 29), days_in_period=28)
    , Row(sequence_id=248, cal_year=2019, cal_period=1, start_date=date(2018, 12, 30), end_date=date(2019, 1, 26), days_in_period=28)
    , Row(sequence_id=249, cal_year=2019, cal_period=2, start_date=date(2019, 1, 27), end_date=date(2019, 2, 23), days_in_period=28)
    , Row(sequence_id=250, cal_year=2019, cal_period=3, start_date=date(2019, 2, 24), end_date=date(2019, 3, 23), days_in_period=28)
    , Row(sequence_id=251, cal_year=2019, cal_period=4, start_date=date(2019, 3, 24), end_date=date(2019, 4, 20), days_in_period=28)
    , Row(sequence_id=252, cal_year=2019, cal_period=5, start_date=date(2019, 4, 21), end_date=date(2019, 5, 18), days_in_period=28)
    , Row(sequence_id=253, cal_year=2019, cal_period=6, start_date=date(2019, 5, 19), end_date=date(2019, 6, 15), days_in_period=28)
    , Row(sequence_id=254, cal_year=2019, cal_period=7, start_date=date(2019, 6, 16), end_date=date(2019, 7, 13), days_in_period=28)
    , Row(sequence_id=255, cal_year=2019, cal_period=8, start_date=date(2019, 7, 14), end_date=date(2019, 8, 10), days_in_period=28)
    , Row(sequence_id=256, cal_year=2019, cal_period=9, start_date=date(2019, 8, 11), end_date=date(2019, 9, 7), days_in_period=28)
    , Row(sequence_id=257, cal_year=2019, cal_period=10, start_date=date(2019, 9, 8), end_date=date(2019, 10, 5), days_in_period=28)
    , Row(sequence_id=258, cal_year=2019, cal_period=11, start_date=date(2019, 10, 6), end_date=date(2019, 11, 2), days_in_period=28)
    , Row(sequence_id=259, cal_year=2019, cal_period=12, start_date=date(2019, 11, 3), end_date=date(2019, 11, 30), days_in_period=28)
    , Row(sequence_id=260, cal_year=2019, cal_period=13, start_date=date(2019, 12, 1), end_date=date(2019, 12, 28), days_in_period=28)
    , Row(sequence_id=261, cal_year=2020, cal_period=1, start_date=date(2019, 12, 29), end_date=date(2020, 1, 25), days_in_period=28)
    , Row(sequence_id=262, cal_year=2020, cal_period=2, start_date=date(2020, 1, 26), end_date=date(2020, 2, 22), days_in_period=28)
    , Row(sequence_id=263, cal_year=2020, cal_period=3, start_date=date(2020, 2, 23), end_date=date(2020, 3, 21), days_in_period=28)
    , Row(sequence_id=264, cal_year=2020, cal_period=4, start_date=date(2020, 3, 22), end_date=date(2020, 4, 18), days_in_period=28)
    , Row(sequence_id=265, cal_year=2020, cal_period=5, start_date=date(2020, 4, 19), end_date=date(2020, 5, 16), days_in_period=28)
    , Row(sequence_id=266, cal_year=2020, cal_period=6, start_date=date(2020, 5, 17), end_date=date(2020, 6, 13), days_in_period=28)
    , Row(sequence_id=267, cal_year=2020, cal_period=7, start_date=date(2020, 6, 14), end_date=date(2020, 7, 11), days_in_period=28)
    , Row(sequence_id=268, cal_year=2020, cal_period=8, start_date=date(2020, 7, 12), end_date=date(2020, 8, 8), days_in_period=28)
    , Row(sequence_id=269, cal_year=2020, cal_period=9, start_date=date(2020, 8, 9), end_date=date(2020, 9, 5), days_in_period=28)
    , Row(sequence_id=270, cal_year=2020, cal_period=10, start_date=date(2020, 9, 6), end_date=date(2020, 10, 3), days_in_period=28)
    , Row(sequence_id=271, cal_year=2020, cal_period=11, start_date=date(2020, 10, 4), end_date=date(2020, 10, 31), days_in_period=28)
    , Row(sequence_id=272, cal_year=2020, cal_period=12, start_date=date(2020, 11, 1), end_date=date(2020, 11, 28), days_in_period=28)
    , Row(sequence_id=273, cal_year=2020, cal_period=13, start_date=date(2020, 11, 29), end_date=date(2021, 1, 2), days_in_period=35)
    , Row(sequence_id=274, cal_year=2021, cal_period=1, start_date=date(2021, 1, 3), end_date=date(2021, 1, 30), days_in_period=28)
    , Row(sequence_id=275, cal_year=2021, cal_period=2, start_date=date(2021, 1, 31), end_date=date(2021, 2, 27), days_in_period=28)
    , Row(sequence_id=276, cal_year=2021, cal_period=3, start_date=date(2021, 2, 28), end_date=date(2021, 3, 27), days_in_period=28)
    , Row(sequence_id=277, cal_year=2021, cal_period=4, start_date=date(2021, 3, 28), end_date=date(2021, 4, 24), days_in_period=28)
    , Row(sequence_id=278, cal_year=2021, cal_period=5, start_date=date(2021, 4, 25), end_date=date(2021, 5, 22), days_in_period=28)
    , Row(sequence_id=279, cal_year=2021, cal_period=6, start_date=date(2021, 5, 23), end_date=date(2021, 6, 19), days_in_period=28)
    , Row(sequence_id=280, cal_year=2021, cal_period=7, start_date=date(2021, 6, 20), end_date=date(2021, 7, 17), days_in_period=28)
    , Row(sequence_id=281, cal_year=2021, cal_period=8, start_date=date(2021, 7, 18), end_date=date(2021, 8, 14), days_in_period=28)
    , Row(sequence_id=282, cal_year=2021, cal_period=9, start_date=date(2021, 8, 15), end_date=date(2021, 9, 11), days_in_period=28)
    , Row(sequence_id=283, cal_year=2021, cal_period=10, start_date=date(2021, 9, 12), end_date=date(2021, 10, 9), days_in_period=28)
    , Row(sequence_id=284, cal_year=2021, cal_period=11, start_date=date(2021, 10, 10), end_date=date(2021, 11, 6), days_in_period=28)
    , Row(sequence_id=285, cal_year=2021, cal_period=12, start_date=date(2021, 11, 7), end_date=date(2021, 12, 4), days_in_period=28)
    , Row(sequence_id=286, cal_year=2021, cal_period=13, start_date=date(2021, 12, 5), end_date=date(2022, 1, 1), days_in_period=28)

]

ad_promo_mc_data = [
    # 2015
      Row(sequence_id=1, cal_year=2015, cal_month=1,  start_date=date(2015,  1,  1), end_date=date(2015,  1, 28), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2015, cal_month=2,  start_date=date(2015,  1, 29), end_date=date(2015,  2, 25), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2015, cal_month=3,  start_date=date(2015,  2, 26), end_date=date(2015,  4,  1), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2015, cal_month=4,  start_date=date(2015,  4,  2), end_date=date(2015,  4, 29), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2015, cal_month=5,  start_date=date(2015,  4, 30), end_date=date(2015,  5, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2015, cal_month=6,  start_date=date(2015,  5, 28), end_date=date(2015,  7,  1), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2015, cal_month=7,  start_date=date(2015,  7,  2), end_date=date(2015,  7, 29), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2015, cal_month=8,  start_date=date(2015,  7, 30), end_date=date(2015,  8, 26), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2015, cal_month=9,  start_date=date(2015,  8, 27), end_date=date(2015,  9, 30), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2015, cal_month=10, start_date=date(2015, 10,  1), end_date=date(2015, 10, 28), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2015, cal_month=11, start_date=date(2015, 10, 29), end_date=date(2015, 12,  2), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2015, cal_month=12, start_date=date(2015, 12,  3), end_date=date(2015, 12, 30), weeks_in_month=4)
    # 2016
    , Row(sequence_id=1, cal_year=2016, cal_month=1,  start_date=date(2015, 12, 31), end_date=date(2016,  1, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2016, cal_month=2,  start_date=date(2016,  1, 28), end_date=date(2016,  2, 24), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2016, cal_month=3,  start_date=date(2016,  2, 25), end_date=date(2016,  3, 30), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2016, cal_month=4,  start_date=date(2016,  3, 31), end_date=date(2016,  4, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2016, cal_month=5,  start_date=date(2016,  4, 28), end_date=date(2016,  6,  1), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2016, cal_month=6,  start_date=date(2016,  6,  2), end_date=date(2016,  6, 29), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2016, cal_month=7,  start_date=date(2016,  6, 30), end_date=date(2016,  7, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2016, cal_month=8,  start_date=date(2016,  7, 28), end_date=date(2016,  8, 31), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2016, cal_month=9,  start_date=date(2016,  9,  1), end_date=date(2016,  9, 28), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2016, cal_month=10, start_date=date(2016,  9, 29), end_date=date(2016, 10, 26), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2016, cal_month=11, start_date=date(2016, 10, 27), end_date=date(2016, 11, 30), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2016, cal_month=12, start_date=date(2016, 12,  1), end_date=date(2016, 12, 28), weeks_in_month=4)
    # 2017
    , Row(sequence_id=1, cal_year=2017, cal_month=1,  start_date=date(2016, 12, 29), end_date=date(2017,  1, 25), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2017, cal_month=2,  start_date=date(2017,  1, 26), end_date=date(2017,  2, 22), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2017, cal_month=3,  start_date=date(2017,  2, 23), end_date=date(2017,  3, 29), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2017, cal_month=4,  start_date=date(2017,  3, 30), end_date=date(2017,  4, 26), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2017, cal_month=5,  start_date=date(2017,  4, 27), end_date=date(2017,  5, 31), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2017, cal_month=6,  start_date=date(2017,  6,  1), end_date=date(2017,  6, 28), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2017, cal_month=7,  start_date=date(2017,  6, 29), end_date=date(2017,  7, 26), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2017, cal_month=8,  start_date=date(2017,  7, 27), end_date=date(2017,  8, 30), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2017, cal_month=9,  start_date=date(2017,  8, 31), end_date=date(2017,  9, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2017, cal_month=10, start_date=date(2017,  9, 28), end_date=date(2017, 10, 25), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2017, cal_month=11, start_date=date(2017, 10, 26), end_date=date(2017, 11, 29), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2017, cal_month=12, start_date=date(2017, 11, 30), end_date=date(2017, 12, 27), weeks_in_month=4)
    # 2018
    , Row(sequence_id=1, cal_year=2018, cal_month=1,  start_date=date(2017, 12, 28), end_date=date(2018,  1, 24), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2018, cal_month=2,  start_date=date(2018,  1, 25), end_date=date(2018,  2, 21), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2018, cal_month=3,  start_date=date(2018,  2, 22), end_date=date(2018,  3, 28), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2018, cal_month=4,  start_date=date(2018,  3, 29), end_date=date(2018,  4, 25), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2018, cal_month=5,  start_date=date(2018,  4, 26), end_date=date(2018,  5, 30), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2018, cal_month=6,  start_date=date(2018,  5, 31), end_date=date(2018,  6, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2018, cal_month=7,  start_date=date(2018,  6, 28), end_date=date(2018,  7, 25), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2018, cal_month=8,  start_date=date(2018,  7, 26), end_date=date(2018,  8, 29), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2018, cal_month=9,  start_date=date(2018,  8, 30), end_date=date(2018,  9, 26), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2018, cal_month=10, start_date=date(2018,  9, 27), end_date=date(2018, 10, 24), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2018, cal_month=11, start_date=date(2018, 10, 25), end_date=date(2018, 11, 28), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2018, cal_month=12, start_date=date(2018, 11, 29), end_date=date(2018, 12, 26), weeks_in_month=4)
    #2019
    , Row(sequence_id=1, cal_year=2019, cal_month=1,  start_date=date(2019,  1, 3), end_date=date(2019,  1, 30), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2019, cal_month=2,  start_date=date(2019,  1, 31), end_date=date(2019,  2, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2019, cal_month=3,  start_date=date(2019,  2, 28), end_date=date(2019,  3, 27), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2019, cal_month=4,  start_date=date(2019,  3, 28), end_date=date(2019,  5, 1), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2019, cal_month=5,  start_date=date(2019,  5, 2), end_date=date(2019,  5, 29), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2019, cal_month=6,  start_date=date(2019,  5, 30), end_date=date(2019,  6, 26), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2019, cal_month=7,  start_date=date(2019,  6, 27), end_date=date(2019,  7, 31), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2019, cal_month=8,  start_date=date(2019,  8, 1), end_date=date(2019,  8, 28), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2019, cal_month=9,  start_date=date(2019,  8, 29), end_date=date(2019,  9, 25), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2019, cal_month=10, start_date=date(2019,  9, 26), end_date=date(2019, 10, 30), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2019, cal_month=11, start_date=date(2019, 10, 31), end_date=date(2019, 11, 27), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2019, cal_month=12, start_date=date(2019, 11, 28), end_date=date(2020, 1, 1), weeks_in_month=4)
    #2020
    , Row(sequence_id=1, cal_year=2020, cal_month=1,  start_date=date(2020,  1, 2), end_date=date(2020,  1, 29), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2020, cal_month=2,  start_date=date(2020,  1, 30), end_date=date(2020,  2, 26), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2020, cal_month=3,  start_date=date(2020,  2, 27), end_date=date(2020,  4, 1), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2020, cal_month=4,  start_date=date(2020,  4, 2), end_date=date(2020,  4, 29), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2020, cal_month=5,  start_date=date(2020,  4, 30), end_date=date(2020,  5, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2020, cal_month=6,  start_date=date(2020,  5, 28), end_date=date(2020,  7, 1), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2020, cal_month=7,  start_date=date(2020,  7, 2), end_date=date(2020,  8, 26), weeks_in_month=8)
    , Row(sequence_id=1, cal_year=2020, cal_month=8,  start_date=date(2020,  8, 27), end_date=date(2020,  9, 30), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2020, cal_month=9,  start_date=date(2020,  10, 1), end_date=date(2020, 10, 28), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2020, cal_month=10, start_date=date(2020,  10, 29), end_date=date(2020, 12, 30), weeks_in_month=9)
     #2021
    , Row(sequence_id=1, cal_year=2021, cal_month=1,  start_date=date(2020, 12, 31), end_date=date(2021, 2, 17), weeks_in_month=6)
    , Row(sequence_id=1, cal_year=2021, cal_month=2,  start_date=date(2021,  2, 18), end_date=date(2021,  3, 31), weeks_in_month=6)
    , Row(sequence_id=1, cal_year=2021, cal_month=3,  start_date=date(2021,  4, 1), end_date=date(2021,  4, 28), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2021, cal_month=4,  start_date=date(2021,  4, 29), end_date=date(2021,  5, 26), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2021, cal_month=5,  start_date=date(2021,  5, 27), end_date=date(2021,  6, 30), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2021, cal_month=6,  start_date=date(2021,  7, 1), end_date=date(2021,  7, 28), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2021, cal_month=7,  start_date=date(2021,  7, 29), end_date=date(2021,  8, 25), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2021, cal_month=8,  start_date=date(2021,  8, 26), end_date=date(2021,  9, 29), weeks_in_month=5)
    , Row(sequence_id=1, cal_year=2021, cal_month=9,  start_date=date(2021,  9, 30), end_date=date(2021, 10, 27), weeks_in_month=4)
    , Row(sequence_id=1, cal_year=2021, cal_month=10, start_date=date(2021,  10, 28), end_date=date(2021, 12, 29), weeks_in_month=9)
    ]


def init_pc_df(pc_data):

    print ("Creating pc_df")
    pc_df = sqlContext.createDataFrame(pc_data)
    pc_df.cache()

    return pc_df
    
def get_pc_df():

    print ("Creating pc_df")
    pc_df = sqlContext.createDataFrame(pc_data)
    pc_df.cache()

    return pc_df


def init_ad_promo_mc_df(ad_promo_mc_data):

    print ("Creating ad_promo_mc_df")
    ad_promo_mc_df = sqlContext.createDataFrame(ad_promo_mc_data)
    ad_promo_mc_df.cache()

    return ad_promo_mc_df


# Initialize/load AAP period calendar
pc_df = init_pc_df(pc_data)

# Initialize AAP ad promo monthly calendar
ad_promo_mc_df = init_ad_promo_mc_df(ad_promo_mc_data)


def load_sales_details(daily_changes=False, as_of_date=None):
        """ Load sales details from configured data store

            if daily_changes = True ; Will load the sales data of maximum processed dttm

            Else False ; Will load complete sales data

            Initially set to false, if load_sales_details(True) is called then sales data of maximum processed dttm is loaded

        Returns:
            rdd (RDD) : RDD (sales transaction level details) loaded from the data source.

        """
        if daily_changes:
            #data_filter = "and csdate between '2018-07-21' and '2018-07-30'"

            df_csdates = sqlContext.sql("select distinct csdate from " + sales_details_table_name + \
                                             " where date(processed_dttm) = (select date(max(processed_dttm)) from " + sales_details_table_name + ")")

            csdates_rdd = df_csdates.rdd.collect()

            csdates = []
            for row in csdates_rdd:
                csdates.append("'" + str(row.csdate) + "'")

            csdates_list = ','.join(csdates)

            data_filter = " and csdate in (" + csdates_list + ")"
        else:
            if as_of_date:
                csdates = []

                for i in as_of_date:
                    csdates.append("'" + str(i) + "'")

                csdates_list = ','.join(csdates)

                data_filter = " and csdate in (" + csdates_list + ")"
            else:
                data_filter = ""

        sqlContext.sql("use " + sales_details_db_name)
        # Consider only records which has csdtyp 01,04,11,21
        tran = sqlContext.sql("select * from " + sales_details_table_name + " where csityp in (11,12) and csdsts = 0 " + data_filter)

        # Data for DIY ecommerce transactions
        ecom_tran = sqlContext.sql("select * from " + sales_details_table_name + \
                                        " where csdsts = 0 and csstor = 1020 and csityp in (11,12) and csostr is not null and csostr != 0 " + data_filter)

        ecom_tran.show()

        ecom_tran.createOrReplaceTempView("ecom_tran")

        ecom_tran_modify = sqlContext.sql("SELECT store_rk,sku_rk,date_rk \
                                                                      ,csostr AS csstor \
                                                                      ,cscen,csreg,csroll,cstran,csseq \
                                                                      ,csdtyp \
                                                                      ,cssku,csretl,cscost \
                                                                      ,csqty,csexpr,csexcs,csdsts \
                                                                      ,cspovr,cstil,csupc,csexvt \
                                                                      ,csvexm,csityp,csexds,csrgpr \
                                                                      ,csrstp,csdrsn,csosls,csostr \
                                                                      ,csocen,csodat,cspspr,csddoc \
                                                                      ,csscan,csatyp,csacct,csprtp \
                                                                      ,csscur,cstcur,cstrat,cstmd \
                                                                      ,cstrtl,cstrpr,cstppr,cstcst \
                                                                      ,cstxpr,cstxcs,cstxvt,cstxds \
                                                                      ,csoreg,csotrn,csprcd,cssdlc \
                                                                      ,cspcmp,cscevt,load_id,processed_dttm \
                                                                      ,invoice_year,invoice_period,csdate FROM ecom_tran")

        return tran.unionAll(ecom_tran_modify)

load_sales_test = load_sales_details()
load_sales_test.show(2)


def load_cq_sales_details(daily_changes=False):
        """ Load sales details from configured data store

            if daily_changes = True ; Will load the sales data of maximum processed dttm

            Else False ; Will load complete sales data

            Initially set to false, if load_sales_details(True) is called then sales data of maximum processed dttm is loaded

        Returns:
            rdd (RDD) : RDD (sales transaction level details) loaded from the data source.

        """
        if daily_changes:
            #data_filter = "and csdate between '2018-09-01' and '2018-10-30'"

            df_csdates = sqlContext.sql("select distinct cast(csdate as date) from " + sales_details_table_name + \
                                             " where processed_dttm = (select max(processed_dttm) from " + sales_details_table_name + ")")

            csdates_rdd = df_csdates.rdd.collect()

            csdates = []
            for row in csdates_rdd:
                csdates.append("'" + str(row.csdate) + "'")

            csdates_list = ','.join(csdates)

            data_filter = " and cast(csdate as date) in (" + csdates_list + ")"

        else:

            data_filter = ""

        df_exploris = sqlContext.sql('select * from '+sales_details_table_name)
        df_exploris = df_exploris.withColumn("csdate", df_exploris["csdate"].cast("date"))
        df_exploris.createOrReplaceTempView('df_exploris')
    
        # Consider only records which has csdtyp 01,04,11,21
        tran = sqlContext.sql("select * from df_exploris where csityp in (11,12) and csdsts = 0 " + data_filter)

        return tran


load_test = load_cq_sales_details()
load_test.show(2)


def load_sales_header():
        """ Load sales details from configured data store

        Returns:
            rdd (RDD) : RDD (sales transaction level details) loaded from the data source.

        """

        sqlContext.sql("use " + sales_details_db_name)
        return sqlContext.sql("select * from " + sales_header_table_name)

def load_store_sales_by_period():
        sqlContext.sql("use " + store_sales_by_period_db_name)
        return sqlContext.sql("select * from " + store_sales_by_period_table_name)

def drop_sales_details_with_0_quantity(df):
        """ Transformation to filter un-necessary/irrelavent sales

        Args:
            df (DataFrame) : DataFrame (sales transaction level details) to filter.

        Returns:
            rdd (DataFrame) : Filtered DataFrame

        """

        return df


def get_store_sales_by_period(df):

        """ Transformation to compute cy, py, ppy aggregates

        Args:
            df (DataFrame) : DataFrame (sales store,sku level details) to transform.

        Returns:
            df (DataFrame) : Transformed DataFrame

        """
        df=df.withColumn("customer_type",when(df.csacct == ' ', 'DIY').otherwise('DIFM'))
        #df.printSchema()
        df=df.withColumnRenamed("csstor","store_number").withColumnRenamed("cssku", "sku_number").withColumnRenamed("invoice_year", "fiscal_year").withColumnRenamed("invoice_period", "fiscal_period")
        #df=df.withColumnRenamed("store_number_cq","store_number").withColumnRenamed("sku_number_cq", "sku_number").withColumnRenamed("invoice_year", "fiscal_year").withColumnRenamed("invoice_period", "fiscal_period")
        #df.printSchema()
        #remove conditions on csdtyp | use csityp instead of csdtyp everywhere in code
        ssbp = df.groupby("store_number"
                       , "sku_number"
                       , "fiscal_year"
                       , "fiscal_period"
                       , "customer_type"
                       ).agg(sum(col("csqty")).alias("qty_sold")
                             ,sum(col("csexpr")).alias("gross_sales")
                             ,sum(col("csqty") * col("cscost")).alias("sales_cost")
                             )
        #

        """agg(sum(when(((df.csdtyp == '01') | (df.csdtyp == '04') | (df.csdtyp == '11') | (df.csdtyp == '21')), col("csqty")).
                                      otherwise(0)).alias("qty_sold")
                             ,sum(when(((df.csdtyp == '01') | (df.csdtyp == '04') | (df.csdtyp == '11') | (df.csdtyp == '21')), col("csexpr")).
                                 otherwise(0)).alias("gross_sales")
                             , sum(when(((df.csdtyp == '01') | (df.csdtyp == '04') | (df.csdtyp == '11') | (df.csdtyp == '21')), col("csqty") * col("cscost")).
                                   otherwise(0)).alias("sales_cost")
                             )"""

        return ssbp


def agg_sales_by_store_sku_daily_sales(df_sales):

        """ Transformation to aggregate sales by store, sku based on daily basis of processed_dttm

                Args:
                        df_sales_last_processed (DataFrame) : DataFrame (sales transaction level details) to transform.

                        Returns:
                            df_daily_sales (DataFrame) : Transformed DataFrame

            """

        df_agg_daily_sales = df_sales.groupBy("date_rk",
                                                  "csstor",
                                                  "cssku",
                                                  "csdate",
                                                  "invoice_period",
                                                  "invoice_year").agg(sum(col('csqty')).alias('qty_sold'),
                                                                      sum(col('csexpr')).alias('sales_price'),
                                                                      sum(col("csqty") * col("cscost")).alias(
                                                                          'sales_cost'))

        df_agg_daily_sales.createOrReplaceTempView("df_agg_daily_sales")

        df_sku_master = sqlContext.sql("select skunum, sku_rk,valid_end_dttm from " + sku_master_table_name + \
                                            " where valid_end_dttm = '9999-12-31 00:00:00'")
        df_sku_master.createOrReplaceTempView("df_sku_master")

        df_sku_master_join_agg_sales = sqlContext.sql("select B.sku_rk, A.cssku, A.csstor, A.qty_sold, A.date_rk, A.sales_price, A.sales_cost, A.invoice_period, A.invoice_year,A.csdate \
                                                                from df_agg_daily_sales A left join df_sku_master B on A.cssku = B.skunum")

        df_sku_master_join_agg_sales.createOrReplaceTempView("df_sku_master_join_agg_sales")

        df_store_master = sqlContext.sql("select storenum, store_rk, valid_end_dttm from " + store_master_table_name +
                                              " where valid_end_dttm = '9999-12-31 00:00:00' ")
        df_store_master.createOrReplaceTempView("df_store_master")

        df_store_master_join_agg_sales = sqlContext.sql("select B.store_rk,A.csstor, A.sku_rk, A.date_rk, A.cssku, A.qty_sold, A.sales_price, A.sales_cost, A.invoice_period, A.invoice_year,A.csdate \
                                                                  from df_sku_master_join_agg_sales A left join df_store_master B on A.csstor = B.storenum ")

        df_store_master_join_agg_sales.createOrReplaceTempView("df_store_master_join_agg_sales")

        df_daily_sales = sqlContext.sql("select cast(store_rk as bigint), \
                                                         cast(sku_rk as bigint), \
                                                         cast(date_rk as bigint), \
                                                         cast(csstor as bigint), \
                                                         cast(cssku as decimal(20,0)), \
                                                         cast(qty_sold as decimal(22,3)), \
                                                         cast(sales_price as decimal(25,2)), \
                                                         cast(sales_cost as decimal(25,4)), \
                                                         cast(invoice_year as int), \
                                                         cast(invoice_period as int), \
                                                         csdate from df_store_master_join_agg_sales")

        return df_daily_sales


def agg_sales_by_store_sku_weekly(df_sales_data, full_refresh=True):

        df_cal_hierarchy = sqlContext.sql("select dfyr, dwoy, dgdt, dper from " + cal_hierarchy_table_name)
        df_cal_hierarchy.cache()
        df_cal_hierarchy.createOrReplaceTempView("df_cal_hierarchy")

        if full_refresh:
            df = sqlContext.sql("select * from " + daily_sales_store_sku)
            df.createOrReplaceTempView("df")

        else:
            df_sales_data.createOrReplaceTempView("df_sales_data")

            df_csdates = sqlContext.sql("select distinct csdate from df_sales_data")

            df_csdates.createOrReplaceTempView("df_csdates")
            daily_csdates_rdd = df_csdates.rdd.collect()
            daily_csdates = []
            for row in daily_csdates_rdd:
                daily_csdates.append("'" + str(row.csdate) + "'")

            daily_csdates_list = ','.join(daily_csdates)

            daily_date_filter = "(" + daily_csdates_list + ")"

            df_get_yr_woy = sqlContext.sql(
                    "select distinct dfyr, dwoy from df_cal_hierarchy where dgdt in " + daily_date_filter)

            rdd_yr_woy = df_get_yr_woy.rdd.collect()

            df_weekly_dates = None

            for row in rdd_yr_woy:
                df_dates = sqlContext.sql(
                        "select dgdt from df_cal_hierarchy where dfyr = " + str(row['dfyr']) + " and dwoy = " + str(
                            row['dwoy']))
                if df_weekly_dates:
                    df_weekly_dates = df_weekly_dates.union(df_dates)
                else:
                    df_weekly_dates = df_dates

            df_weekly_dates.show(50)

            df_week_dates = df_weekly_dates.rdd.collect()
            weekly_csdates = []
            for row in df_week_dates:
                weekly_csdates.append("'" + str(row.dgdt) + "'")

                weekly_csdates_list = ','.join(weekly_csdates)

                weekly_date_filter = "(" + weekly_csdates_list + ")"

            df = sqlContext.sql("select * from " + daily_sales_store_sku + " where csdate in " + weekly_date_filter)
            df.createOrReplaceTempView("df")

        df_weekly = sqlContext.sql("select B.dper, B.dwoy, A.store_rk,A.csstor, A.sku_rk, A.cssku, A.qty_sold, A.sales_price, A.sales_cost, A.invoice_period, A.invoice_year,A.csdate \
                                             from df A left join df_cal_hierarchy B on A.csdate = B.dgdt")

        df_weekly_sales = df_weekly.groupBy("store_rk",
                                                "sku_rk",
                                                "csstor",
                                                "cssku",
                                                "dwoy",
                                                "dper",
                                                "invoice_period",
                                                "invoice_year").agg(sum(col('qty_sold')).alias('qty_sold'),
                                                            sum(col('sales_price')).alias('sales_price'),
                                                            sum(col('sales_cost')).alias('sales_cost'))

        df_weekly_sales.createOrReplaceTempView("df_weekly_sales")

        df_final_weekly_sales = sqlContext.sql("select cast(store_rk as int), \
                                                                  cast(sku_rk as int),\
                                                                  cast(csstor as int) storenum, \
                                                                  cast(cssku as bigint) skunum, \
                                                                  cast(dper as tinyint), \
                                                                  cast(qty_sold as decimal(31,3)), \
                                                                  cast(sales_price as decimal(34,2)), \
                                                                  cast(sales_cost as decimal(34,4)), \
                                                                  cast(invoice_year as smallint) dfyr, \
                                                                  cast(invoice_period as tinyint), \
                                                                  cast(dwoy as tinyint) from df_weekly_sales ")

        return df_final_weekly_sales



def agg_sales_by_sku_weekly(df_agg_sales_by_store_sku_weekly = None):

        sqlContext.sql("msck repair table " + agg_sales_weekly_store_sku)

        if df_agg_sales_by_store_sku_weekly:
            df = df_agg_sales_by_store_sku_weekly
        else:
            df = sqlContext.sql("select * from " + agg_sales_weekly_store_sku)

        df_weekly_sales_by_sku = df.groupBy("sku_rk",
                                            "skunum",
                                            "dwoy",
                                            "dper",
                                            "invoice_period",
                                            "dfyr").agg(sum(col('qty_sold')).alias('qty_sold'),
                                                            sum(col('sales_price')).alias('sales_price'),
                                                            sum(col('sales_cost')).alias('sales_cost'))

        return df_weekly_sales_by_sku


def agg_sales_by_store_sku_ad_promo_monthly(dates=[]):

        df_ad_promo_mc = get_ad_promo_mc_df()

        ad_promo_cal = []

        for row in df_ad_promo_mc.rdd.collect():
            date = row.start_date
            while (date <= row.end_date):
                ad_promo_cal.append(Row(date=date, cal_year=row.cal_year, cal_month=row.cal_month))
                date = date + timedelta(days=1)

        df_promo_cal = sqlContext.createDataFrame(ad_promo_cal)

        df_promo_cal.createOrReplaceTempView("df_promo_cal")

        df = sqlContext.sql("select A.store_rk, \
                                         A.sku_rk,\
                                         A.csstor, \
                                         A.cssku, \
                                         A.qty_sold, \
                                         A.sales_price, \
                                         A.sales_cost, \
                                         B.cal_year, \
                                         B.cal_month from " + daily_sales_store_sku + " A left join df_promo_cal B on A.csdate=B.date")

        if dates != []:

            print ("Filtering on months based on dates")
            df.createOrReplaceTempView("df")

            as_of_date = []
            for each_date in dates:
                as_of_date.append("'" + str(each_date) + "'")

            csdates_list = ','.join(as_of_date)

            date_filter = " date in (" + csdates_list + ")"

            print(date_filter)

            df_filter_year_month_cal = sqlContext.sql(
                "select distinct cal_year as year, cal_month as month from df_promo_cal where " + date_filter)

            df_filter_year_month_cal.createOrReplaceTempView("df_filter_year_month_cal")

            df = sqlContext.sql(
                "select A.csstor, A.cssku, A.qty_sold, A.sales_price, A.sales_cost, A.cal_year, A.cal_month from df A join df_filter_year_month_cal B on A.cal_year=B.year and A.cal_month=B.month")
            df.createOrReplaceTempView("df")
            df_filtered = sqlContext.sql("select distinct cal_year, cal_month from df")
            df_filtered.show()

        print("Computing ETL...")
        df_agg_ad_promo_monthly_sales = df.groupBy("store_rk",
                                                   "sku_rk",
                                                   "csstor",
                                                   "cssku",
                                                   "cal_year",
                                                   "cal_month").agg(sum(col("qty_sold")).alias("qty_sold")
                                                                  , sum(col("sales_price")).alias("sales_price")
                                                                  , sum(col("sales_cost")).alias("sales_cost"))

        df_agg_ad_promo_monthly_sales.createOrReplaceTempView("df_final_ad_promo_monthly_sales")

        df_sku_base_product_group = sqlContext.sql("select sku_number,merchandise_group_desc from " + sku_prodgrp_table_name)
        df_sku_base_product_group.createOrReplaceTempView("df_sku_base_product_group")

        df_sku_base_product_join = sqlContext.sql("select A.store_rk, A.sku_rk, A.csstor, A.cssku, A.qty_sold, A.sales_price, A.sales_cost, A.cal_year, A.cal_month, B.merchandise_group_desc \
                                                                  from df_final_ad_promo_monthly_sales A left join df_sku_base_product_group B \
                                                                       on A.cssku = B.sku_number")

        df_sku_base_product_join.createOrReplaceTempView("df_final_ad_promo_sales")

        df_ad_promo_monthly_sales_by_store_sku = sqlContext.sql("select cast(store_rk as int), \
                                                                       cast(sku_rk as int), \
                                                                       cast(csstor as bigint) stornum, \
                                                                       cast(cssku as bigint) skunum, \
                                                                       cast(qty_sold as decimal(31,3)),\
                                                                       cast(sales_price as decimal(34,2)), \
                                                                       cast(sales_cost as decimal(34,4)),\
                                                                       cast (cal_year as smallint) year, \
                                                                       cast (cal_month as tinyint) month,\
                                                                       cast (merchandise_group_desc as varchar(50))  \
                                                                       from df_final_ad_promo_sales ")

        return df_ad_promo_monthly_sales_by_store_sku



def agg_sales_by_sku_ad_promo_monthly(dates=[]):

        """ Transformation to aggregate sales by sku based on promo monthly calendar

                Args:
                    df (DataFrame) : DataFrame (sales transaction level details) to transform.

                Returns:
                    df_ad_promo_monthly_sales (DataFrame) : Transformed DataFrame

        """

	    # print(dates)

        # invoice_cshdet.createOrReplaceTempView("invoice_cshdet")

        df_ad_promo_mc = get_ad_promo_mc_df()

        ad_promo_cal = []

        for row in df_ad_promo_mc.rdd.collect():
            date = row.start_date
            while (date <= row.end_date):
                ad_promo_cal.append(Row(date=date, cal_year=row.cal_year, cal_month=row.cal_month))
                date = date + timedelta(days=1)

        df_promo_cal = sqlContext.createDataFrame(ad_promo_cal)

        df_promo_cal.createOrReplaceTempView("df_promo_cal")

        df = sqlContext.sql("select A.sku_rk, A.cssku, A.qty_sold, A.sales_price, A.sales_cost, B.cal_year, B.cal_month from " + daily_sales_store_sku + " A left join df_promo_cal B on A.csdate=B.date")

        if dates != []:

            print("Filtering on months based on dates")
            df.createOrReplaceTempView("df")

            as_of_date = []
            for each_date in dates:
                as_of_date.append("'" + str(each_date) + "'")

            csdates_list = ','.join(as_of_date)

            date_filter = " date in (" + csdates_list + ")"

            print(date_filter)

            df_filter_year_month_cal = sqlContext.sql("select distinct cal_year as year, cal_month as month from df_promo_cal where " + date_filter)

            df_filter_year_month_cal.createOrReplaceTempView("df_filter_year_month_cal")

            df = sqlContext.sql("select A.cssku, A.qty_sold, A.sales_price, A.sales_cost, A.cal_year, A.cal_month from df A join df_filter_year_month_cal B on A.cal_year=B.year and A.cal_month=B.month")
            df.createOrReplaceTempView("df")
            df_filtered = sqlContext.sql("select distinct cal_year, cal_month from df")
            df_filtered.show()

            print("Computing ETL...")
            #df.createOrReplaceTempView("df")

            #df_filtered_2 = sqlContext.sql("select distinct cal_year, cal_month from df")
            #df_filtered_2.show()
            df_agg_ad_promo_monthly_sales = df.groupBy("sku_rk","cssku","cal_year","cal_month").agg(sum(col("qty_sold")).alias("qty_sold")
                                                                                         , sum(col("sales_price")).alias("gross_price")
                                                                                         , sum(col("sales_cost")).alias("sales_cost"))

            df_agg_ad_promo_monthly_sales.createOrReplaceTempView("df_final_ad_promo_monthly_sales")

            df_sku_base_product_group = sqlContext.sql("select sku_number,merchandise_group_desc from " +  sku_prodgrp_table_name)
            df_sku_base_product_group.createOrReplaceTempView("df_sku_base_product_group")

            df_sku_base_product_join = sqlContext.sql("select A.sku_rk, A.cssku, A.qty_sold, A.gross_price, A.sales_cost, A.cal_year, A.cal_month, B.merchandise_group_desc \
                                                           from df_final_ad_promo_monthly_sales A left join df_sku_base_product_group B \
                                                            on A.cssku = B.sku_number")

            df_sku_base_product_join.createOrReplaceTempView("df_final_ad_promo_sales")

            df_ad_promo_monthly_sales = sqlContext.sql("select cast(sku_rk as int), \
                                                                    cast(cssku as bigint) skunum, \
                                                                    cast(qty_sold as decimal(31,3)),\
                                                                    cast(gross_sales as decimal(34,2)) sales_price, \
                                                                    cast(sales_cost as decimal(34,4)),\
                                                                    cast (cal_year as smallint) year, \
                                                                    cast (cal_month as tinyint) month,\
                                                                    cast (merchandise_group_desc as varchar(50))  \
                                                                    from df_final_ad_promo_sales ")

        return df_ad_promo_monthly_sales


def agg_sales_by_store_sku_transaction(df):
        """ Transformation to aggregate sales by store, sku, transaction

        Args:
            df (DataFrame) : DataFrame (sales transaction level details) to transform.

        Returns:
            df (DataFrame) : Transformed DataFrame

        """

        return df.groupby("invoice_year"
                          , "invoice_period"
                          , "csdate"
                          , "csstor"
                          , "cstran"
                          , "csreg"
                          , "csroll"
                          , "cssku"
                          ).agg(count("csacct").alias("sources_total")
                                , sum(when(df.csacct != ' ', 1).otherwise(0)).alias("sources_outstore")
                                , sum(when(df.csacct == ' ', 1).otherwise(0)).alias("sources_instore")
                                , sum("csqty").alias("qty_total")
                                , sum(when(df.csacct != ' ', col("csqty")).otherwise(0)).alias("qty_outstore")
                                , sum(when(df.csacct == ' ', col("csqty")).otherwise(0)).alias("qty_instore")
                                , sum(when(df.csdtyp == '11', -1 * col("csqty")).otherwise(0)).alias("qty_returns")
                                , sum("csexpr").alias("amt_total")
                                , avg("cscost").alias("cost_unit")
                                                                )


def agg_sales_by_store_sku(df):
        """ Transformation to aggregate sales by store, sku

        Args:
            df (DataFrame) : DataFrame (sales store,sku level details) to transform.

        Returns:
            df (DataFrame) : Transformed DataFrame

        """

        return df.groupby("invoice_year"
                          , "invoice_period"
                          , "csdate"
                          , "csstor"
                          , "cssku"
                          ).agg(count("csacct").alias("sources_total")
                                , sum(when(df.csacct != ' ', 1).otherwise(0)).alias("sources_outstore")
                                , sum(when(df.csacct == ' ', 1).otherwise(0)).alias("sources_instore")
                                , sum("csqty").alias("qty_total")
                                , sum(when(df.csacct != ' ', col("csqty")).otherwise(0)).alias("qty_outstore")
                                , sum(when(df.csacct == ' ', col("csqty")).otherwise(0)).alias("qty_instore")
                                , sum(when(df.csdtyp == '11', -1 * col("csqty")).otherwise(0)).alias("qty_returns")
                                , sum("csexpr").alias("amt_total")
                                , avg("cscost").alias("cost_unit")
                                )              


def agg_sales_by_store_sku_transaction_rolling_window_cy(agg_with_sku_data_part_type):
        """ Transformation to compute cy, py, ppy aggregates

        Args:
            df (DataFrame) : DataFrame (sales transaction level details) to transform.

        Returns:
            df (DataFrame) : Transformed DataFrame

        """
        agg_with_sku_data_part_type.registerTempTable("dfx")

        agg_cy = sqlContext.sql("select\
                          csstor \
                        , cstran \
                        , csreg \
                        , csroll \
                        , cssku , sources_total\
                        , sources_outstore \
                        , sources_instore \
                        , qty_total \
                        , qty_outstore \
                        , qty_instore \
                        , qty_returns \
                        ,case when csdate between date_add(add_months(csdate,-12),1) and csdate then sum(qty_total) \
                         over w else 0 end as cy_qty_total \
                        ,case when csdate between date_add(add_months(csdate,-12),1) and csdate then sum(qty_outstore)\
                         over w else 0 end as cy_qty_outstore \
                        ,case when csdate between date_add(add_months(csdate,-12),1) and csdate then sum(qty_instore) \
                         over w else 0 end as cy_qty_instore \
        ,case when csdate between date_add(add_months(csdate,-24),1) and date_add(add_months(csdate,-12),1) \
        then sum(qty_total) over w else 0 end as py_qty_total \
        ,case when csdate between date_add(add_months(csdate,-24),1) and date_add(add_months(csdate,-12),1) \
        then sum(qty_outstore) over w else 0 end as py_qty_outstore \
        ,case when csdate between date_add(add_months(csdate,-24),1) and date_add(add_months(csdate,-12),1) \
        then sum(qty_instore) over w else 0 end as py_qty_instore \
        ,case when csdate between date_add(add_months(csdate,-36),1) and date_add(add_months(csdate,-24),1) \
        then sum(qty_total) over w else 0 end as ppy_qty_total \
        ,case when csdate between date_add(add_months(csdate,-36),1) and date_add(add_months(csdate,-24),1) \
        then sum(qty_outstore) over w else 0 end as ppy_qty_outstore \
        ,case when csdate between date_add(add_months(csdate,-36),1) and date_add(add_months(csdate,-24),1) \
         then sum(qty_instore) over w else 0 end as ppy_qty_instore \
                        ,amt_total \
                        ,cost_unit \
                        , store_zipcode \
                        , store_state \
                        , store_dc \
                        , store_lat \
                        , store_long \
                        , store_open_flag \
                        , store_open_date \
                        , store_close_date \
                        , store_superhub \
                        , merchandise_division_desc \
                        , merchandise_group_desc \
                        , merchandise_department_desc \
                        , merchandise_class_desc \
                        , merchandise_subclass_desc \
                        , mpog_id \
                        , mpog_description \
                        , mpog_flag \
                        , discontinued_flg \
                        , stocking_location \
                        , part_type \
                        , invoice_year \
                        , invoice_period \
                        , csdate \
                from dfx \
                    WINDOW w as (partition by invoice_year,invoice_period, csstor, cstran, csreg, csroll\
                         , cssku)")





        return agg_cy  



def agg_sales_by_store_sku_rolling_window_cy(agg_with_sku_part_type):
        """ Transformation to compute cy, py, ppy aggregates

        Args:
            df (DataFrame) : DataFrame (sales store,sku level details) to transform.

        Returns:
            df (DataFrame) : Transformed DataFrame

        """
        agg_with_sku_part_type.registerTempTable("dfx")

        agg_store_sku_df = sqlContext.sql("select csstor \
                        , cssku \
                        , sources_total\
                        , sources_outstore \
                        , sources_instore \
                        , qty_total \
                        , qty_outstore \
                        , qty_instore \
                        , qty_returns \
                        ,case when csdate between date_add(add_months(csdate,-12),1) and csdate then sum(qty_total) \
                         over w else 0 end as cy_qty_total \
                        ,case when csdate between date_add(add_months(csdate,-12),1) and csdate then sum(qty_outstore)\
                         over w else 0 end as cy_qty_outstore \
                        ,case when csdate between date_add(add_months(csdate,-12),1) and csdate then sum(qty_instore) \
                         over w else 0 end as cy_qty_instore \
                        ,case when csdate between date_add(add_months(csdate,-24),1) and date_add(add_months(csdate,-12),1) then sum(qty_total) \
                         over w else 0 end as py_qty_total \
                        ,case when csdate between date_add(add_months(csdate,-24),1) and date_add(add_months(csdate,-12),1) then sum(qty_outstore)\
                         over w else 0 end as py_qty_outstore \
                        ,case when csdate between date_add(add_months(csdate,-24),1) and date_add(add_months(csdate,-12),1) then sum(qty_instore) \
                         over w else 0 end as py_qty_instore \
                        ,case when csdate between date_add(add_months(csdate,-36),1) and date_add(add_months(csdate,-24),1) then sum(qty_total) \
                         over w else 0 end as ppy_qty_total \
                        ,case when csdate between date_add(add_months(csdate,-36),1) and date_add(add_months(csdate,-24),1) then sum(qty_outstore)\
                         over w else 0 end as ppy_qty_outstore \
                        ,case when csdate between date_add(add_months(csdate,-36),1) and date_add(add_months(csdate,-24),1) then sum(qty_instore) \
                         over w else 0 end as ppy_qty_instore \
                        , amt_total\
                        , cost_unit \
                        , '' as cy_gross_sales\
                        , '' as py_gross_sales\
                        , '' as ppy_gross_sales\
                        , (case when csdate between date_add(add_months(csdate,-12),1) and csdate then sum(qty_total) \
                         over w else 0 end)*cost_unit as cy_sales_cost \
                        , (case when csdate between date_add(add_months(csdate,-24),1) and date_add(add_months(csdate,-12),1) then sum(qty_total) \
                         over w else 0 end)*cost_unit as py_sales_cost \
                        ,(case when csdate between date_add(add_months(csdate,-36),1) and date_add(add_months(csdate,-24),1) then sum(qty_total) \
                        over w else 0 end)*cost_unit as ppy_sales_cost \
                        ,'' as cq_unit_sales \
                        , '' as cq_gross_sales \
                        , '' as cq_sales_cost \
                        , '' as ppy_unit_sales \
                        , store_zipcode\
                        , store_state \
                        , store_dc\
                        , store_open_date \
                        , store_close_date\
                        , store_open_flag\
                        , store_lat\
                        , store_long \
                        , store_superhub \
                        , merchandise_division_desc\
                        , merchandise_group_desc \
                        , merchandise_department_desc\
                        , merchandise_class_desc \
                        , merchandise_subclass_desc\
                        , mpog_id \
                        , mpog_description\
                        , mpog_flag\
                        , discontinued_flg\
                        , stocking_location \
                        , part_type \
                        , invoice_year \
                        , invoice_period\
                        , csdate\
                         from dfx \
                         WINDOW w as (partition by invoice_year,invoice_period, invoice_year, csstor, cssku)")

        return agg_store_sku_df


def fps_start_end_info(cdate, nPeriods):
    current_period_info = fp_info(cdate)
    # If cdate is the last day of the period, include that and go back nPeriods -1
    # If not, go back nPeriods.
    start_period_sequence_id = current_period_info.sequence_id - nPeriods + 1
    end_period_sequence_id = current_period_info.sequence_id;
    end_period_info = None
    if (current_period_info.end_date != cdate):
        start_period_sequence_id -= 1
        end_period_sequence_id -= 1
        #end_period_info = current_period_info
    else:
        #end_period_sequence_id -= 1
        pass
    start_period_info = get_pc_df().filter(col('sequence_id') == start_period_sequence_id).first()

    if not end_period_info:
        end_period_info = get_pc_df().filter(col('sequence_id') == end_period_sequence_id).first()

    x=start_period_info.start_date
    #print 'hello'
    #print x

    #y = Row(start_period_info= start_period_info, end_period_info= end_period_info)
    #print y
    return  Row(start_period_info= start_period_info, end_period_info= end_period_info)



def fp_info(cdate):
    # The return value will be a Row and will have the following fields:
    # sequence_id
    # cal_period
    # cal_year
    # end_date # datetime.datetime
    # start_date # datetime.datetime
    # days_in_period
    return  get_pc_df().filter((cdate >= col('start_date')) & (cdate <= col('end_date'))).first()



def fps_start_end_info(cdate, nPeriods):
    current_period_info = fp_info(cdate)
    # If cdate is the last day of the period, include that and go back nPeriods -1
    # If not, go back nPeriods.
    start_period_sequence_id = current_period_info.sequence_id - nPeriods + 1
    end_period_sequence_id = current_period_info.sequence_id;
    end_period_info = None
    if (current_period_info.end_date != cdate):
        start_period_sequence_id -= 1
        end_period_sequence_id -= 1
        #end_period_info = current_period_info
    else:
        #end_period_sequence_id -= 1
        pass
    start_period_info = get_pc_df().filter(col('sequence_id') == start_period_sequence_id).first()

    if not end_period_info:
        end_period_info = get_pc_df().filter(col('sequence_id') == end_period_sequence_id).first()

    x=start_period_info.start_date
    #print 'hello'
    #print x

    #y = Row(start_period_info= start_period_info, end_period_info= end_period_info)
    #print y
    return  Row(start_period_info= start_period_info, end_period_info= end_period_info)


def agg_sales_by_store_sku_rolling_sales(df, as_of_date):
        """ Transformation to compute cy, py, ppy aggregates

        Args:
            df (DataFrame) : DataFrame (sales store,sku level details) to transform.

        Returns:
            df (DataFrame) : Transformed DataFrame

        """
        df.createOrReplaceTempView("df")
        # cy calendar year date range
        max_date = sqlContext.sql('select max(csdate) as fy from df')
        cy_end_date = max_date.first()[0]
        cy_start_date = get_previous_year_start_date(max_date.first()[0])
        print("DEBUG cy_start_date ")
        print(cy_start_date)
        print("DEBUG cy_end_date ")
        print(cy_end_date)

        # py calendear year date range
        py_end_date = cy_start_date+ datetime.timedelta(days=-1)
        py_start_date = get_previous_year_start_date(cy_start_date+ datetime.timedelta(days=-1))
        print("DEBUG py_start_date ")
        print(py_start_date)
        print("DEBUG py_end_date ")
        print(py_end_date)

        # ppy calendear year date range
        ppy_end_date = py_start_date+ datetime.timedelta(days=-1)
        ppy_start_date = get_previous_year_start_date(py_start_date+ datetime.timedelta(days=-1))
        print("DEBUG ppy_start_date ")
        print(ppy_start_date)
        print("DEBUG ppy_end_date ")
        print(ppy_end_date)
        
        # 3py calendear year date range
        pppy_end_date = ppy_start_date+ datetime.timedelta(days=-1)
        pppy_start_date = get_previous_year_start_date(ppy_start_date+ datetime.timedelta(days=-1))
        print("DEBUG 3py_start_date ")
        print(pppy_start_date)
        print("DEBUG 3py_end_date ")
        print(pppy_end_date)

        # cq 3 period date range
        cq_fp_period_info = fps_start_end_info(as_of_date, 3)
        cq_fp_start_date = cq_fp_period_info.start_period_info.start_date
        cq_fp_end_date = cq_fp_period_info.end_period_info.end_date
        print("DEBUG cq_fp_start_date ")
        print(cq_fp_start_date)
        print("DEBUG cq_fp_end_date ")
        print(cq_fp_end_date)

        # cy 13 period date range
        cy_fp_period_info = fps_start_end_info(as_of_date, 13)
        cy_fp_start_date = cy_fp_period_info.start_period_info.start_date
        cy_fp_end_date = cy_fp_period_info.end_period_info.end_date
        print("DEBUG cy_fp_start_date ")
        print(cy_fp_start_date)
        print("DEBUG cy_fp_end_date ")
        print(cy_fp_end_date)

        # py 13 period date range
        py_fp_period_info = fps_start_end_info(as_of_date, 26)
        py_fp_start_date = py_fp_period_info.start_period_info.start_date
        py_fp_end_date = cy_fp_start_date - datetime.timedelta(days=1)
        print("DEBUG py_fp_start_date ")
        print(py_fp_start_date)
        print("DEBUG py_fp_end_date ")
        print(py_fp_end_date)

        # ppy 13 period date range
        ppy_fp_period_info = fps_start_end_info(as_of_date, 39)
        ppy_fp_start_date = ppy_fp_period_info.start_period_info.start_date
        ppy_fp_end_date = py_fp_start_date - datetime.timedelta(days=1)
        print("DEBUG ppy_fp_start_date ")
        print(ppy_fp_start_date)
        print("DEBUG ppy_fp_end_date ")
        print(ppy_fp_end_date)
        
        # 3py 13 period date range
        pppy_fp_period_info = fps_start_end_info(as_of_date, 52)
        pppy_fp_start_date = pppy_fp_period_info.start_period_info.start_date
        pppy_fp_end_date = ppy_fp_start_date - datetime.timedelta(days=1)
        print("DEBUG 3py_fp_start_date ")
        print(pppy_fp_start_date)
        print("DEBUG 3py_fp_end_date ")
        print(pppy_fp_end_date)

        print("DEBUG df.printSchema() ")
        df.printSchema()

        x =  df.groupby( "csstor"
                          , "cssku"
                          ).agg(count("csacct").alias("sources_total")
                                , sum(when(df.csacct != ' ', 1).otherwise(0)).alias("sources_outstore")
                                , sum(when(((df.csacct == ' ') | (df.csacct.isNull())), 1).otherwise(0)).alias("sources_instore")

                                # Year based qty_total
                                , sum(when(((df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csqty")).
                                      otherwise(0)).alias("cyfy_qty_total")

                                # CY (Financial period based current year ==> -13 periods)
                                , sum(when(((df.csdate >= cy_fp_start_date) & (df.csdate <= cy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_cy_unit_sales")
                                , sum(when(((df.csacct != ' ') & (df.csdate >= cy_fp_start_date) & (df.csdate <= cy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_cy_unit_sales_transfer")
                                , sum(when((((df.csacct == ' ') | (df.csacct.isNull())) & (df.csdate >= cy_fp_start_date) & (df.csdate <= cy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_cy_unit_sales_on_hand")
                                , sum(when(((df.csdate >= cy_fp_start_date) & (df.csdate <= cy_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("p_cy_gross_sales")
                                , sum(when(((df.csdate >= cy_fp_start_date) & (df.csdate <= cy_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("p_cy_sales_cost")

                                # PY (Financial period based current year ==> -26 to -14 periods)
                                , sum(when(((df.csdate >= py_fp_start_date) & (df.csdate <= py_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_py_unit_sales")
                                , sum(when(((df.csacct != ' ') & (df.csdate >= py_fp_start_date) & (df.csdate <= py_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_py_unit_sales_transfer")
                                , sum(when((((df.csacct == ' ') | (df.csacct.isNull())) & (df.csdate >= py_fp_start_date) & (df.csdate <= py_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_py_unit_sales_on_hand")
                                , sum(when(((df.csdate >= py_fp_start_date) & (df.csdate <= py_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("p_py_gross_sales")
                                , sum(when(((df.csdate >= py_fp_start_date) & (df.csdate <= py_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("p_py_sales_cost")

                                # PPY (Financial period based current year ==> -39 to -25 periods)
                                , sum(when(((df.csdate >= ppy_fp_start_date) & (df.csdate <= ppy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_ppy_unit_sales")
                                , sum(when(((df.csacct != ' ') & (df.csdate >= ppy_fp_start_date)), col("csqty")).
                                      otherwise(0)).alias("p_ppy_unit_sales_transfer")
                                , sum(when((((df.csacct == ' ') | (df.csacct.isNull())) & (df.csdate >= ppy_fp_start_date) & (df.csdate <= ppy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_ppy_unit_sales_on_hand")
                                , sum(when(((df.csdate >= ppy_fp_start_date) & (df.csdate <= ppy_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("p_ppy_gross_sales")
                                , sum(when(((df.csdate >= ppy_fp_start_date) & (df.csdate <= ppy_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("p_ppy_sales_cost")
                                      
                                # 3PY (Financial period based current year ==> -52 to -39 periods)
                                , sum(when(((df.csdate >= pppy_fp_start_date) & (df.csdate <= pppy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_pppy_unit_sales")
                                , sum(when(((df.csacct != ' ') & (df.csdate >= pppy_fp_start_date)), col("csqty")).
                                      otherwise(0)).alias("p_pppy_unit_sales_transfer")
                                , sum(when((((df.csacct == ' ') | (df.csacct.isNull())) & (df.csdate >= pppy_fp_start_date) & (df.csdate <= pppy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_pppy_unit_sales_on_hand")
                                , sum(when(((df.csdate >= pppy_fp_start_date) & (df.csdate <= pppy_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("p_pppy_gross_sales")
                                , sum(when(((df.csdate >= pppy_fp_start_date) & (df.csdate <= pppy_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("p_pppy_sales_cost")

                                # CQ (Financial period based current quarter)
                                , sum(when(((df.csdate >= cq_fp_start_date) & (df.csdate <= cq_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("cq_unit_sales")
                                , sum(when(((df.csdate >= cq_fp_start_date) & (df.csdate <= cq_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("cq_gross_sales")
                                , sum(when(((df.csdate >= cq_fp_start_date) & (df.csdate <= cq_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("cq_sales_cost")

                                # CY ( based on max csdate from invoice_cshdet current year ==> 1 year back)
                                , sum(when(((df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csqty")).
                                      otherwise(0)).alias("cy_unit_sales")
                                , sum(when(((df.csacct != ' ') & (df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)),col("csqty")).
                                      otherwise(0)).alias("cy_unit_sales_transfer")
                                , sum(when((((df.csacct == ' ') | (df.csacct.isNull())) & (df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csqty")).
                                      otherwise(0)).alias("cy_unit_sales_on_hand")
                                , sum(when(((df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csexpr")).
                                      otherwise(0)).alias("cy_gross_sales")
                                , sum(when(((df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("cy_sales_cost")

                                # PY ( based on max csdate from invoice_cshdet current year ==> 2 years back)
                                , sum(when(((df.csdate >= py_start_date) & (df.csdate <= py_end_date)), col("csqty")).
                                      otherwise(0)).alias("py_unit_sales")
                                , sum(when(((df.csacct != ' ') & (df.csdate >= py_start_date) & (df.csdate <= py_end_date)), col("csqty")).
                                      otherwise(0)).alias("py_unit_sales_transfer")
                                , sum(when((((df.csacct == ' ') | (df.csacct.isNull())) & ( df.csdate >= py_start_date) & (df.csdate <= py_end_date)), col("csqty")).
                                      otherwise(0)).alias("py_unit_sales_on_hand")
                                ,sum(when(((df.csdate >= py_start_date) & (df.csdate <= py_end_date)), col("csexpr")).
                                      otherwise(0)).alias("py_gross_sales")
                                , sum(when(((df.csdate >= py_start_date) & (df.csdate <= py_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("py_sales_cost")

                                # PPY (based on max csdate from invoice_cshdet current year ==> 3 years back)
                                , sum(when(((df.csdate >= ppy_start_date) & (df.csdate <= ppy_end_date)), col("csqty")).
                                      otherwise(0)).alias("ppy_unit_sales")
                                , sum(when(((df.csacct != ' ') & (df.csdate >= ppy_start_date) & (df.csdate <= ppy_end_date)), col("csqty")).
                                      otherwise(0)).alias("ppy_unit_sales_transfer")
                                , sum(when((((df.csacct == ' ') | (df.csacct.isNull())) & (df.csdate >= ppy_start_date) & (df.csdate <= ppy_end_date)), col("csqty")).
                                      otherwise(0)).alias("ppy_unit_sales_on_hand")
                                ,sum(when(((df.csdate >= ppy_start_date) & (df.csdate <= ppy_end_date)), col("csexpr")).
                                      otherwise(0)).alias("ppy_gross_sales")
                                , sum(when(((df.csdate >= ppy_start_date) & (df.csdate <= ppy_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("ppy_sales_cost")
                                      
                                # 3PY (based on max csdate from invoice_cshdet current year ==> 3 years back)
                                , sum(when(((df.csdate >= pppy_start_date) & (df.csdate <= pppy_end_date)), col("csqty")).
                                      otherwise(0)).alias("pppy_unit_sales")
                                , sum(when(((df.csacct != ' ') & (df.csdate >= pppy_start_date) & (df.csdate <= pppy_end_date)), col("csqty")).
                                      otherwise(0)).alias("pppy_unit_sales_transfer")
                                , sum(when((((df.csacct == ' ') | (df.csacct.isNull())) & (df.csdate >= pppy_start_date) & (df.csdate <= pppy_end_date)), col("csqty")).
                                      otherwise(0)).alias("pppy_unit_sales_on_hand")
                                ,sum(when(((df.csdate >= pppy_start_date) & (df.csdate <= pppy_end_date)), col("csexpr")).
                                      otherwise(0)).alias("pppy_gross_sales")
                                , sum(when(((df.csdate >= pppy_start_date) & (df.csdate <= pppy_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("pppy_sales_cost")

                                , sum(when(df.csacct != ' ', col("csqty")).otherwise(0)).alias("qty_outstore")
                                , sum(when(df.csacct == ' ', col("csqty")).otherwise(0)).alias("qty_instore")
                                , sum(when(df.csdtyp == '11', -1 * col("csqty")).otherwise(0)).alias("qty_returns")
                                , sum("csexpr").alias("amt_total")
                                , avg("cscost").alias("cost_unit")
                                , lit(as_of_date).alias("as_of_date")
                                )

        # x.withColumn('as_of_date', lit(as_of_date))

        return x



def agg_sales_sku_rolling_sales(df, as_of_date):
        """ Transformation to compute cy, py, ppy aggregates

        Args:
            df (DataFrame) : DataFrame (sales store,sku level details) to transform.

        Returns:
            df (DataFrame) : Transformed DataFrame

        """
        df.registerTempTable("df")
        # cy calendar year date range
        max_date = sqlContext.sql('select max(csdate) as fy from df')
        cy_end_date = max_date.first()[0]
        cy_start_date = get_previous_year_start_date(max_date.first()[0])
        print("DEBUG cy_start_date ")
        print(cy_start_date)
        print("DEBUG cy_end_date ")
        print(cy_end_date)

        # py calendear year date range
        py_end_date = cy_start_date+ datetime.timedelta(days=-1)
        py_start_date = get_previous_year_start_date(cy_start_date+ datetime.timedelta(days=-1))
        print("DEBUG py_start_date ")
        print(py_start_date)
        print("DEBUG py_end_date ")
        print(py_end_date)

        # ppy calendear year date range
        ppy_end_date = py_start_date+ datetime.timedelta(days=-1)
        ppy_start_date = get_previous_year_start_date(py_start_date+ datetime.timedelta(days=-1))
        print("DEBUG ppy_start_date ")
        print(ppy_start_date)
        print("DEBUG ppy_end_date ")
        print(ppy_end_date)
        
        # 3py calendear year date range
        pppy_end_date = ppy_start_date+ datetime.timedelta(days=-1)
        pppy_start_date = get_previous_year_start_date(ppy_start_date+ datetime.timedelta(days=-1))
        print("DEBUG 3py_start_date ")
        print(pppy_start_date)
        print("DEBUG 3py_end_date ")
        print(pppy_end_date)

        # cq 3 period date range
        cq_fp_period_info = fps_start_end_info(as_of_date, 3)
        cq_fp_start_date = cq_fp_period_info.start_period_info.start_date
        cq_fp_end_date = cq_fp_period_info.end_period_info.end_date
        print("DEBUG cq_fp_start_date ")
        print(cq_fp_start_date)
        print("DEBUG cq_fp_end_date ")
        print(cq_fp_end_date)

        # cy 13 period date range
        cy_fp_period_info = fps_start_end_info(as_of_date, 13)
        cy_fp_start_date = cy_fp_period_info.start_period_info.start_date
        cy_fp_end_date = cy_fp_period_info.end_period_info.end_date
        print("DEBUG cy_fp_start_date ")
        print(cy_fp_start_date)
        print("DEBUG cy_fp_end_date ")
        print(cy_fp_end_date)

        # py 13 period date range
        py_fp_period_info = fps_start_end_info(as_of_date, 26)
        py_fp_start_date = py_fp_period_info.start_period_info.start_date
        py_fp_end_date = cy_fp_start_date - datetime.timedelta(days=1)
        print("DEBUG py_fp_start_date ")
        print(py_fp_start_date)
        print("DEBUG py_fp_end_date ")
        print(py_fp_end_date)

        # ppy 13 period date range
        ppy_fp_period_info = fps_start_end_info(as_of_date, 39)
        ppy_fp_start_date = ppy_fp_period_info.start_period_info.start_date
        ppy_fp_end_date = py_fp_start_date - datetime.timedelta(days=1)
        print("DEBUG ppy_fp_start_date ")
        print(ppy_fp_start_date)
        print("DEBUG ppy_fp_end_date ")
        print(ppy_fp_end_date)
        
        # 3py 13 period date range
        pppy_fp_period_info = fps_start_end_info(as_of_date, 52)
        pppy_fp_start_date = pppy_fp_period_info.start_period_info.start_date
        pppy_fp_end_date = ppy_fp_start_date - datetime.timedelta(days=1)
        print("DEBUG 3py_fp_start_date ")
        print(pppy_fp_start_date)
        print("DEBUG 3py_fp_end_date ")
        print(pppy_fp_end_date)

        print("DEBUG df.printSchema() ")
        df.printSchema()

        y =  df.groupby( "cssku"
                           ).agg(count("csacct").alias("sources_total")
                                , sum(when(df.csacct != ' ', 1).otherwise(0)).alias("sources_outstore")
                                , sum(when(((df.csacct == ' ') | (df.csacct.isNull())), 1).otherwise(0)).alias("sources_instore")

                                # Year based qty_total
                                , sum(when(((df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csqty")).
                                      otherwise(0)).alias("cyfy_qty_total")

                                # CY (Financial period based current year ==> -13 periods)
                                , sum(when(((df.csdate >= cy_fp_start_date) & (df.csdate <= cy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_cy_unit_sales")
                                , sum(when(((df.csdate >= cy_fp_start_date) & (df.csdate <= cy_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("p_cy_gross_sales")
                                , sum(when(((df.csdate >= cy_fp_start_date) & (df.csdate <= cy_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("p_cy_sales_cost")

                                # PY (Financial period based current year ==> -26 to -14 periods)
                                , sum(when(((df.csdate >= py_fp_start_date) & (df.csdate <= py_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_py_unit_sales")
                                , sum(when(((df.csdate >= py_fp_start_date) & (df.csdate <= py_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("p_py_gross_sales")
                                , sum(when(((df.csdate >= py_fp_start_date) & (df.csdate <= py_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("p_py_sales_cost")

                                # PPY (Financial period based current year ==> -39 to -25 periods)
                                , sum(when(((df.csdate >= ppy_fp_start_date) & (df.csdate <= ppy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_ppy_unit_sales")
                                , sum(when(((df.csdate >= ppy_fp_start_date) & (df.csdate <= ppy_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("p_ppy_gross_sales")
                                , sum(when(((df.csdate >= ppy_fp_start_date) & (df.csdate <= ppy_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("p_ppy_sales_cost")
                                      
                                # 3PY (Financial period based current year ==> -52 to -39 periods)
                                , sum(when(((df.csdate >= pppy_fp_start_date) & (df.csdate <= pppy_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("p_3py_unit_sales")
                                , sum(when(((df.csdate >= pppy_fp_start_date) & (df.csdate <= pppy_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("p_3py_gross_sales")
                                , sum(when(((df.csdate >= pppy_fp_start_date) & (df.csdate <= pppy_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("p_3py_sales_cost")

                                # CQ (Financial period based current quarter)
                                , sum(when(((df.csdate >= cq_fp_start_date) & (df.csdate <= cq_fp_end_date)), col("csqty")).
                                      otherwise(0)).alias("cq_unit_sales")
                                , sum(when(((df.csdate >= cq_fp_start_date) & (df.csdate <= cq_fp_end_date)), col("csexpr")).
                                      otherwise(0)).alias("cq_gross_sales")
                                , sum(when(((df.csdate >= cq_fp_start_date) & (df.csdate <= cq_fp_end_date)), col("csqty") * col("cscost")).
                                      otherwise(0)).alias("cq_sales_cost")

                                 # CY ( based on max csdate from invoice_cshdet current year ==> 1 year back)
                                 , sum(when(((df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csqty")).
                                       otherwise(0)).alias("cy_unit_sales")
                                 , sum(when(((df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csexpr")).
                                       otherwise(0)).alias("cy_gross_sales")
                                 , sum(when(((df.csdate >= cy_start_date) & (df.csdate <= cy_end_date)), col("csqty") * col("cscost")).
                                       otherwise(0)).alias("cy_sales_cost")

                                 # PY ( based on max csdate from invoice_cshdet current year ==> 2 years back)
                                 , sum(when(((df.csdate >= py_start_date) & (df.csdate <= py_end_date)), col("csqty")).
                                       otherwise(0)).alias("py_unit_sales")
                                 , sum(when(((df.csdate >= py_start_date) & (df.csdate <= py_end_date)), col("csexpr")).
                                       otherwise(0)).alias("py_gross_sales")
                                 , sum(when(((df.csdate >= py_start_date) & (df.csdate <= py_end_date)), col("csqty") * col("cscost")).
                                       otherwise(0)).alias("py_sales_cost")

                                 # PPY (based on max csdate from invoice_cshdet current year ==> 3 years back)
                                 ,sum(when(((df.csdate >= ppy_start_date) & (df.csdate <= ppy_end_date)), col("csqty")).
                                       otherwise(0)).alias("ppy_unit_sales")
                                 ,sum(when(((df.csdate >= ppy_start_date) & (df.csdate <= ppy_end_date)), col("csexpr")).
                                       otherwise(0)).alias("ppy_gross_sales")
                                 , sum(when(((df.csdate >= ppy_start_date) & (df.csdate <= ppy_end_date)), col("csqty") * col("cscost")).
                                       otherwise(0)).alias("ppy_sales_cost")
                                       
                                # 3PY (based on max csdate from invoice_cshdet current year ==> 3 years back)
                                 ,sum(when(((df.csdate >= pppy_start_date) & (df.csdate <= pppy_end_date)), col("csqty")).
                                       otherwise(0)).alias("3py_unit_sales")
                                 ,sum(when(((df.csdate >= pppy_start_date) & (df.csdate <= pppy_end_date)), col("csexpr")).
                                       otherwise(0)).alias("3py_gross_sales")
                                 , sum(when(((df.csdate >= pppy_start_date) & (df.csdate <= pppy_end_date)), col("csqty") * col("cscost")).
                                       otherwise(0)).alias("3py_sales_cost")

                                , sum(when(df.csacct != ' ', col("csqty")).otherwise(0)).alias("qty_outstore")
                                , sum(when(df.csacct == ' ', col("csqty")).otherwise(0)).alias("qty_instore")
                                , sum(when(df.csdtyp == '11', -1 * col("csqty")).otherwise(0)).alias("qty_returns")
                                , sum("csexpr").alias("amt_total")
                                , avg("cscost").alias("cost_unit")
                                , lit(as_of_date).alias("as_of_date")
                                )

        # x.withColumn('as_of_date', lit(as_of_date))

        return y


def persist_agg_sales_by_sku(df, method):
        """ Persist aggregates sales by store,sku to file.

        Args:
            rdd (DataFrame) : DataFrame (sales store,sku level details) to transform.

        """
        df.registerTempTable("sales_sku_df")
        agg_sales_by_sku = sqlContext.sql(" select cssku as sku_number \
                                                 , sources_total \
                                                 , sources_outstore \
                                                 , sources_instore \
                                                 , cyfy_qty_total \
                                                 , p_cy_unit_sales \
                                                 , p_cy_gross_sales \
                                                 , p_cy_sales_cost \
                                                 , p_py_unit_sales \
                                                 , p_py_gross_sales \
                                                 , p_py_sales_cost \
                                                 , p_ppy_unit_sales \
                                                 , p_ppy_gross_sales \
                                                 , p_ppy_sales_cost \
                                                 , cq_unit_sales \
                                                 , cq_gross_sales \
                                                 , cq_sales_cost \
                                                 , cy_unit_sales \
                                                 , cy_gross_sales \
                                                 , cy_sales_cost \
                                                 , py_unit_sales\
                                                 , py_gross_sales \
                                                 , py_sales_cost \
                                                 , ppy_unit_sales\
                                                 , ppy_gross_sales \
                                                 , ppy_sales_cost \
                                                 , qty_outstore \
                                                 , qty_instore \
                                                 , qty_returns \
                                                 , amt_total \
                                                 , cost_unit \
                                                 , as_of_date from sales_sku_df")

        return agg_sales_by_sku


def persist_agg_sales_by_store_sku_rolling(df, method):
        """ Persist aggregates sales by store,sku to file.

        Args:
            rdd (DataFrame) : DataFrame (sales store,sku level details) to transform.

        """
        df.registerTempTable("sales_store_sku_df")
        agg_sales_by_store_sku = sqlContext.sql(" select csstor as store_number, cssku as sku_number \
                                                 , sources_total \
                                                 , sources_outstore \
                                                 , sources_instore \
                                                 , cyfy_qty_total \
                                                 , p_cy_unit_sales \
                                                 , p_cy_unit_sales_transfer \
                                                 , p_cy_unit_sales_on_hand \
                                                 , p_cy_gross_sales \
                                                 , p_cy_sales_cost \
                                                 , p_py_unit_sales \
                                                 , p_py_unit_sales_transfer \
                                                 , p_py_unit_sales_on_hand \
                                                 , p_py_gross_sales \
                                                 , p_py_sales_cost \
                                                 , p_ppy_unit_sales \
                                                 , p_ppy_unit_sales_transfer \
                                                 , p_ppy_unit_sales_on_hand \
                                                 , p_ppy_gross_sales \
                                                 , p_ppy_sales_cost \
                                                 , cq_unit_sales \
                                                 , cq_gross_sales \
                                                 , cq_sales_cost \
                                                 , cy_unit_sales \
                                                 , cy_unit_sales_transfer \
                                                 , cy_unit_sales_on_hand \
                                                 , cy_gross_sales \
                                                 , cy_sales_cost \
                                                 , py_unit_sales\
                                                 , py_unit_sales_transfer \
                                                 , py_unit_sales_on_hand \
                                                 , py_gross_sales \
                                                 , py_sales_cost \
                                                 , ppy_unit_sales\
                                                 , ppy_unit_sales_transfer \
                                                 , ppy_unit_sales_on_hand \
                                                 , ppy_gross_sales \
                                                 , ppy_sales_cost \
                                                 , qty_outstore \
                                                 , qty_instore \
                                                 , qty_returns \
                                                 , amt_total \
                                                 , cost_unit \
                                                 , as_of_date from sales_store_sku_df")


        return agg_sales_by_store_sku