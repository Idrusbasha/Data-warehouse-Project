-- =============================================================================
-- Create Dimension Table: gold.dim_customers
-- =============================================================================
create view gold.dim_customers as
select 
row_number() over (order by ca.cst_key) as customer_key,
ca.cst_id as customer_id,
ca.cst_key as customer_number,
ca.cst_firstname as first_name,
ca.cst_lastname as last_name,
case when ca.cst_gndr != 'n/a' then ca.cst_gndr
else coalesce(cu.gen,'n/a')
end AS gender,
ca.cst_marital_status as marital_status,
ci.cntry as country,
ca.cst_create_date as create_date,
cu.bdate as birth_date
from
silver.crm_cust_info ca
left join silver.erp_cust_az12 cu
on ca.cst_key = cu.cid
left join silver.erp_loc_a101 ci
on ca.cst_key = ci.cid

-- =============================================================================
-- Create Dimension Table: gold.dim_products
-- =============================================================================

create view gold.dim_products as
select 
ROW_NUMBER() over (order by pt.prd_start_dt, pt.prd_key) AS product_key,
pt.prd_id as product_id,
pt.prd_key as product_number,
pt.prd_nm as product_name,
pt.cat_id as category_id,
ct.cat as category,
ct.subcat as sub_category,
ct.maintenance as maintenance,
pt.prd_cost as cost,
pt.prd_line as product_line,
pt.prd_start_dt as product_start_date
from 
silver.crm_prd_info pt
left join silver.erp_px_cat_g1v2 ct
on pt.cat_id = ct.id
where pt.prd_end_dt is null 

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

create view gold.fact_sales as
select
sd.sls_ord_num as order_number,
pd.product_key,
ca.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_customers ca
on sd.sls_cust_id = ca.customer_id 
left join gold.dim_products pd
on sd.sls_prd_key = pd.product_number
