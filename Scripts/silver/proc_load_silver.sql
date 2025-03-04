/*
===============================================================================
Stored Procedure: Load Silver Layer
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
	===============================================================================
*/



create or alter procedure silver.load_layer as
Begin
declare @start_time datetime, @end_time datetime,@batch_start_time datetime, @batch_end_time datetime;
begin try

set @batch_start_time = GETDATE();
print '===================================='
print 'Load Procedure start'
print '===================================='
set @start_time = GETDATE();
print '>> Truncating table : silver.crm_cust_info'
truncate table silver.crm_cust_info
print '>> Inserting table : silver.crm_cust_info'
insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
select 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
case when upper(Trim(cst_marital_status)) = 'M' then 'Married'
	when upper(trim(cst_marital_status)) = 'S' then 'Single'
	Else 'n/a' 
	end as cst_marital_status,

case when upper(trim(cst_gndr)) = 'M' then 'Male'
	when upper(trim(cst_gndr)) = 'F' then 'Female'
	Else 'n/a' 
	end as cst_gndr,
cst_create_date
from (
select *,
row_number()
over(partition by cst_id order by cst_create_date desc) as flag_last
from 
bronze.crm_cust_info
where cst_id is not null) t
where flag_last = 1;

set @end_time = GETDATE()
print '>> Load Duration :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '>>------------------------------'






set @start_time = GETDATE();
print '>> Truncating table : silver.crm_prd_info'
truncate table silver.crm_prd_info
print '>> Inserting table : silver.crm_prd_info'
insert into silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
select 
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
case 
	when UPPER(trim(prd_line)) = 'M' then 'Mountain'
	when UPPER(trim(prd_line)) = 'S' then 'Other Sales'
	when UPPER(trim(prd_line)) = 'R' then 'Road'
	Else 'n/a'
end as prd_line,
cast(prd_start_dt AS date) as prd_start_dt,
cast(
	LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 AS date) as prd_end_dt
 from bronze.crm_prd_info
set @end_time = GETDATE()
print '>> Load Duration :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '>>------------------------------'





 set @start_time = GETDATE();
 print '>> Truncating table : silver.crm_sales_details'
truncate table silver.crm_sales_details
print '>> Inserting table : silver.crm_sales_details'
insert into silver.crm_sales_details(
  sls_ord_num,
 sls_prd_key,
 sls_cust_id,
 sls_order_dt,
 sls_ship_dt,
 sls_due_dt,
 sls_sales,
 sls_quantity,
 sls_price)
  select 
 sls_ord_num,
 sls_prd_key,
 sls_cust_id,
 case 
 when sls_order_dt <= 0 
 or sls_order_dt is null 
 or LEN(sls_order_dt) != 8 then null
 else cast(cast(sls_order_dt as varchar) As date)
 end as sls_order_dt,
 case 
 when sls_ship_dt <= 0 
 or sls_ship_dt is null 
 or LEN(sls_ship_dt) != 8 then null
 else cast(cast(sls_ship_dt as varchar) As date)
 end as sls_ship_dt,
  case 
 when sls_due_dt <= 0 
 or sls_due_dt is null 
 or LEN(sls_due_dt) != 8 then null
 else cast(cast(sls_due_dt as varchar) As date)
 end as sls_due_dt,
case
 when sls_sales != sls_quantity* abs(sls_price) or sls_sales <=0 or sls_sales is null
 then  sls_quantity * abs(sls_price)
 else sls_sales
 end as sls_sales,
  sls_quantity as  sls_quantity,
 case 
 when sls_price IS NULL or sls_price <=0
 then sls_sales/nullif(sls_quantity,0)
 else sls_price
 end as sls_price
  from bronze.crm_sales_details
   set @end_time = GETDATE()
print '>> Load Duration :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '>>------------------------------'






set @start_time = GETDATE();
print '>> Truncating table : silver.erp_cust_az12'
truncate table silver.erp_cust_az12
print '>> Inserting table : silver.erp_cust_az12'
  insert into silver.erp_cust_az12(cid,bdate,gen)
  select 
  case
  when CID like 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
  ELSE CID
  END AS CID,
  CASE WHEN BDATE>GETDATE() THEN NULL
  ELSE BDATE
  END as BDATE,
  CASE
  WHEN UPPER(TRIM(GEN)) IN	('F','Female') then 'Female'
  WHEN UPPER(TRIM(GEN)) IN	('M','Male') then 'Male'
  else 'n/a'
  end as GEN
  from bronze.erp_cust_az12
  set @end_time = GETDATE()
print '>> Load Duration :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '>>------------------------------'




set @start_time = GETDATE();
print '>> Truncating table : silver.erp_loc_a101'
truncate table silver.erp_loc_a101
print '>> Inserting table : silver.erp_loc_a101'
INSERT INTO silver.erp_loc_a101(cid,cntry)
select
REPLACE(CID,'-','') AS CID,
CASE
WHEN trim(CNTRY) in ('US','USA') THEN 'United States'
when trim(CNTRY) = 'DE' THEN 'Germany'
when trim(CNTRY) = '' or trim(CNTRY) is null THEN 'n/a'
else TRIM(CNTRY)
end as CNTRY
FROM bronze.erp_loc_a101
set @end_time = GETDATE()
print '>> Load Duration :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '>>------------------------------'




set @start_time = GETDATE();
print '>> Truncating table : silver.erp_px_cat_g1v2'
truncate table silver.erp_px_cat_g1v2
print '>> Inserting table : silver.erp_px_cat_g1v2'
insert into silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance)
select ID, 
CAT,
SUBCAT,
MAINTENANCE
from bronze.erp_px_cat_g1v2
set @end_time = GETDATE()
print '>> Load Duration :'+ cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '>>------------------------------'

set @batch_end_time = GETDATE();
print '===================================='
print 'Load Procedure Completed'
print '>> Load Duration :'+ cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' Seconds'
print '===================================='
end try
    BEGIN CATCH
        PRINT 'Error Occurred: ' + ERROR_MESSAGE();
    END CATCH

end;


exec silver.load_layer
