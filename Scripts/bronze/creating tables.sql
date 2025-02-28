
-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
This file will help you too how 'to create tables in database'
  **warning**
 'If already tables exist in your database it will delete and create new table' **incase tables 'are' empty**  
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
if OBJECT_ID('bronze_crm_cust_info','U') is not null
	drop table bronze_crm_cust_info;
go
create table bronze_crm_cust_info(
cst_id int,
cst_key	Nvarchar(50),
cst_firstname	Nvarchar(50),
cst_lastname	Nvarchar(50),
cst_marital_status	Nvarchar(50),
cst_gndr	Nvarchar(50),
cst_create_date date);
go
if OBJECT_ID('bronze_crm_prd_info','U') is not null
	drop table bronze_crm_prd_info;
go
create table bronze_crm_prd_info(
prd_id int,
prd_key Nvarchar(100),
prd_nm	Nvarchar(100),
prd_cost int,
prd_line Nvarchar(50),
prd_start_dt date,
prd_end_dt date);

go
if OBJECT_ID('bronze_crm_sales_info','U') is not null
	drop table bronze_crm_sales_info;
go
create table bronze_crm_sales_info(
sls_ord_num Nvarchar(50),
sls_prd_key	Nvarchar(100),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int);
go
if OBJECT_ID('bronze_erp_cust_az12','U') is not null
	drop table bronze_erp_cust_az12;
go
create table bronze_erp_cust_az12(
CID Nvarchar(50),
BDATE date,
GEN	Nvarchar(50));
go
if OBJECT_ID('bronze_erp_loc_a101','U') is not null
	drop table bronze_erp_loc_a101;
go
create table bronze_erp_loc_a101(
CID Nvarchar(50),
CNTRY Nvarchar(50));
go
if OBJECT_ID('bronze_erp_px_cat_g1v2','U') is not null
	drop table bronze_erp_px_cat_g1v2;
go
create table bronze_erp_px_cat_g1v2(
ID Nvarchar(50),
CAT Nvarchar(50),
SUBCAT Nvarchar(50),
MAINTENANCE Nvarchar(50));
