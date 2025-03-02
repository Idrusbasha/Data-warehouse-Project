	create or alter procedure bronze.load_bronze as
	begin
	declare @start_time datetime, @end_time datetime
		print '--------------------------------------------------'
		print 'Loading CRM Tables'
		print '--------------------------------------------------'
		set @start_time = GETDATE();
		print 'Deleting Cust_info Tables'
		truncate table bronze.crm_cust_info;
	
		print 'Loading Cust_info Tables'
		bulk insert bronze.crm_cust_info
		from 'C:\Users\idrus\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(firstrow = 2,
			fieldterminator = ',',
			tablock);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time)as nvarchar) +'seconds';

		set @start_time = GETDATE();
		print 'Deleting prd_info Tables'
		truncate table bronze.crm_prd_info;
		
		print 'Loading prd_info Tables'
		bulk insert bronze.crm_prd_info
		from 'C:\Users\idrus\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(firstrow = 2,
			fieldterminator = ',',
			tablock);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time)as nvarchar) +'seconds';


		set @start_time = GETDATE();
		print 'Deleting sales_details Tables'
		truncate table bronze.crm_sales_details;
	
		print 'Loading sales_details Tables'
		bulk insert bronze.crm_sales_details
		from 'C:\Users\idrus\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(firstrow = 2,
			fieldterminator = ',',
			tablock);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time)as nvarchar) +'seconds';

		print '--------------------------------------------------'
		print 'Loading ERP Tables'
		print '--------------------------------------------------'
		set @start_time = GETDATE();
		print 'Deleting cust_az12 Tables'
		truncate table bronze.erp_cust_az12;
	
		print 'Loading cust_az12 Tables'
			bulk insert bronze.erp_cust_az12
		from 'C:\Users\idrus\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(firstrow = 2,
			fieldterminator = ',',
			tablock);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time)as nvarchar) +'seconds';
		
		set @start_time = GETDATE();
		print 'Deleting loc_a101 Tables'
		truncate table bronze.erp_loc_a101;
		
		print 'Loading loc_a101 Tables'	

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\idrus\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(firstrow = 2,
			fieldterminator = ',',
			tablock);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time)as nvarchar) +'seconds';
	
		set @start_time = GETDATE();
		print 'Deleting px_cat_g1v2 Tables'
		truncate table bronze.erp_px_cat_g1v2;
		
		print 'Loading px_cat_g1v2 Tables'	
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\idrus\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(firstrow = 2,
			fieldterminator = ',',
			tablock);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(Datediff(second, @start_time, @end_time)as nvarchar) +'seconds';
	end
