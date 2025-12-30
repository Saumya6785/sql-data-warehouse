/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin 
		declare @start_time datetime, @end_time datetime, @start_batch_time datetime, @end_batch_time datetime;
		begin try
			set @start_batch_time = getdate();
			print '==============================';
			print 'loading the broze layer';
			print '==============================';

			print '--------------------------------';
			print 'CRM table';
			print '--------------------------------';

			set @start_time = getdate();
			print '>>Truncating the table:bronze.crm_cust_info';
			truncate table bronze.crm_cust_info;
			print '>> loading the table bronze.crm_cust_info';
			bulk insert bronze.crm_cust_info
				from "C:\Users\Saumya1\Downloads\cust_info\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
				with (
					firstrow =2,
					fieldterminator = ',',
					tablock 
				);
			set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'

			set @start_time = getdate();
			print '>>Truncating the table:bronze.crm_prd_info';
			truncate table bronze.crm_prd_info;
			print '>> loading the table bronze.crm_prd_info';
			bulk insert bronze.crm_prd_info
				from "C:\Users\Saumya1\Downloads\cust_info\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
				with (
					firstrow =2,
					fieldterminator = ',',
					tablock 
				);
			set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'

			set @start_time = getdate();
			print '>>Truncating the table:bronze.crm_sales_details';
			truncate table bronze.crm_sales_details;
			print '>> loading the table bronze.crm_sales_details';
			bulk insert bronze.crm_sales_details
				from "C:\Users\Saumya1\Downloads\cust_info\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
				with (
					firstrow =2,
					fieldterminator = ',',
					tablock 
				);
			set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'
	
			print '--------------------------------';
			print 'ERP table';
			print '--------------------------------';

			set @start_time = getdate();
			print '>>Truncating the table:bronze.erp_cust_az12';
			truncate table bronze.erp_cust_az12;
			print '>> loading the table bronze.erp_cust_az12';
			bulk insert bronze.erp_cust_az12
				from "C:\Users\Saumya1\Downloads\cust_info\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv"
				with (
					firstrow =2,
					fieldterminator = ',',
					tablock 
				);
			set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'

			set @start_time = getdate();
			print '>>Truncating the table:bronze.erp_loc_a101';
			truncate table bronze.erp_loc_a101;
			print '>> loading the table bronze.erp_loc_a101';
			bulk insert bronze.erp_loc_a101
				from "C:\Users\Saumya1\Downloads\cust_info\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv"
				with (
					firstrow =2,
					fieldterminator = ',',
					tablock 
				);
			set @end_time = getdate();
			print '>> Load duration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'

			set @start_time = getdate();
			print '>>Truncating the table:bronze.erp_px_cat_g1v2';
			truncate table bronze.erp_px_cat_g1v2;
			print '>> loading the table bronze.erp_px_cat_g1v2';
			bulk insert bronze.erp_px_cat_g1v2
				from "C:\Users\Saumya1\Downloads\cust_info\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv"
				with (
					firstrow =2,
					fieldterminator = ',',
					tablock 
				);
			set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
			

			set @end_batch_time = getdate();
			print '============================';
			print ' Loading batch time completed';
			print '>> Load duration of broze layer: '+ cast(datediff(second,@start_batch_time,@end_batch_time) as nvarchar) + ' seconds';
	end try
	begin catch
		print '==========================================';
		print ' Error occured during loading the broze layer';
		print ' Error message'+ error_message();
		print ' Error message'+ cast (error_message() as nvarchar);
		print ' Error message'+ cast (error_message() as nvarchar);
		print '==========================================';
	end caTCH
end
