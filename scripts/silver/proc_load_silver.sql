/*
==============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==============================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None

    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;

==============================================================================
*/


create or alter procedure silver.load_silver as
begin
	
	declare @start_time datetime, @end_time datetime, @start_batch_time datetime, @end_batch_time datetime;
		begin try
			set @start_batch_time = getdate();
			print '==============================';
			print 'loading the silver layer';
			print '==============================';

			print '--------------------------------';
			print 'CRM table';
			print '--------------------------------';

	set @start_time = getdate()
	print '>> Truncating table silver.crm_cust_info'
	truncate table silver.crm_cust_info;
	print '>> inserting data into silver.crm_cust_info'
	insert into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date
	)
	select
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case 
			when upper(trim(cst_material_status)) = 'S' then 'Single'
			when upper(trim(cst_material_status)) = 'M' then 'Married'
			else 'n/a'
		end as cst_material_status,
		case
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end as cst_gndr,
		cst_create_date
	from
	(
		select*,
			ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as latest
		from bronze.crm_cust_info) as duplicate
	where latest = 1;

			set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'


	set @start_time = getdate()
	print '>> Truncating table silver.crm_prd_info'
	truncate table silver.crm_prd_info;
	print '>> inserting data into silver.crm_prd_info'
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
		SUBSTRING(prd_key , 7, len(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		case  UPPER(trim(prd_line)) 
			when 'M' then 'Mountain' 
			when 'R' then 'Road'
			when 'S' then 'other sales'
			when 'T' then 'Touring'
			else 'n/a'
		end as prd_line,
		prd_start_dt,
		dateadd(DAY,-1,
		lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt
	from bronze.crm_prd_info;

	set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'


	set @start_time = getdate()
	print '>> Truncating table silver.crm_sales_details'
	truncate table silver.crm_sales_details;
	print '>> inserting data into silver.crm_sales_details'
	insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price	)

	select
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 
			then null
		else cast(cast(sls_order_dt as varchar(50)) as date) 
		end as sls_order_dt,

		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 
			then null
		else cast(cast(sls_ship_dt as varchar(50)) as date) 
		end as sls_ship_dt,

		case when sls_due_dt = 0 or len(sls_due_dt) != 8 
			then null 
		else cast(cast(sls_due_dt  as varchar) as date)
		end as sls_due_dt,

		case when sls_sales is null or sls_sales<=0 or sls_sales != sls_quantity * abs(sls_price)
			then sls_quantity*abs(sls_price)
		else sls_sales
		end as sls_sales,

		sls_quantity ,
		case when sls_price is null or sls_price<=0 
			then sls_sales/nullif(sls_quantity,0)
		else sls_price
		end as sls_price

	from bronze.crm_sales_details;

	set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'

			print '--------------------------------';
			print 'ERP table';
			print '--------------------------------';

	set @start_time = getdate()
	print '>> Truncating table silver.erp_cust_az12'
	truncate table silver.erp_cust_az12;
	print '>> inserting data into silver.erp_cust_az12'
	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen)

	select
		case when cid like 'NAS%' then substring(cid,4,len(cid))
			else cid
		end as cid,

		case when bdate> getdate() then null
			else bdate
		end as bdate,

		case when upper(trim(gen)) in ('F' , 'Female') then 'Female'
			 when upper(trim(gen)) in ('M' , 'Male') then 'Male'
			 else 'n/a'
		end as gen

	from bronze.erp_cust_az12;

	set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'

	set @start_time = getdate()
	print '>> Truncating table silver.erp_loc_a101'
	truncate table silver.erp_loc_a101;
	print '>> inserting data into silver.erp_loc_a101'
	insert into silver.erp_loc_a101(
	cid,
	cntry)
	select
	replace(cid,'-','') as cid,
	case when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US','USA') then 'United state'
		when trim(cntry) = '' or trim(cntry)  is null then 'n/a'
		else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101;
	set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'


	set @start_time = getdate()
	print '>> Truncating table silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2;
	print '>> inserting data into silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
	select
		id,
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2;
	set @end_time = getdate();
			print '>> Load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ ' seconds'

	set @end_batch_time = getdate()
	print '============================';
			print ' Loading batch time completed';
			print '>> Load duration of silver layer: '+ cast(datediff(second,@start_batch_time,@end_batch_time) as nvarchar) + ' seconds';
	end try

	begin catch
		print '==========================================';
		print ' Error occured during loading the silver layer';
		print ' Error message'+ error_message();
		print ' Error message'+ cast (error_message() as nvarchar);
		print ' Error message'+ cast (error_message() as nvarchar);
		print '==========================================';
	end caTCH

end
