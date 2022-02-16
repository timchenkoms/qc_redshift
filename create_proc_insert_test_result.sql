SET search_path TO {{currentschema}};
	
create or replace procedure proc_insert_test_result(
    p_test_type varchar(50),
	p_test_group varchar(200),
	p_test_action varchar(100),
	p_test_description varchar(1000),
    p_test_time timestamp,
	p_starttime timestamp,
    p_order_run varchar(50),
    p_schema_name varchar(100),
    p_table_name varchar(100),
    p_column_names varchar(200),
	p_skip_errors int,
	p_subset_condition varchar(500),
    p_input_parameters varchar(1000),
    p_result_code int,
    p_result_message varchar(500),
    p_result_row_count int,
    p_result_error_count int,
    p_result_error_data varchar(4000),
	p_result_query varchar(4000))
    language plpgsql
as
$$
DECLARE
    lv_endtime timestamp;
	lv_sql varchar(4000);
	lv_test_group varchar(200) = lower(p_test_group);
	lv_result_count int;
BEGIN
    SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_endtime;

	SELECT count(1)
	into lv_result_count
	FROM svv_all_schemas
	WHERE schema_name = 'tmp_{{currentschema}}_'||nvl(lv_test_group,'');
	
	if lv_result_count = 1 then
		lv_sql = 'SET search_path TO tmp_{{currentschema}}_'||nvl(p_test_group,'');
		EXECUTE lv_sql;
	else
		lv_sql = 'SET search_path TO {{currentschema}}';
		EXECUTE lv_sql;
	end if;
	
    if p_order_run is not null then
        delete from test_results
        where order_run = p_order_run
		  and test_type = upper(p_test_type)
		  and nvl(test_time,to_timestamp('01/01/0001 00:00:00', 'mm/dd/yyyy HH24:MI:SS')) = 
			  nvl(p_test_time,to_timestamp('01/01/0001 00:00:00', 'mm/dd/yyyy HH24:MI:SS'))
		  and nvl(lower(test_group),'xxx') = nvl(lv_test_group, 'xxx')
		  and nvl(test_action,'xxx') = nvl(p_test_action,'xxx')
          and schema_name = lower(p_schema_name)
          and table_name = lower(p_table_name)
          and nvl(column_names,'xxx') = nvl(lower(p_column_names),'xxx')
		  and nvl(skip_errors,0) = nvl(p_skip_errors,0)
          and nvl(input_parameters,'xxx') = nvl(p_input_parameters,'xxx')
		  and nvl(subset_condition,'xxx') = nvl(p_subset_condition,'xxx');
    end if;
	
	insert into test_results(
		test_type, test_group, test_action, test_time, starttime, endtime, order_run,
		schema_name, table_name, column_names,
		skip_errors, subset_condition, input_parameters,
		result_code, result_message, result_row_count,
		result_error_count, result_error_data, result_query, test_description)
    values(upper(p_test_type), lv_test_group, p_test_action, p_test_time, p_starttime, lv_endtime, p_order_run,
		   lower(p_schema_name), lower(p_table_name), lower(p_column_names),
		   p_skip_errors, p_subset_condition, p_input_parameters,
		   p_result_code, p_result_message, p_result_row_count,
		   p_result_error_count, p_result_error_data, p_result_query, p_test_description);
		   
	SET search_path TO {{currentschema}};
END;
$$;