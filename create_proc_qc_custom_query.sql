SET search_path TO {{currentschema}};

create or replace procedure proc_qc_custom_query(
    p_test_group varchar(200),
	p_test_action varchar(100),
	p_test_description varchar(1000),
	p_test_time timestamp,
	p_order_run varchar(50),
    p_schema_name varchar(100),
    p_table_name varchar(100),
    p_column_names varchar(200),
	p_skip_errors int,
    p_query varchar(4000))
    language plpgsql
as
$$
DECLARE
    lv_sql varchar(4000);
	lv_count int;
	lv_result_query varchar(4000);
    lv_result_error_count int;
    lv_starttime timestamp;
    lv_input_parameters varchar(4000);
    lv_result_error_data varchar(4000);
    lv_result_code int;
    lv_result_message varchar(500);
    lv_test_type varchar(100) = 'custom query';
    rec RECORD;
    lv_column_names varchar(4000) = '';
    lv_column_name varchar(200);
BEGIN
    SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
    lv_input_parameters = 'p_query = '||nvl(p_query,'NULL');
	
	if p_query not ilike '% error_data %' then
        lv_result_message = 'The custom query for the table '||p_schema_name||'.'||p_table_name||' should include ERROR_DATA column.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

	lv_sql = 'select count(1) from ('||p_query||')';
	
    EXECUTE lv_sql INTO lv_result_error_count;
	lv_result_query = lv_sql;

    if lv_result_error_count - nvl(p_skip_errors,0) <= 0 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,NULL,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,NULL,
        lv_input_parameters, 1, NULL,NULL, lv_result_error_count, NULL, lv_result_query);
    else
	    lv_result_message = lv_result_error_count||' error_data row(s) for the table '||p_schema_name||'.'||p_table_name;
        lv_result_message = lv_result_message||' were occured.';
	
		lv_sql = p_query||' limit 10';
		lv_sql = 'select listagg(error_data, ''/n'') within group (order by error_data) from ('||lv_sql||')';
		EXECUTE lv_sql INTO lv_result_error_data;
		lv_result_error_data = replace(lv_result_error_data, '/n', chr(10));
        lv_result_error_data = 'error_data'||chr(10)||lv_result_error_data;
		
		CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,NULL,
        lv_input_parameters, 0, lv_result_message,NULL, lv_result_error_count,lv_result_error_data, lv_result_query);
    end if;
EXCEPTION WHEN others THEN
RAISE EXCEPTION 'Test "%" for %.% column(s) %, % fails. [%] % ',lv_test_type,p_schema_name, p_table_name,p_column_names, lv_input_parameters, SQLSTATE,SQLERRM;
END;
$$;
