SET search_path TO {{currentschema}};

create or replace procedure proc_qc_allowed_increment(
    p_test_group varchar(200),
	p_test_action varchar(100),
	p_test_description varchar(1000),
	p_test_time timestamp,
	p_order_run varchar(50),
    p_schema_name varchar(100),
    p_table_name varchar(100),
	p_subset_condition varchar(500),
    p_delta_record_count int)
    language plpgsql
as
$$
DECLARE
    lv_sql varchar(4000);
	lv_result_query varchar(4000);
    lv_result_error_count int;
    lv_starttime timestamp;
    lv_input_parameters varchar(4000);
    lv_result_code int;
    lv_result_message varchar(500);
    lv_test_type varchar(100) := 'allowed increment';
    rec RECORD;
    lv_result_row_count int;
    lv_result_row_count_prev int;
BEGIN
    SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
    lv_input_parameters = 'p_delta_record_count = '||nvl(p_delta_record_count,0);

    CALL {{currentschema}}.proc_check_table(p_schema_name, p_table_name, NULL, lv_result_code, lv_result_message);
    if lv_result_code <> 1 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,NULL,NULL,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    lv_sql = 'select count(1) from '||p_schema_name||'.'||p_table_name;
	lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
    EXECUTE lv_sql INTO lv_result_row_count;
	lv_result_query = lv_sql;

	select result_row_count
	into lv_result_row_count_prev
	from (
        select result_row_count
        from {{currentschema}}.test_results
		where test_type = upper(lv_test_type)
		and nvl(test_group,'xxx') = nvl(lower(p_test_group),'xxx')
		and schema_name = lower(p_schema_name)
		and table_name = lower(p_table_name)
		and nvl(p_subset_condition,'xxx') = nvl(p_subset_condition,'xxx')
        order by starttime desc)
	limit 1;
	lv_result_query = 'N/A';

    lv_result_code = case when nvl(lv_result_row_count_prev,0) + nvl(p_delta_record_count,0) <= nvl(lv_result_row_count,0) or nvl(lv_result_row_count_prev,0) = 0 then 1 else 0 end;
    if lv_result_code = 0 then
        lv_result_error_count = nvl(lv_result_row_count_prev,0) + nvl(p_delta_record_count,0) - nvl(lv_result_row_count,0);
        lv_result_message = 'The table '||p_schema_name||'.'||p_table_name||' has '|| lv_result_row_count_prev - lv_result_row_count||' rows less from the previous run than allowed.';
    else
		lv_result_error_count = 0;
	end if;

    CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,NULL,NULL,p_subset_condition,
    lv_input_parameters, lv_result_code, NULL,lv_result_row_count, lv_result_error_count, NULL, lv_result_query);

EXCEPTION WHEN others THEN
RAISE EXCEPTION 'Test "%" for %.%, % fails. [%] % ',lv_test_type,p_schema_name, p_table_name,lv_input_parameters, SQLSTATE,SQLERRM;
END;
$$;