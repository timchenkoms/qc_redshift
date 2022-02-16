SET search_path TO {{currentschema}};

create or replace procedure proc_qc_prior_match(
    p_test_group varchar(200),
	p_test_action varchar(100),
	p_test_description varchar(1000),
	p_test_time timestamp,
	p_order_run varchar(50),
    p_schema_name varchar(100),
    p_table_name varchar(100),
    p_column_names varchar(200),
	p_skip_errors int,
	p_subset_condition varchar(500),
    p_prior_schema_name varchar(100),
    p_mode varchar(20))
    language plpgsql
as
$$
DECLARE
	lv_input_parameters varchar(4000);
	lv_mode varchar(20) = nvl(trim(lower(p_mode)),'');
	lv_starttime timestamp;
	lv_result_message varchar(500);
	lv_test_type varchar(100) := 'prior match';
	lv_sql varchar(4000);
	lv_result_count int;
	lv_test_group varchar(200) = lower(p_test_group);
BEGIN
	SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
    lv_input_parameters = 'p_match_schema_name = '||nvl(p_prior_schema_name,'NULL');
    lv_input_parameters = lv_input_parameters||', p_match_table_name = '||nvl(p_table_name,'NULL');
    lv_input_parameters = lv_input_parameters||', p_match_column_names = '||nvl(p_column_names,'NULL');
	    lv_input_parameters = lv_input_parameters||', p_match_subset_condition = '||nvl(p_subset_condition,'NULL');
    lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(p_mode,'NULL');

	if lv_mode not in ('same','no-drops') then
        lv_result_message = 'The parameter p_mode does not have acceptable value for the table '||p_schema_name||'.'||p_table_name||', column(s)' ||p_column_names||'.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
    call {{currentschema}}.proc_qc_data_match(
	p_test_group,
	p_test_action,
	p_test_description,
	p_test_time,
    p_order_run,
    p_schema_name,
    p_table_name,
    p_column_names,
	p_skip_errors,
	p_subset_condition,
    p_prior_schema_name,
    p_table_name,
    p_column_names,
	p_subset_condition,
    p_mode);
	
	SELECT count(1)
	into lv_result_count
	FROM svv_all_schemas
	WHERE schema_name = 'tmp_{{currentschema}}_'||nvl(lv_test_group,'');
	
	if lv_result_count = 1 then
		lv_sql = 'SET search_path TO tmp_{{currentschema}}_'||nvl(lv_test_group,'');
		EXECUTE lv_sql;
	else
		lv_sql = 'SET search_path TO {{currentschema}}';
		EXECUTE lv_sql;
	end if;
	
	update test_results
	set test_type = 'PRIOR MATCH'
	where test_type = 'DATA MATCH'
	and nvl(lower(test_group),'xxx') = nvl(lv_test_group,'xxx')
	and nvl(test_action,'xxx') = nvl(p_test_action,'xxx')
	and nvl(test_time,to_date('0000-00-01 00:00:01','yyyy-mm-dd hh24:mi:ss')) = 
		nvl(p_test_time,to_date('0000-00-01 00:00:01','yyyy-mm-dd hh24:mi:ss'))
	and nvl(order_run,'xxx') = nvl(p_order_run,'xxx')
	and nvl(schema_name,'xxx') = nvl(p_schema_name,'xxx')
	and nvl(table_name,'xxx') = nvl(p_table_name,'xxx')
	and nvl(column_names,'xxx') = nvl(p_column_names,'xxx')
	and nvl(skip_errors,0) = nvl(p_skip_errors,0)
	and nvl(subset_condition,'xxx') = nvl(p_subset_condition,'xxx');
	
	SET search_path TO {{currentschema}};
END;
$$;
