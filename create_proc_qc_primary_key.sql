SET search_path TO {{currentschema}};

create or replace procedure proc_qc_primary_key(
    p_test_group varchar(200),
	p_test_action varchar(100),
	p_test_description varchar(1000),
	p_test_time timestamp,
	p_order_run varchar(50),
    p_schema_name varchar(100),
    p_table_name varchar(100),
    p_column_names varchar(200),
	p_skip_errors int,
	p_subset_condition varchar(500))
    language plpgsql
as
$$
DECLARE
	lv_sql varchar(4000);
	lv_result_count int;
	lv_test_group varchar(200) = lower(p_test_group);
BEGIN
    CALL {{currentschema}}.proc_qc_not_null(
	p_test_group,
	p_test_action,
	p_test_description,
	p_test_time,
    p_order_run,
    p_schema_name,
    p_table_name,
    p_column_names,
	p_skip_errors,
	p_subset_condition);
	
    CALL {{currentschema}}.proc_qc_uniqueness(
	p_test_group,
	p_test_action,
	p_test_description,
	p_test_time,
    p_order_run,
    p_schema_name,
    p_table_name,
    p_column_names,
	p_skip_errors,
	p_subset_condition);
	
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
	set test_type = 'PRIMARY KEY', 
		input_parameters = ltrim(nvl(input_parameters,'')||', p_mode = NOT NULL',', ')
	where test_type = 'NOT NULL'
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
	
	update test_results
	set test_type = 'PRIMARY KEY', 
		input_parameters = ltrim(nvl(input_parameters,'')||', p_mode = UNIQUENESS',', ')
	where test_type = 'UNIQUENESS'
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