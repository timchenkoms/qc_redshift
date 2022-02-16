SET search_path TO {{currentschema}};

create or replace procedure proc_qc_condition_check(
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
    p_condition varchar(1000))
    language plpgsql
as
$$
DECLARE
    lv_sql varchar(4000);
	lv_result_query varchar(4000);
    lv_result_error_count int;
    lv_starttime timestamp;
    lv_input_parameters varchar(4000);
    lv_result_error_data varchar(4000);
    lv_result_code int;
    lv_result_message varchar(500);
    lv_test_type varchar(100) := 'condition check';
    rec RECORD;
    lv_column_names varchar(4000) = '';
    lv_column_name varchar(200);
BEGIN
    SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
    lv_input_parameters = 'p_condition = '||nvl(p_condition,'NULL');

    if p_condition is NULL then
        lv_result_message = 'There is no any condition to check data in the table '||p_schema_name||'.'||p_table_name||'.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    if p_column_names is NULL then
        lv_result_message = 'There is no columns listed in the condition to check data in the table '||p_schema_name||'.'||p_table_name||'.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    CALL {{currentschema}}.proc_check_table(p_schema_name, p_table_name, p_column_names, lv_result_code, lv_result_message);
    if lv_result_code <> 1 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    lv_sql = 'select count(1) from (';
    lv_sql = lv_sql||'select distinct '||p_column_names||' from '||p_schema_name||'.'||p_table_name||' where not ('||p_condition||')';
	lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition)||')';
    RAISE INFO 'lv_sql = %', lv_sql;
    EXECUTE lv_sql INTO lv_result_error_count;
	lv_result_query = lv_sql;

    if lv_result_error_count - nvl(p_skip_errors,0) <= 0 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
        lv_input_parameters, 1, NULL,NULL, lv_result_error_count, NULL, lv_result_query);
    else
        lv_result_message = lv_result_error_count||' value sets of column(s) '||p_column_names||' of the table '||p_schema_name||'.'||p_table_name||' do not pass the condition ('||p_condition||').';
        FOR I IN 1..regexp_count(','||p_column_names, ',')
        LOOP
            lv_column_name = ltrim(regexp_substr(','||p_column_names,',[^,]*',1,I),',');
            lv_column_names =  lv_column_names||replace('decode(nvl(regexp_count(name, '',''),0),0,nvl(cast(name as varchar),''NULL''), ''"''||name||''"'')||'',''||','name', lv_column_name);
        END LOOP;
        lv_column_names = rtrim(lv_column_names,'||'',''||');

        lv_sql = 'select '||lv_column_names||' as error_data from (';
        lv_sql = lv_sql||'select distinct '||p_column_names||' from '||p_schema_name||'.'||p_table_name||' where not ('||p_condition||')';
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition)||')';
		lv_sql = lv_sql||' limit 10';

		lv_sql = 'select listagg(error_data, ''/n'') within group (order by error_data) from ('||lv_sql||')';
		RAISE INFO 'lv_sql = %', lv_sql;
		EXECUTE lv_sql INTO lv_result_error_data;
		lv_result_error_data = replace(lv_result_error_data, '/n', chr(10));
        lv_result_error_data = p_column_names||chr(10)||lv_result_error_data;

        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
        lv_input_parameters, 0, lv_result_message,NULL, lv_result_error_count,lv_result_error_data, lv_result_query);
    end if;
EXCEPTION WHEN others THEN
RAISE EXCEPTION 'Test "%" for %.% column(s) %, % fails. [%] % ',lv_test_type,p_schema_name, p_table_name,p_column_names, lv_input_parameters, SQLSTATE,SQLERRM;
END;
$$;
