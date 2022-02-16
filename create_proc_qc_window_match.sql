SET search_path TO {{currentschema}};

create or replace procedure proc_qc_window_match(
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
    p_window_column varchar(100),
    p_window_days int,
	p_mode varchar(20))
    language plpgsql
as
$$
DECLARE
    lv_sql varchar(4000);
	lv_result_query varchar(4000);
    lv_result_error_count int;
    lv_starttime timestamp;
	lv_latetime timestamp;
    lv_input_parameters varchar(4000);
    lv_result_error_data varchar(4000);
    lv_result_code int;
    lv_result_message varchar(500);
    lv_test_type varchar(100) := 'window match';
    rec RECORD;
    lv_column_names varchar(4000) = '';
    lv_column_name varchar(200);
	lv_mode varchar(20) = nvl(trim(lower(replace(replace(p_mode,chr(13),''),chr(10),''))),'');
BEGIN
    SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	
    lv_input_parameters = 'p_window_column = '||nvl(p_window_column,'NULL')||', p_window_days = '||nvl(p_window_days,1);
	lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(p_mode,'NULL');

    if p_column_names is NULL then
        lv_result_message = 'The list of column names of the table '||p_schema_name||'.'||p_table_name||' should include at least one column name.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    if p_window_column is NULL then
        lv_result_message = 'The test for the table '||p_schema_name||'.'||p_table_name||', column(s)' ||p_column_names||' was run with no window-column-name.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    if regexp_count(p_window_column,',') > 0 then
        lv_result_message = 'The window-column-name argument '||p_window_column||' has more than one column in the test for the table '||p_schema_name||'.'||p_table_name||', column(s)' ||p_column_names||'.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	if lv_mode not in ('same','no-drops') then
        lv_result_message = 'The parameter p_mode does not have acceptable value for the table '||p_schema_name||'.'||p_table_name||', column(s)' ||p_column_names||'.';
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

    CALL {{currentschema}}.proc_check_table(p_schema_name, p_table_name, p_window_column, lv_result_code, lv_result_message);
    if lv_result_code <> 1 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    lv_sql = 'select count(1) from information_schema.columns where table_schema = '''||lower(p_schema_name);
    lv_sql = lv_sql||''' and table_name = '''||lower(p_table_name)||''' and column_name = '''||lower(p_window_column)||'''';
    lv_sql = lv_sql||' and (data_type = ''date'' or data_type like ''%time%'')';
    EXECUTE lv_sql INTO lv_result_code;
    if lv_result_code = 0 then
        lv_result_message = 'The window-column '||p_window_column||' of the table '||||p_schema_name||'.'||p_table_name||' has no time/date data type.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
        lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	lv_sql = 'select count('||p_window_column||') from '||p_schema_name||'.'||p_table_name;
	lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
	EXECUTE lv_sql INTO lv_result_error_count;
	
	if lv_result_error_count = 0 then
		lv_result_message = 'The window-column '||p_window_column||' of the table '||||p_schema_name||'.'||p_table_name||' has no time/date.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
        lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    else
		lv_sql = 'select max('||p_window_column||') from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
		EXECUTE lv_sql INTO lv_latetime;
	end if;
	
	
	if lv_mode = 'same' then
    	lv_sql = 'select count(1) from (';
    	lv_sql = lv_sql||'(select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
    	lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - nvl(p_window_days, 1)||''' AND '''||lv_latetime||'''';
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition);
    	lv_sql = lv_sql||' minus select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
    	lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - 2 * nvl(p_window_days, 1)||''' AND '''||lv_latetime - nvl(p_window_days, 1)||'''';
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition)||')';
    	lv_sql = lv_sql||'union (select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - 2 * nvl(p_window_days, 1)||''' AND '''||lv_latetime- nvl(p_window_days, 1)||'''';
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition);
    	lv_sql = lv_sql||' minus select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
    	lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - nvl(p_window_days, 1)||''' AND '''||lv_latetime||'''';
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition)||'))';
	elsif lv_mode = 'no-drops' then
		lv_sql = 'select count(1) from (';
    	lv_sql = lv_sql||'select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - 2 * nvl(p_window_days, 1)||''' AND '''||lv_latetime- nvl(p_window_days, 1)||'''';
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition);
    	lv_sql = lv_sql||' minus select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
    	lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - nvl(p_window_days, 1)||''' AND '''||lv_latetime||'''';
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition)||')';
	else
		lv_sql = '';
	end if;
    EXECUTE lv_sql INTO lv_result_error_count;
	lv_result_query = lv_sql;

    if lv_result_error_count -nvl(p_skip_errors, 0) <= 0 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
        lv_input_parameters, 1, NULL,NULL, lv_result_error_count, NULL, lv_result_query);
    else
        lv_result_message = lv_result_error_count||' value sets of column(s) '||p_column_names||' of the table '||p_schema_name||'.'||p_table_name;
        lv_result_message = lv_result_message||' are different from the previous time window.';
        FOR I IN 1..regexp_count(','||p_column_names, ',')
        LOOP
            lv_column_name = ltrim(regexp_substr(','||p_column_names,',[^,]*',1,I),',');
            lv_column_names =  lv_column_names||replace('decode(nvl(regexp_count(name, '',''),0),0,nvl(cast(name as varchar),''NULL''), ''"''||name||''"'')||'',''||','name', lv_column_name);
        END LOOP;
        --lv_column_names = rtrim(lv_column_names,'||'',''||');
		
		if lv_mode = 'same' then
        	lv_sql = 'select '||lv_column_names||'kind as error_data from (';
        	lv_sql = lv_sql||'(select '||p_column_names||', ''insert'' kind from '||p_schema_name||'.'||p_table_name;
        	lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - nvl(p_window_days, 1)||''' AND '''||lv_latetime||'''';
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition);
        	lv_sql = lv_sql||' minus select '||p_column_names||', ''insert'' kind from '||p_schema_name||'.'||p_table_name;
        	lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - 2 * nvl(p_window_days, 1)||''' AND '''||lv_latetime - nvl(p_window_days, 1)||'''';
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition)||')';
        	lv_sql = lv_sql||'union (select '||p_column_names||', ''delete'' kind from '||p_schema_name||'.'||p_table_name;
        	lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - 2 * nvl(p_window_days, 1)||''' AND '''||lv_latetime- nvl(p_window_days, 1)||'''';
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition);
        	lv_sql = lv_sql||' minus select '||p_column_names||', ''delete'' kind from '||p_schema_name||'.'||p_table_name;
        	lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - nvl(p_window_days, 1)||''' AND '''||lv_latetime||'''';
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition)||'))';
		--lv_sql = lv_sql||' order by '||p_column_names;
		elsif lv_mode = 'no-drops' then
		    lv_sql = 'select '||lv_column_names||'kind as error_data from (';
			lv_sql = lv_sql||'select '||p_column_names||', ''delete'' kind from '||p_schema_name||'.'||p_table_name;
			lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - 2 * nvl(p_window_days, 1)||''' AND '''||lv_latetime- nvl(p_window_days, 1)||'''';
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition);
    		lv_sql = lv_sql||' minus select '||p_column_names||', ''delete'' from '||p_schema_name||'.'||p_table_name;
    		lv_sql = lv_sql||' where '||p_window_column||' between '''||lv_latetime - nvl(p_window_days, 1)||''' AND '''||lv_latetime||'''';
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition)||')';
		else
			lv_sql = '';
		end if;

		lv_sql = lv_sql||' limit 10';
		lv_sql = 'select listagg(error_data, ''/n'') within group (order by error_data) from ('||lv_sql||')';
		RAISE INFO 'lv_sql = %', lv_sql;
		EXECUTE lv_sql INTO lv_result_error_data;
		lv_result_error_data = replace(lv_result_error_data, '/n', chr(10));
        lv_result_error_data = p_column_names||',kind'||chr(10)||lv_result_error_data;

        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
        lv_input_parameters, 0, lv_result_message,NULL,lv_result_error_count, lv_result_error_data, lv_result_query);
    end if;

EXCEPTION WHEN others THEN
RAISE EXCEPTION 'Test "%" for %.% column(s) %, % fails. [%] % ',lv_test_type,p_schema_name, p_table_name,p_column_names, lv_input_parameters, SQLSTATE,SQLERRM;
END;
$$;
