SET search_path TO {{currentschema}};

create or replace procedure proc_qc_data_match(
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
    p_match_schema_name varchar(100),
    p_match_table_name varchar(100),
    p_match_column_names varchar(200),
	p_match_subset_condition varchar(500),
    p_mode varchar(20))
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
    lv_test_type varchar(100) := 'data match';
    rec RECORD;
    lv_column_names varchar(4000) = '';
    lv_column_name varchar(200);
	lv_mode varchar(20) = nvl(trim(lower(replace(replace(p_mode,chr(13),''),chr(10),''))),'');
BEGIN
    SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
    lv_input_parameters = 'p_match_schema_name = '||nvl(p_match_schema_name,'NULL');
    lv_input_parameters = lv_input_parameters||', p_match_table_name = '||nvl(p_match_table_name,'NULL');
    lv_input_parameters = lv_input_parameters||', p_match_column_names = '||nvl(p_match_column_names,'NULL');
	lv_input_parameters = lv_input_parameters||', p_match_subset_condition = '||nvl(p_match_subset_condition,'NULL');
    lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(p_mode,'NULL');

    if p_column_names is NULL then
        lv_result_message = 'The list of column names in the table '||p_schema_name||'.'||p_table_name||' should include at least one column name.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    if p_match_column_names is NULL then
        lv_result_message = 'The list of column names in the match-table '||p_match_schema_name||'.'||p_match_table_name||' should include at least one column name.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	if lv_mode not in ('same','no-drops','foreign key') then
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

    CALL {{currentschema}}.proc_check_table(p_match_schema_name, p_match_table_name, p_match_column_names, lv_result_code, lv_result_message);
    if lv_result_code <> 1 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	CALL {{currentschema}}.proc_match_column_type(p_schema_name, p_table_name, p_column_names, p_match_schema_name, p_match_table_name, p_match_column_names, lv_result_code, lv_result_message);
    if lv_result_code <> 1 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    if lv_mode = 'same' then
        lv_sql = 'select count(1) from (';
        lv_sql = lv_sql||'(select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
        lv_sql = lv_sql||' minus select '||p_match_column_names||' from '||p_match_schema_name||'.'||p_match_table_name;
		lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition)||')';
        lv_sql = lv_sql||'union (select '||p_match_column_names||' from '||p_match_schema_name||'.'||p_match_table_name;
		lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
        lv_sql = lv_sql||' minus select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition)||'))';
    elsif lv_mode = 'no-drops' then
        lv_sql = 'select count(1) from (';
        lv_sql = lv_sql||'select '||p_match_column_names||' from '||p_match_schema_name||'.'||p_match_table_name;
		lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
        lv_sql = lv_sql||' minus select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition)||')';
	elsif lv_mode = 'foreign key' then
	    lv_sql = 'select count(1) from (';
    	lv_sql = lv_sql||'select '||p_column_names||' from '||p_schema_name||'.'||p_table_name;
    	lv_sql = lv_sql||' where not ('||replace(p_column_names, ',', ' is null and ')||' is null'||')';
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition);
    	lv_sql = lv_sql||' minus select '||p_match_column_names||' from '||p_match_schema_name||'.'||p_match_table_name;
		lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition)||')';
    else
        lv_sql = '';
    end if;
    EXECUTE lv_sql INTO lv_result_error_count;
	lv_result_query = lv_sql;

    if lv_result_error_count - nvl(p_skip_errors,0) <= 0 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
        lv_input_parameters, 1, NULL,NULL, lv_result_error_count, NULL, lv_result_query);
    else
        lv_result_message = lv_result_error_count||' value sets of column(s) '||p_column_names||' of the table '||p_schema_name||'.'||p_table_name;
        lv_result_message = lv_result_message||' are different with the match-table '||p_match_schema_name||'.'||p_match_table_name||', column(s) '||p_match_column_names||'.';
        FOR I IN 1..regexp_count(','||p_column_names, ',')
        LOOP
            lv_column_name = ltrim(regexp_substr(','||p_column_names,',[^,]*',1,I),',');
            lv_column_names =  lv_column_names||replace('decode(nvl(regexp_count(name, '',''),0),0,nvl(cast(name as varchar),''NULL''), ''"''||name||''"'')||'',''||','name', lv_column_name);
        END LOOP;

        if lv_mode = 'same' then
            lv_sql = 'select '||lv_column_names||'kind as error_data from (';
            lv_sql = lv_sql||'(select '||p_column_names||', ''insert'' kind from '||p_schema_name||'.'||p_table_name;
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
            lv_sql = lv_sql||' minus select '||p_match_column_names||', ''insert'' kind from '||p_match_schema_name||'.'||p_match_table_name;
			lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition)||')';
            lv_sql = lv_sql||'union (select '||p_match_column_names||', ''delete'' from '||p_match_schema_name||'.'||p_match_table_name;
			lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
            lv_sql = lv_sql||' minus select '||p_column_names||', ''delete'' from '||p_schema_name||'.'||p_table_name;
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition)||'))';
        elsif lv_mode = 'no-drops' then
            lv_sql = 'select '||lv_column_names||'kind as error_data from (';
            lv_sql = lv_sql||'select '||p_match_column_names||', ''delete'' kind from '||p_match_schema_name||'.'||p_match_table_name;
			lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
            lv_sql = lv_sql||' minus select '||p_column_names||', ''delete'' from '||p_schema_name||'.'||p_table_name;
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition)||')';
        elsif lv_mode = 'foreign key' then			
			lv_sql = 'select '||lv_column_names||'kind as error_data from (';
			lv_sql = lv_sql||'select '||p_column_names||', ''unexpected'' kind from '||p_schema_name||'.'||p_table_name;
    		lv_sql = lv_sql||' where not ('||replace(p_column_names, ',', ' is null and ')||' is null'||')';
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' and '||p_subset_condition);
    		lv_sql = lv_sql||' minus select '||p_match_column_names||', ''unexpected'' from '||p_match_schema_name||'.'||p_match_table_name;
			lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition)||')';
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
        lv_input_parameters, 0, lv_result_message,NULL, lv_result_error_count,lv_result_error_data, lv_result_query);
    end if;
EXCEPTION WHEN others THEN
RAISE EXCEPTION 'Test "%" for %.% column(s) %, % fails. [%] % ',lv_test_type,p_schema_name, p_table_name,p_column_names, lv_input_parameters, SQLSTATE,SQLERRM;
END;
$$;
