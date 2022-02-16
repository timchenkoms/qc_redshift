SET search_path TO {{currentschema}};

create or replace procedure proc_qc_aggregate_match(
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
	p_groupby_names varchar(200),
	p_having_condition varchar(500),
    p_match_schema_name varchar(100),
    p_match_table_name varchar(100),
    p_match_column_names varchar(200),
	p_match_subset_condition varchar(500),
	p_match_groupby_names varchar(200),
	p_match_having_condition varchar(500),
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
    lv_test_type varchar(100) := 'aggregate match';
    rec RECORD;
    lv_column_names varchar(4000) = '';
    lv_column_name varchar(200);
	lv_mode varchar(20) = nvl(trim(lower(replace(replace(p_mode,chr(13),''),chr(10),''))),'');
BEGIN
    SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
    lv_input_parameters = 'p_groupby_names = '||nvl(p_groupby_names,'NULL');
	lv_input_parameters = lv_input_parameters||', p_having_condition = '||nvl(p_having_condition,'NULL');
	lv_input_parameters = lv_input_parameters||', p_match_schema_name = '||nvl(p_match_schema_name,'NULL');
    lv_input_parameters = lv_input_parameters||', p_match_table_name = '||nvl(p_match_table_name,'NULL');
    lv_input_parameters = lv_input_parameters||', p_match_column_names = '||nvl(p_match_column_names,'NULL');
	lv_input_parameters = lv_input_parameters||', p_match_subset_condition = '||nvl(p_match_subset_condition,'NULL');
	lv_input_parameters = lv_input_parameters||', p_match_groupby_names = '||nvl(p_match_groupby_names,'NULL');
	lv_input_parameters = lv_input_parameters||', p_match_having_condition = '||nvl(p_match_having_condition,'NULL');
    lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(p_mode,'NULL');

    if p_groupby_names is NULL then
        lv_result_message = 'The list of column names in group by clause for the table '||p_schema_name||'.'||p_table_name||' should include at least one column name.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	if not (REGEXP_COUNT(p_column_names,'\\(') > 0
	   and REGEXP_COUNT(p_column_names,'\\(') = REGEXP_COUNT(p_column_names,'\\)')
	   and REGEXP_COUNT(p_column_names,'\\(') - 1 = REGEXP_COUNT(p_column_names,','))
	   or p_column_names is null
	then
        lv_result_message = 'The list of aggegate functions with column names for the table '||p_schema_name||'.'||p_table_name||' should include at least one aggregate function.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    if p_match_groupby_names is NULL then
        lv_result_message = 'The list of column names in group by clause for the match-table '||p_match_schema_name||'.'||p_match_table_name||' should include at least one column name.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	if not (REGEXP_COUNT(p_match_column_names,'\\(') > 0
	   and REGEXP_COUNT(p_match_column_names,'\\(') = REGEXP_COUNT(p_match_column_names,'\\)')
	   and REGEXP_COUNT(p_match_column_names,'\\(') - 1 = REGEXP_COUNT(p_match_column_names,','))
	   or p_match_column_names is null
	then
        lv_result_message = 'The list of aggegate functions with column names for the table '||p_schema_name||'.'||p_table_name||' should include at least one aggregate function.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	if lv_mode not in ('same','no-drops','numeric increment') then
        lv_result_message = 'The parameter p_mode does not have acceptable value for the table '||p_schema_name||'.'||p_table_name||', column(s)' ||p_column_names||'.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    CALL {{currentschema}}.proc_check_table(p_schema_name, p_table_name, p_groupby_names, lv_result_code, lv_result_message);
    if lv_result_code <> 1 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    CALL {{currentschema}}.proc_check_table(p_match_schema_name, p_match_table_name, p_match_groupby_names, lv_result_code, lv_result_message);
    if lv_result_code <> 1 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	CALL {{currentschema}}.proc_match_column_type(p_schema_name, p_table_name, p_groupby_names, p_match_schema_name, p_match_table_name, p_match_groupby_names, lv_result_code, lv_result_message);
    if lv_result_code <> 1 then
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;
	
	if REGEXP_COUNT(p_column_names,',') <> REGEXP_COUNT(p_match_column_names,',') then
        lv_result_message = 'The number of aggegate functions for the table '||p_schema_name||'.'||p_table_name||' should be the same as a number of aggegate functions for the table '||p_match_schema_name||'.'||p_match_table_name||'.';
        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
          lv_input_parameters, -1, lv_result_message,NULL, NULL, NULL, NULL);
        return;
    end if;

    if lv_mode = 'same' then
        lv_sql = 'select count(1) from ((';
        lv_sql = lv_sql||'select '||decode(p_groupby_names,null,'',p_groupby_names||', ')||p_column_names||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
		lv_sql = lv_sql||decode(p_groupby_names,null,'',' group by '||p_groupby_names);
		lv_sql = lv_sql||decode(p_having_condition, null,'','','',' having '||p_having_condition);
        lv_sql = lv_sql||' minus select '|| decode(p_match_groupby_names,null,'',p_match_groupby_names||', ')||p_match_column_names||' from '||p_match_schema_name||'.'||p_match_table_name;
		lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
		lv_sql = lv_sql||decode(p_match_groupby_names,null,'',' group by '||p_match_groupby_names);
		lv_sql = lv_sql||decode(p_match_having_condition, null,'','','',' having '||p_match_having_condition);
		lv_sql = lv_sql||') union (select '||decode(p_match_groupby_names,null,'',p_match_groupby_names||', ')||p_match_column_names||' from '||p_match_schema_name||'.'||p_match_table_name;
		lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
		lv_sql = lv_sql||decode(p_match_groupby_names,null,'',' group by '||p_match_groupby_names);
		lv_sql = lv_sql||decode(p_match_having_condition, null,'','','',' having '||p_match_having_condition);
        lv_sql = lv_sql||' minus select '|| decode(p_groupby_names,null,'',p_groupby_names||', ')||p_column_names||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
		lv_sql = lv_sql||decode(p_groupby_names,null,'',' group by '||p_groupby_names);
		lv_sql = lv_sql||decode(p_having_condition, null,'','','',' having '||p_having_condition);
		lv_sql = lv_sql||'))';
    elsif lv_mode = 'no-drops' then
        lv_sql = 'select count(1) from (';
        lv_sql = lv_sql||'select '||decode(p_match_groupby_names,null,'',p_match_groupby_names||', ')||p_match_column_names||' from '||p_match_schema_name||'.'||p_match_table_name;
		lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
		lv_sql = lv_sql||decode(p_match_groupby_names,null,'',' group by '||p_match_groupby_names);
		lv_sql = lv_sql||decode(p_match_having_condition, null,'','','',' having '||p_match_having_condition);
        lv_sql = lv_sql||' minus select '||decode(p_groupby_names,null,'',p_groupby_names||', ')||p_column_names||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
		lv_sql = lv_sql||decode(p_groupby_names,null,'',' group by '||p_groupby_names);
		lv_sql = lv_sql||decode(p_having_condition, null,'','','',' having '||p_having_condition);
		lv_sql = lv_sql||')';
    elsif lv_mode = 'numeric increment' then
        lv_sql = 'select count(1) from (';
		lv_sql = lv_sql||'select '||decode(p_match_groupby_names,null,'',p_match_groupby_names);
        FOR I IN 1..regexp_count(','||p_match_column_names, ',')
		LOOP lv_sql = lv_sql||', sum(agg_col_'||i||')'; END LOOP;
		lv_sql = lv_sql||' from (';
		lv_sql = lv_sql||'select '||decode(p_groupby_names,null,'',p_groupby_names);
		FOR I IN 1..regexp_count(','||p_column_names, ',')
		LOOP
            lv_column_name = ltrim(regexp_substr(','||p_column_names,',[^,]*',1,I),',');
            lv_sql = lv_sql||', '||lv_column_name||' agg_col_'||i;
        END LOOP;
		lv_sql = lv_sql||' from '||p_schema_name||'.'||p_table_name;
		lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
		lv_sql = lv_sql||decode(p_groupby_names,null,'',' group by '||p_groupby_names);
		lv_sql = lv_sql||decode(p_having_condition, null,'','','',' having '||p_having_condition);
		lv_sql = lv_sql||' union all select ';
		lv_sql = lv_sql||decode(p_match_groupby_names,null,'',p_match_groupby_names);
		FOR I IN 1..regexp_count(','||p_match_column_names, ',')
        LOOP
            lv_column_name = ltrim(regexp_substr(','||p_match_column_names,',[^,]*',1,I),',');
            lv_sql = lv_sql||', -'||lv_column_name;
        END LOOP;
		lv_sql = lv_sql||' from '||p_match_schema_name||'.'||p_match_table_name;
		lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
		lv_sql = lv_sql||decode(p_match_groupby_names,null,'',' group by '||p_match_groupby_names);
		lv_sql = lv_sql||decode(p_match_having_condition, null,'','','',' having '||p_match_having_condition);
		lv_sql = lv_sql||') group by '||p_match_groupby_names;
		lv_sql = lv_sql||' having';
		FOR I IN 1..regexp_count(','||p_match_column_names, ',')
		LOOP lv_sql = lv_sql||' sum(agg_col_'||i||') < 0 or'; END LOOP;
		lv_sql = rtrim(lv_sql,'or')||')';
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
        lv_result_message = lv_result_error_count||' value sets of column group(s) '||p_groupby_names||' of the table '||p_schema_name||'.'||p_table_name;
        lv_result_message = lv_result_message||' are different with the match-table '||p_match_schema_name||'.'||p_match_table_name||', column group(s) '||p_match_groupby_names||'.';
        FOR I IN 1..regexp_count(','||p_groupby_names, ',')
        LOOP
            lv_column_name = ltrim(regexp_substr(','||p_groupby_names,',[^,]*',1,I),',');
            lv_column_names =  lv_column_names||replace('decode(nvl(regexp_count(name, '',''),0),0,nvl(cast(name as varchar),''NULL''), ''"''||name||''"'')||'',''||','name', lv_column_name);
        END LOOP;

        if lv_mode = 'same' then
            lv_sql = 'select '||lv_column_names||'kind as error_data from (';
        	lv_sql = lv_sql||'(select '||decode(p_groupby_names,null,'',p_groupby_names||', ')||p_column_names||', ''insert'' kind from '||p_schema_name||'.'||p_table_name;
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
			lv_sql = lv_sql||decode(p_groupby_names,null,'',' group by '||p_groupby_names);
			lv_sql = lv_sql||decode(p_having_condition, null,'','','',' having '||p_having_condition);
        	lv_sql = lv_sql||' minus select '||decode(p_match_groupby_names,null,'',p_match_groupby_names||', ')||p_match_column_names||', ''insert'' kind from '||p_match_schema_name||'.'||p_match_table_name;
			lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
			lv_sql = lv_sql||decode(p_match_groupby_names,null,'',' group by '||p_match_groupby_names);
			lv_sql = lv_sql||decode(p_match_having_condition, null,'','','',' having '||p_match_having_condition);
			lv_sql = lv_sql||')';
			lv_sql = lv_sql||' union (select '||decode(p_match_groupby_names,null,'',p_match_groupby_names||', ')||p_match_column_names||', ''delete'' kind from '||p_match_schema_name||'.'||p_match_table_name;
			lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
			lv_sql = lv_sql||decode(p_match_groupby_names,null,'',' group by '||p_match_groupby_names);
			lv_sql = lv_sql||decode(p_match_having_condition, null,'','','',' having '||p_match_having_condition);
        	lv_sql = lv_sql||' minus select '||decode(p_groupby_names,null,'',p_groupby_names||', ')||p_column_names||', ''delete'' kind from '||p_schema_name||'.'||p_table_name;
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
			lv_sql = lv_sql||decode(p_groupby_names,null,'',' group by '||p_groupby_names);
			lv_sql = lv_sql||decode(p_having_condition, null,'','','',' having '||p_having_condition);
			lv_sql = lv_sql||'))';
        elsif lv_mode = 'no-drops' then
            lv_sql = 'select '||lv_column_names||'kind as error_data from (';
        	lv_sql = lv_sql||'select '||decode(p_match_groupby_names,null,'',p_match_groupby_names||', ')||p_match_column_names||', ''delete'' kind from '||p_match_schema_name||'.'||p_match_table_name;
			lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
			lv_sql = lv_sql||decode(p_match_groupby_names,null,'',' group by '||p_match_groupby_names);
			lv_sql = lv_sql||decode(p_match_having_condition, null,'','','',' having '||p_match_having_condition);
        	lv_sql = lv_sql||' minus select '||decode(p_groupby_names,null,'',p_groupby_names||', ')||p_column_names||', ''delete'' kind from '||p_schema_name||'.'||p_table_name;
			lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
			lv_sql = lv_sql||decode(p_groupby_names,null,'',' group by '||p_groupby_names);
			lv_sql = lv_sql||decode(p_having_condition, null,'','','',' having '||p_having_condition);
			lv_sql = lv_sql||')';
        elsif lv_mode = 'numeric increment' then
		    lv_column_names = rtrim(lv_column_names,'||'',''||');
		
		    lv_sql = 'select '||lv_column_names||' as error_data from (';
		    lv_sql = lv_sql||'select '||decode(p_match_groupby_names,null,'',p_match_groupby_names);
		    lv_sql = lv_sql||' from (';
		    lv_sql = lv_sql||'select '||decode(p_groupby_names,null,'',p_groupby_names);
		    FOR I IN 1..regexp_count(','||p_column_names, ',')
		    LOOP
                lv_column_name = ltrim(regexp_substr(','||p_column_names,',[^,]*',1,I),',');
                lv_sql = lv_sql||', '||lv_column_name||' agg_col_'||i;
            END LOOP;
		    lv_sql = lv_sql||' from '||p_schema_name||'.'||p_table_name;
		    lv_sql = lv_sql||decode(p_subset_condition, null,'','','',' where '||p_subset_condition);
		    lv_sql = lv_sql||decode(p_groupby_names,null,'',' group by '||p_groupby_names);
		    lv_sql = lv_sql||decode(p_having_condition, null,'','','',' having '||p_having_condition);
		    lv_sql = lv_sql||' union all select ';
		    lv_sql = lv_sql||decode(p_match_groupby_names,null,'',p_match_groupby_names);
		    FOR I IN 1..regexp_count(','||p_match_column_names, ',')
            LOOP
                lv_column_name = ltrim(regexp_substr(','||p_match_column_names,',[^,]*',1,I),',');
                lv_sql = lv_sql||', -'||lv_column_name;
            END LOOP;
		    lv_sql = lv_sql||' from '||p_match_schema_name||'.'||p_match_table_name;
		    lv_sql = lv_sql||decode(p_match_subset_condition, null,'','','',' where '||p_match_subset_condition);
		    lv_sql = lv_sql||decode(p_match_groupby_names,null,'',' group by '||p_match_groupby_names);
		    lv_sql = lv_sql||decode(p_match_having_condition, null,'','','',' having '||p_match_having_condition);
		    lv_sql = lv_sql||') group by '||p_match_groupby_names;
		    lv_sql = lv_sql||' having';
		    FOR I IN 1..regexp_count(','||p_match_column_names, ',')
		    LOOP lv_sql = lv_sql||' sum(agg_col_'||i||') < 0 or'; END LOOP;
		else
            lv_sql = '';
        end if;
		
		lv_sql = lv_sql||' limit 10';
		lv_sql = 'select listagg(error_data, ''/n'') within group (order by error_data) from ('||lv_sql||')';
		EXECUTE lv_sql INTO lv_result_error_data;
		lv_result_error_data = replace(lv_result_error_data, '/n', chr(10));
        lv_result_error_data = p_groupby_names||
		  decode(lv_mode,'numeric increment','',',kind')||
		  chr(10)||lv_result_error_data;

        CALL {{currentschema}}.proc_insert_test_result(lv_test_type,p_test_group,p_test_action,
		p_test_description, p_test_time, lv_starttime,
		p_order_run, p_schema_name, p_table_name,p_column_names,p_skip_errors,p_subset_condition,
        lv_input_parameters, 0, lv_result_message,NULL, lv_result_error_count,lv_result_error_data, lv_result_query);
    end if;
EXCEPTION WHEN others THEN
RAISE EXCEPTION 'Test "%" for %.% column(s) %, % fails. [%] % ',lv_test_type,p_schema_name, p_table_name,p_column_names, lv_input_parameters, SQLSTATE,SQLERRM;
END;
$$;
