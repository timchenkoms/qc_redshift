SET search_path TO {{currentschema}};

create or replace procedure proc_qc_master(
	p_test_group varchar(200),
	p_test_time timestamp,
	p_order_run varchar(50)
)
language plpgsql
as $$
DECLARE
  rec RECORD;
  lv_test_group varchar(200) = lower(p_test_group);
  lv_result_count int;
  lv_sql varchar(4000);
  lv_sql_set_search_path varchar(4000);
  lv_test_set_schema varchar(100);
  lv_input_parameters varchar(4000);
  lv_starttime timestamp;
BEGIN

	select count(1)
	into lv_result_count
	from {{currentschema}}.test_groups
	where lower(test_group) = lv_test_group;
	
	if lv_result_count = 1 then
		select test_set_schema
		into lv_test_set_schema
		from {{currentschema}}.test_groups
		where lower(test_group) = lv_test_group;

		SELECT count(1)
		into lv_result_count
		FROM svv_all_schemas
		WHERE lower(schema_name) = lv_test_set_schema;
	
		if lv_result_count = 1 then
			lv_sql_set_search_path = 'SET search_path TO '||lv_test_set_schema;
		else
			lv_test_set_schema = null;
		end if;
	end if;
	
	if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  --- primary_key
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, test_disable, check_result
               FROM primary_key_test_set
              WHERE lower(test_group) = lv_test_group
			  	and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = null;
	  CALL {{currentschema}}.proc_insert_test_result('primary_key',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_primary_key(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors, rec.subset_condition);
	end if;	
  END LOOP;
  
  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- uniqueness
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, test_disable, check_result
               FROM uniqueness_test_set
              WHERE lower(test_group) = lv_test_group
			  	and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = null;
	  CALL {{currentschema}}.proc_insert_test_result('uniqueness',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
  	  call {{currentschema}}.proc_qc_uniqueness(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors, rec.subset_condition);
    end if;
  END LOOP;
  
  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- not_null
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, test_disable, check_result
               FROM not_null_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
    if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = null;
	  CALL {{currentschema}}.proc_insert_test_result('not_null',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_not_null(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors, rec.subset_condition);
    end if;
  END LOOP;
  
  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- allowed_increment
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, delta_record_count, subset_condition, test_disable, check_result
               FROM allowed_increment_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = 'p_delta_record_count = '||nvl(rec.delta_record_count,0);
	  CALL {{currentschema}}.proc_insert_test_result('allowed_increment',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, null, null,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_allowed_increment(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.subset_condition, rec.delta_record_count);
    end if;
  END LOOP;

  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- data_match
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, match_schema_name, match_table_name, match_column_names, match_subset_condition, mode, test_disable, check_result
               FROM data_match_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = 'p_match_schema_name = '||nvl(rec.match_schema_name,'NULL');
      lv_input_parameters = lv_input_parameters||', p_match_table_name = '||nvl(rec.match_table_name,'NULL');
      lv_input_parameters = lv_input_parameters||', p_match_column_names = '||nvl(rec.match_column_names,'NULL');
	  lv_input_parameters = lv_input_parameters||', p_match_subset_condition = '||nvl(rec.match_subset_condition,'NULL');
      lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(rec.mode,'NULL');
	  CALL {{currentschema}}.proc_insert_test_result('data_match',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_data_match(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors, rec.subset_condition,
		rec.match_schema_name, rec.match_table_name, rec.match_column_names,
		rec.match_subset_condition, rec.mode);
    end if;
  END LOOP;

  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- prior_match
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, schema_prior, mode, test_disable, check_result
               FROM prior_match_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = 'p_match_schema_name = '||nvl(rec.schema_prior,'NULL');
      lv_input_parameters = lv_input_parameters||', p_match_table_name = '||nvl(rec.table_name,'NULL');
      lv_input_parameters = lv_input_parameters||', p_match_column_names = '||nvl(rec.column_names,'NULL');
	  lv_input_parameters = lv_input_parameters||', p_match_subset_condition = '||nvl(rec.subset_condition,'NULL');
      lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(rec.mode,'NULL');
	  CALL {{currentschema}}.proc_insert_test_result('prior_match',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_prior_match(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors, rec.subset_condition,
		rec.schema_prior, rec.mode);
    end if;
  END LOOP;
  
  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- value_match
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, value_set, mode, test_disable, check_result
               FROM value_match_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = 'p_value_set = '||nvl(rec.value_set,'NULL');
      lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(rec.mode,'NULL');
	  CALL {{currentschema}}.proc_insert_test_result('value_match',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_value_match(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors, rec.subset_condition,
		rec.value_set, rec.mode);
    end if;
  END LOOP;
  
  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- window_match
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, date_column, window_days, mode, test_disable, check_result
               FROM window_match_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = 'p_window_column = '||nvl(rec.date_column,'NULL')||', p_window_days = '||nvl(rec.window_days,1);
	  lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(rec.mode,'NULL');
	  CALL {{currentschema}}.proc_insert_test_result('window_match',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_window_match(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors, rec.subset_condition,
		rec.date_column, rec.window_days, rec.mode);
    end if;
  END LOOP;
  
  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- condition_check
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, condition, test_disable, check_result
               FROM condition_check_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = 'p_condition = '||nvl(rec.condition,'NULL');
	  CALL {{currentschema}}.proc_insert_test_result('condition_check',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_condition_check(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors, rec.subset_condition,
		rec.condition);
    end if;
  END LOOP;
  
  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
  -- aggregate_match
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, subset_condition, groupby_names, having_condition, match_schema_name, match_table_name, match_column_names, match_subset_condition, match_groupby_names, match_having_condition, mode, test_disable, check_result
               FROM aggregate_match_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = 'p_groupby_names = '||nvl(rec.groupby_names,'NULL');
	  lv_input_parameters = lv_input_parameters||', p_having_condition = '||nvl(rec.having_condition,'NULL');
	  lv_input_parameters = lv_input_parameters||', p_match_schema_name = '||nvl(rec.match_schema_name,'NULL');
      lv_input_parameters = lv_input_parameters||', p_match_table_name = '||nvl(rec.match_table_name,'NULL');
      lv_input_parameters = lv_input_parameters||', p_match_column_names = '||nvl(rec.match_column_names,'NULL');
	  lv_input_parameters = lv_input_parameters||', p_match_subset_condition = '||nvl(rec.match_subset_condition,'NULL');
	  lv_input_parameters = lv_input_parameters||', p_match_groupby_names = '||nvl(rec.match_groupby_names,'NULL');
	  lv_input_parameters = lv_input_parameters||', p_match_having_condition = '||nvl(rec.match_having_condition,'NULL');
      lv_input_parameters = lv_input_parameters||', p_mode = '||nvl(rec.mode,'NULL');
	  CALL {{currentschema}}.proc_insert_test_result('aggregate_match',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		rec.subset_condition, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_aggregate_match(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,
		rec.subset_condition, rec.groupby_names, rec.having_condition, 
		rec.match_schema_name, rec.match_table_name, rec.match_column_names,
		rec.match_subset_condition, rec.match_groupby_names, rec.match_having_condition, rec.mode);
    end if;
  END LOOP;
  
  if lv_test_set_schema is not null then EXECUTE lv_sql_set_search_path; end if;
    -- custom_query
  FOR rec IN SELECT distinct test_group, test_action, test_description, schema_name, table_name, column_names, skip_errors, query, test_disable, check_result
               FROM custom_query_test_set
              WHERE lower(test_group) = lv_test_group
			    and nvl(test_disable,0) <> 1
  LOOP
  	if rec.test_disable = -1 then
      SELECT CONVERT_TIMEZONE('US/Eastern', cast(TIMEOFDAY() as timestamp)) INTO lv_starttime;
	  lv_input_parameters = 'p_query = '||nvl(rec.query,'NULL');
	  CALL {{currentschema}}.proc_insert_test_result('custom_query',rec.test_group, 
		rec.test_action, rec.test_description, p_test_time, lv_starttime,
	    p_order_run, rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,    
		null, lv_input_parameters, -1, rec.check_result,NULL, NULL, NULL,NULL);	  
	else
	  call {{currentschema}}.proc_qc_custom_query(
		rec.test_group, rec.test_action, rec.test_description, p_test_time, p_order_run,
		rec.schema_name, rec.table_name, rec.column_names, rec.skip_errors,
		rec.query);
    end if;
  END LOOP;
  
  SET search_path TO {{currentschema}};
  
END;
$$;
