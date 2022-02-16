SET search_path TO {{qc_schema}};

drop view if exists v_test_set_all CASCADE;
create or replace view v_test_set_all(
    test_type, test_group, test_description, test_action, schema_name, table_name,
	column_names, skip_errors, subset_condition, match_schema_name, match_table_name,
	match_column_names, mode, number_value, text_value, test_disable, check_result)
as
select 'DATA MATCH' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       match_schema_name, match_table_name, match_column_names, mode, null number_value, match_subset_condition text_value, test_disable, check_result
from {{currentschema}}.data_match_test_set
union all select 'PRIOR MATCH' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       schema_prior as match_schema_name, null as match_table_name, null as match_column_names, mode, null number_value, null text_value, test_disable, check_result
from {{currentschema}}.prior_match_test_set
union all select 'ALLOWED INCREMENT' as test_type, test_group, test_description, test_action, schema_name, table_name, null as column_names, null as skip_errors, subset_condition,
       null match_schema_name, null match_table_name, null match_column_names, null as mode, delta_record_count number_value, null text_value, test_disable, check_result
from {{currentschema}}.allowed_increment_test_set
union all select 'CONDITION CHECK' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       null match_schema_name, null match_table_name, null match_column_names, null as mode, null number_value, condition text_value, test_disable, check_result
from {{currentschema}}.condition_check_test_set
union all select 'VALUE MATCH' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       null match_schema_name, null match_table_name, null match_column_names, mode, null number_value, value_set text_value, test_disable, check_result
from {{currentschema}}.value_match_test_set
union all select 'NOT NULL' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       null match_schema_name, null match_table_name, null match_column_names, null as mode, null number_value, null text_value, test_disable, check_result
from {{currentschema}}.not_null_test_set
union all select 'PRIMARY KEY' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       null match_schema_name, null match_table_name, null match_column_names, null as mode, null number_value, null text_value, test_disable, check_result
from {{currentschema}}.primary_key_test_set
union all select 'UNIQUENESS' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       null match_schema_name, null match_table_name, null match_column_names, null as mode, null number_value, null text_value, test_disable, check_result
from {{currentschema}}.uniqueness_test_set
union all select 'WINDOW MATCH' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       null match_schema_name, null match_table_name, date_column match_column_names, mode, window_days number_value, null text_value, test_disable, check_result
from {{currentschema}}.window_match_test_set
union all select 'AGGREGATE MATCH' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, subset_condition,
       match_schema_name, match_table_name, match_column_names, mode, null number_value, 
'groupby_names = '||nvl(groupby_names,'')||
', having_condition = '||nvl(having_condition,'')||
', match_subset_condition = '||nvl(match_subset_condition,'')||
', match_groupby_names = '||nvl(match_groupby_names,'')||
', match_having_condition = '||nvl(match_having_condition,'') text_value, test_disable, check_result
from {{currentschema}}.aggregate_match_test_set
union all select 'CUSTOM QUERY' as test_type, test_group, test_description, test_action, schema_name, table_name, column_names, skip_errors, null subset_condition,
       null match_schema_name, null match_table_name, null match_column_names, null as mode, null number_value, query text_value, test_disable, check_result
from {{currentschema}}.custom_query_test_set
;