SET search_path TO {{currentschema}};
/*
create or replace procedure proc_qc_rename_test_group(
	p_old_test_group varchar(200),
	p_new_test_group varchar(200),
	p_table_list varchar(4000)
)
language plpgsql
as $$
DECLARE
  rec RECORD;
  lv_old_test_group varchar(200) = lower(p_old_test_group);
BEGIN
  
	-- primary_key
	update {{currentschema}}.primary_key_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');
  
  -- uniqueness
  	update {{currentschema}}.uniqueness_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');

  -- not_null
  	update {{currentschema}}.not_null_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');
  
  -- allowed_increment
  	update {{currentschema}}.allowed_increment_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');

  -- data_match
  	update {{currentschema}}.data_match_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');

  -- aggregate_match
  	update {{currentschema}}.aggregate_match_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');

  -- prior_match
  	update {{currentschema}}.prior_match_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');
  
  -- value_match
  	update {{currentschema}}.value_match_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');
  
  -- window_match
  	update {{currentschema}}.window_match_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');
  
  -- condition_check
  	update {{currentschema}}.condition_check_test_set
	set test_group = p_new_test_group
	where lower(test_group) = lv_old_test_group
	  and (','||replace(p_table_list,' ','')||',' ilike '%,'||trim(table_name)||',%'
	  	or nvl(p_table_list,'') = '');
  
END;
$$;
*/