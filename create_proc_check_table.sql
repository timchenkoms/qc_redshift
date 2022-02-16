SET search_path TO {{currentschema}};

create or replace procedure proc_check_table(
    p_schema_name varchar(100),
    p_table_name varchar(100),
    p_column_names varchar(200),
    p_result_code inout int,
    p_result_message inout varchar(500)
)
language plpgsql
as
$$
DECLARE
    lv_sql varchar(4000);
    lv_column_names varchar(200);
    lv_count_result int;
BEGIN

    select count(1)
	into lv_count_result
	from SVV_COLUMNS
	where table_schema = trim(lower(p_schema_name))
	  and table_name = trim(lower(p_table_name))
	  and (p_column_names is null or
	  position(','||column_name||',' in ','||replace(lower(p_column_names), ' ','')||',') > 0);
    
	if p_column_names is NULL then
        if lv_count_result = 0 then
            p_result_code = 0;
            p_result_message = 'Table '||p_schema_name||'.'||p_table_name||' does not exist.';
        else
            p_result_code = 1;
        end if;
    else
        if lv_count_result <> REGEXP_COUNT(p_column_names, ',') + 1 then
            p_result_code = 0;
            p_result_message = 'Table '||p_schema_name||'.'||p_table_name||' or columns '||p_column_names||' do not exist.';
        else
            p_result_code = 1;
        end if;
    end if;
END;
$$