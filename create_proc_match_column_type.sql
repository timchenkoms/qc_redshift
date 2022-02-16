SET search_path TO {{currentschema}};

create or replace procedure proc_match_column_type(
    p_schema_name varchar(100),
    p_table_name varchar(100),
    p_column_names varchar(200),
    p_match_schema_name varchar(100),
    p_match_table_name varchar(100),
    p_match_column_names varchar(200),
    p_result_code inout int,
    p_result_message inout varchar(500)
)
language plpgsql
as
$$
DECLARE
    lv_column_types varchar(500) = '';
    lv_match_column_types varchar(500) = '';
    lv_column_type varchar(100);
    lv_column_name varchar(100);
BEGIN
    if regexp_count(p_column_names, ',') <> regexp_count(p_match_column_names, ',') then
        p_result_message = 'The number of columns ('||p_column_names||') of the table '||p_schema_name||'.'||p_table_name;
        p_result_message = p_result_message||' is not equal to the number of columns ('||p_match_column_names||') of the table '||p_match_schema_name||'.'||p_match_table_name||'.';
        p_result_code = 0;
        return;
    end if;

    FOR I IN 1..regexp_count(','||p_column_names, ',')
    LOOP
        lv_column_name = ltrim(regexp_substr(','||p_column_names,',[^,]*',1,I),',');

        select
		  case when data_type in ('char','varchar','character','name',
								  'text','interval') then 'character'
               when data_type in ('abstime','date','timestamp','time') then 'timestamp'
               when data_type in ('int','decimal','double','bigint','integer',
								  'numeric','smallint','real','tid','xid') then 'integer'
               else data_type
	      end
        into lv_column_type
        from (select table_schema, table_name, column_name,
replace(substring(data_type, 1, decode(position(' ' in data_type),0,length(data_type),position(' ' in data_type)-1)),'"','') as data_type from SVV_COLUMNS) SVV_COLUMNS
        where table_schema = trim(lower(p_schema_name))
            and table_name = trim(lower(p_table_name))
            and column_name = trim(lower(lv_column_name));

        lv_column_types =  lv_column_types||','||nvl(lv_column_type,'NULL');
    END LOOP;

    FOR I IN 1..regexp_count(','||p_match_column_names, ',')
    LOOP
        lv_column_name = ltrim(regexp_substr(','||p_match_column_names,',[^,]*',1,I),',');

        select
		  case when data_type in ('char','varchar','character','name',
								  'text','interval') then 'character'
               when data_type in ('abstime','date','timestamp','time') then 'timestamp'
               when data_type in ('int','decimal','double','bigint','integer',
								  'numeric','smallint','real','tid','xid') then 'integer'
               else data_type
	      end
        into lv_column_type
        from (select table_schema, table_name, column_name,
replace(substring(data_type, 1, decode(position(' ' in data_type),0,length(data_type),position(' ' in data_type)-1)),'"','') as data_type from SVV_COLUMNS) SVV_COLUMNS
        where table_schema = trim(lower(p_match_schema_name))
          and table_name = trim(lower(p_match_table_name))
          and column_name = trim(lower(lv_column_name));

        lv_match_column_types =  lv_match_column_types||','||nvl(lv_column_type,'NULL');
    END LOOP;

	if lv_column_types <> lv_match_column_types then
        p_result_message = 'The data type list of columns ('||p_column_names||') of the table '||p_schema_name||'.'||p_table_name;
        p_result_message = p_result_message||' is not equal to the data type list of columns ('||p_match_column_names||') of the table '||p_match_schema_name||'.'||p_match_table_name||'.';
        p_result_code = 0;
        return;
    else
	    p_result_code = 1;
    end if;
END;
$$