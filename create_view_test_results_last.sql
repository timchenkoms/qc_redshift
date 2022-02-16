SET search_path TO {{currentschema}};

drop view if exists v_test_results_last cascade;
create or replace view v_test_results_last(
    test_type, test_group, test_time, starttime, endtime, order_run,
    schema_name, table_name, column_names, skip_errors, subset_condition, input_parameters,
    result_code, result_status, result_message, result_row_count, result_error_count, result_error_data, test_action, test_description, result_query
    ) as 
select tr.test_type, tr.test_group, tr.test_time, tr.starttime, tr.endtime, tr.order_run,
       tr.schema_name, tr.table_name, tr.column_names, tr.skip_errors, tr.subset_condition,
	   tr.input_parameters, 
	   CASE
	   WHEN lower(','||replace(nvl(tr.test_action,''),' ','')||',') like '%,except,%' 
	   		and tr.result_code = 0 THEN -0.5
	   ELSE tr.result_code END as result_code, 
	   CAST(CASE
	   WHEN tr.result_code = 1 THEN 'Success'
       WHEN tr.result_code = -1 THEN 'Error'
	   WHEN lower(','||replace(nvl(tr.test_action,''),' ','')||',') like '%,except,%' 
	   		and tr.result_code = 0 THEN 'Exception'
	   WHEN tr.result_code = 0 THEN 'Fail'
	   ELSE 'Unknown' END as varchar(20))as result_status,
	   tr.result_message, tr.result_row_count, tr.result_error_count, 
	   tr.result_error_data||
	   case when tr.result_error_count > 10 then chr(10)||'(more)' else '' end as result_error_data,
	   tr.test_action,
	   tr.test_description,
	   tr.result_query
from {{currentschema}}.test_results tr
where tr.test_time = (select max(test_time) from {{currentschema}}.test_results tr_last
					 where nvl(lower(tr.test_group),'xxx') = nvl(lower(tr_last.test_group),'xxx'))
;

drop view if exists v_test_results_prepare_for_wiki cascade;
create or replace view v_test_results_prepare_for_wiki(
    test_type, test_group, test_time, starttime, endtime, order_run,
    schema_name, table_name, column_names, skip_errors, input_parameters,
    result_code, result_status, result_message, result_row_count, result_error_count, result_error_data, test_action, test_description, row_number, result_query, post_on_wiki
    ) as
select lower(tr.test_type), lower(tr.test_group), 
       trunc(tr.test_time), tr.starttime, tr.endtime, tr.order_run,
       tr.schema_name, tr.table_name, tr.column_names, tr.skip_errors, 
cast('<p>'||ltrim(rtrim({{currentschema}}.get_html_escape(nvl(tr.input_parameters,''))||', '||
decode(tr.subset_condition,null,'','','',{{currentschema}}.get_html_escape('p_subset_condition: '||tr.subset_condition)),', '),', ')||'</p>' as varchar(1000)) as input_parameters,
	   tr.result_code,
	   cast(CASE
	   WHEN tr.result_code = 1    THEN '<p style = "color: green;" ><b>'||tr.result_status||'</b></p>'
       WHEN tr.result_code = -1   THEN '<p style = "color: maroon;" ><b>'||tr.result_status||'</b></p>'
       WHEN tr.result_code = 0    THEN '<p style = "color: red;"   ><b>'||tr.result_status||'</b></p>'
	   WHEN tr.result_code = -0.5 THEN '<p style = "color: orange;"><b>'||tr.result_status||'</b></p>'
	   ELSE tr.result_status
	   END as varchar(200)) as result_status,
	   cast('<p>'||{{currentschema}}.get_html_escape(tr.result_message)||'</p>' as varchar(4000)) as result_message, 
	   tr.result_row_count, tr.result_error_count, 
	   cast('<p>'||replace({{currentschema}}.get_html_escape(tr.result_error_data),chr(10),'<br/>')||'</p>' as varchar(4000)) as result_error_data, 
	   tr.test_action,
	   tr.test_description,
	   row_number() over (partition by tr.test_group, tr.order_run order by tr.result_code, tr.test_type, tr.schema_name, tr.table_name, tr.column_names, tr.input_parameters, tr.subset_condition) as run_number,
	   CASE when tr.result_code < 1 then {{currentschema}}.get_html_escape(nvl(tr.result_query,tr.result_message)) else NULL END as result_query,
case when
	not lower(','||replace(nvl(tg.test_action,''),' ','')||',') ilike '%,nowiki,%'   and
	not lower(','||replace(nvl(tg.test_action,''),' ','')||',') ilike '%,noaction,%' and
	not lower(','||replace(nvl(tr.test_action,''),' ','')||',') ilike '%,nowiki,%'   and
	not lower(','||replace(nvl(tr.test_action,''),' ','')||',') ilike '%,noaction,%' and
(
	lower(','||replace(nvl(tg.test_action,''),' ','')||',') ilike '%,wiki,%' or
	lower(','||replace(nvl(tr.test_action,''),' ','')||',') ilike '%,wiki,%'
) then 1 else 0 end as post_on_wiki
from {{currentschema}}.v_test_results_last tr
left join {{currentschema}}.test_groups tg
on lower(tr.test_group) = lower(tg.test_group)
;

drop view if exists v_test_results_wiki cascade;
create or replace view v_test_results_wiki(
    test_type, test_group, test_time, starttime, endtime, order_run,
    schema_name, table_name, column_names, skip_errors, input_parameters,
    result_code, result_status, result_message, result_row_count, result_error_count, result_error_data, test_action, test_description, row_number, result_query
    ) as 
select
    test_type, test_group, test_time, starttime, endtime, order_run,
    schema_name, table_name, column_names, skip_errors, input_parameters,
    result_code, result_status, result_message, result_row_count, result_error_count,     
	result_error_data, test_action, test_description, row_number, result_query
from {{currentschema}}.v_test_results_prepare_for_wiki
where post_on_wiki = 1;

drop view if exists v_test_results_email cascade;
create or replace view v_test_results_email(
    test_type, test_group, test_time, starttime, endtime, order_run,
    schema_name, table_name, column_names, skip_errors, input_parameters,
    result_code, result_status, result_message, result_row_count, result_error_count, result_error_data, test_action, test_description, row_number, result_query
    ) as 
select tr.test_type, tr.test_group, tr.test_time, tr.starttime, tr.endtime, tr.order_run,
       tr.schema_name, tr.table_name, tr.column_names, tr.skip_errors, 
cast(ltrim(rtrim(nvl(tr.input_parameters,'')||', '||
decode(tr.subset_condition,null,'','','','p_subset_condition: '||tr.subset_condition),', '),', ') as varchar(1000)) as input_parameters,
	   tr.result_code,
	   tr.result_status,
	   tr.result_message, 
	   tr.result_row_count, tr.result_error_count, 
	   replace(tr.result_error_data,chr(10),'<br\>') result_error_data, 
	   tr.test_action,
	   tr.test_description,
	   row_number() over (partition by tr.test_group, tr.order_run order by tr.result_code, tr.test_type, tr.schema_name, tr.table_name, tr.column_names, tr.input_parameters, tr.subset_condition) as run_number,
	   tr.result_query
from {{currentschema}}.v_test_results_last tr
left join {{currentschema}}.test_groups tg
on lower(tr.test_group) = lower(tg.test_group)
where 
	not lower(','||replace(nvl(tg.test_action,''),' ','')||',') ilike '%,noemail,%'   and
	not lower(','||replace(nvl(tg.test_action,''),' ','')||',') ilike '%,noaction,%' and
	not lower(','||replace(nvl(tr.test_action,''),' ','')||',') ilike '%,noemail,%'   and
	not lower(','||replace(nvl(tr.test_action,''),' ','')||',') ilike '%,noaction,%' and
(
	(lower(','||replace(nvl(tg.test_action,''),' ','')||',') ilike '%,email,%' and
	lower(','||replace(nvl(tr.test_action,''),' ','')||',') not ilike '%,email_except,%') or
	lower(','||replace(nvl(tr.test_action,''),' ','')||',') ilike '%,email,%' or
	(lower(','||replace(nvl(tg.test_action,''),' ','')||',') ilike '%,email_except,%'
	 and tr.result_code > -1 and tr.result_code < 1) or
	(lower(','||replace(nvl(tr.test_action,''),' ','')||',') ilike '%,email_except,%'
	 and tr.result_code > -1 and tr.result_code < 1)
);