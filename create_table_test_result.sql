--drop schema if exists {{currentschema}} cascade;
--create schema {{currentschema}};

SET search_path TO {{currentschema}};
/*
drop table if exists test_results cascade;
create table test_results(
    test_type varchar(50),
    test_group varchar(200),
	test_action varchar(100),
	test_time timestamp,
	starttime timestamp,
    endtime timestamp,
    order_run varchar(50),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
	subset_condition varchar(500),
    input_parameters varchar(1000),
    result_code int,
    result_message varchar(500),
    result_row_count int,
    result_error_count int,
    result_error_data varchar(4000),
	result_query varchar(4000),
	test_description varchar(1000)
);
*/
/*
alter table test_results add column test_description varchar(1000);
*/