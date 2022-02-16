SET search_path TO {{currentschema}};

--jonathan HM
--drop schema if exists qc_jarena cascade;
--create schema qc_jarena;
--set search_path to qc_jarena;


--bwalter PTD
--drop schema if exists qc_bwalter cascade;
--create schema qc_bwalter;
--set search_path to qc_bwalter;


/*
drop table if exists primary_key_test_set cascade;
create table primary_key_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
	subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists uniqueness_test_set cascade;
create table uniqueness_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
	subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists not_null_test_set cascade;
create table not_null_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
	subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists allowed_increment_test_set cascade;
create table allowed_increment_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
	delta_record_count int,
	subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists data_match_test_set cascade;
create table data_match_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
    match_schema_name varchar(100),
    match_table_name varchar(100),
    match_column_names varchar(200),
	mode varchar(20),
	subset_condition varchar(500),
	match_subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists prior_match_test_set cascade;
create table prior_match_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
    schema_prior varchar(100),
	mode varchar(20),
	subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists value_match_test_set cascade;
create table value_match_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
    value_set varchar(500),
	mode varchar(20),
	subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists window_match_test_set cascade;
create table window_match_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
    date_column varchar(100),
	window_days int,
	mode varchar(20),
	subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists condition_check_test_set cascade;
create table condition_check_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
    condition varchar(1000),
	subset_condition varchar(500),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists aggregate_match_test_set cascade;
create table aggregate_match_test_set(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
	subset_condition varchar(500),
	groupby_names varchar(200),
	having_condition varchar(500),
    match_schema_name varchar(100),
    match_table_name varchar(100),
    match_column_names varchar(200),
	match_subset_condition varchar(500),
	match_groupby_names varchar(200),
	match_having_condition varchar(500),
	mode varchar(20),
	test_disable smallint,
	check_result varchar(500)
);

drop table if exists custom_query_test_set cascade;
create table custom_query_test_set(
    test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
    schema_name varchar(100),
    table_name varchar(100),
    column_names varchar(200),
	skip_errors int,
    query varchar(4000),
	test_disable smallint,
	check_result varchar(500)
);
*/

/*
{% if redshift.hostname == 'celgene-hemonc-ptd.c5em0n6rnjwh.us-east-1.redshift.amazonaws.com' %}
REVOKE ALL ON SCHEMA {{currentschema}} from bwalter CASCADE;
GRANT ALL ON SCHEMA qc_bwalter TO bwalter;
GRANT ALL ON ALL TABLES IN SCHEMA qc_bwalter TO bwalter;
GRANT SELECT ON TABLE {{currentschema}}.test_results to bwalter;
GRANT SELECT ON TABLE {{currentschema}}.test_groups to bwalter;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA {{currentschema}} TO bwalter;
{% endif %}

{% if redshift.hostname == 'celgene-hemonc.c5em0n6rnjwh.us-east-1.redshift.amazonaws.com' %}
REVOKE ALL ON SCHEMA {{currentschema}} from jonathan CASCADE;
GRANT ALL ON SCHEMA qc_jarena TO jonathan;
GRANT ALL ON ALL TABLES IN SCHEMA qc_jarena TO jonathan;
GRANT SELECT ON TABLE {{currentschema}}.test_results to jonathan;
GRANT SELECT ON TABLE {{currentschema}}.test_groups to jonathan;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA {{currentschema}} TO jonathan;

REVOKE ALL ON SCHEMA {{currentschema}} from csahni CASCADE;
GRANT ALL ON SCHEMA {{currentschema}} TO csahni;
GRANT ALL ON ALL TABLES IN SCHEMA {{currentschema}} TO csahni;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA {{currentschema}} TO csahni;
{% endif %}
*/

/*
alter table primary_key_test_set add column subset_condition varchar(500);
alter table uniqueness_test_set add column subset_condition varchar(500);
alter table not_null_test_set add column subset_condition varchar(500);
alter table allowed_increment_test_set add column subset_condition varchar(500);
alter table data_match_test_set add column subset_condition varchar(500);
alter table data_match_test_set add column match_subset_condition varchar(500);
alter table prior_match_test_set add column subset_condition varchar(500);
alter table value_match_test_set add column subset_condition varchar(500);
alter table window_match_test_set add column subset_condition varchar(500);
alter table condition_check_test_set add column subset_condition varchar(500);
*/

/*
alter table primary_key_test_set add column test_disable smallint;
alter table uniqueness_test_set add column test_disable smallint;
alter table not_null_test_set add column test_disable smallint;
alter table allowed_increment_test_set add column test_disable smallint;
alter table data_match_test_set add column test_disable smallint;
alter table prior_match_test_set add column test_disable smallint;
alter table value_match_test_set add column test_disable smallint;
alter table window_match_test_set add column test_disable smallint;
alter table condition_check_test_set add column test_disable smallint;
alter table aggregate_match_test_set add column test_disable smallint;
*/