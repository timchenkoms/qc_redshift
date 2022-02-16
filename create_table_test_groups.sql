SET search_path TO {{currentschema}};
/*
drop table if exists test_groups cascade;
create table test_groups(
	test_group varchar(200),
	test_description varchar(1000),
	test_action varchar(100),
	email_list varchar(200),
	email_slack varchar(100),
	wiki_link varchar(200),
	wiki_page_id bigint,
	confluence_space varchar(10),
	variation_link varchar(200),
	test_set_schema varchar(100)
);
*/

--alter table test_groups add column test_set_schema varchar(100);

--drop view if exists v_test_groups_wiki;
create or replace view v_test_groups_wiki(test_group, test_description, test_action, email_list, email_slack, wiki_link, variation_link)
as
select test_group,
       test_description,
       test_action,
       email_list,
	   email_slack,
       '<a href="'||wiki_link||'">'||wiki_link||'</a>' as wiki_link,
	   '<a href="'||variation_link||'">'||variation_link||'</a>' as variation_link
from {{currentschema}}.test_groups;
