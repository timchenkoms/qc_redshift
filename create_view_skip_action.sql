SET search_path TO {{currentschema}};
/*
DROP VIEW IF EXISTS v_skip_action CASCADE;

create or replace view v_skip_action(test_group, action) as
select test_group, action
from test_groups;
*/
