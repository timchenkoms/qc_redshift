drop schema if exists tmp_qc_framework_examples_current cascade;
create schema tmp_qc_framework_examples_current;
drop schema if exists tmp_qc_framework_examples_last cascade;
create schema tmp_qc_framework_examples_last;

SET search_path TO tmp_qc_framework_examples_current;

drop table if exists cities cascade;
create table cities (city_id int, city_name varchar(100));
insert into cities values (1, 'Boston');
insert into cities values (2, 'Worcester');
insert into cities values (3, 'Cambridge');
insert into cities values (4, 'Springfield');
drop table if exists tmp_qc_framework_examples_last.cities cascade;
select * into tmp_qc_framework_examples_last.cities from cities;


drop table if exists orders_history cascade;
create table orders_history(customer varchar(100), city_id int, product_name varchar(100), order_time timestamp, product_amount int, product_price float, order_total float, order_status varchar(20));
insert into orders_history values('Joel Davis',2,'headphones', getdate()-10,1, 190.0, 190.0,'delivered');
insert into orders_history values('Joel Davis',2,'keyboard', getdate()-10,1, 60.0, 60.0,'delivered');
insert into orders_history values('Joel Davis',2,'mouse', getdate()-10,1, 25.0, 25.0,'delivered');
insert into orders_history values('Joel Davis',2,'laptop', getdate()-10,1, 900.0, 900.0,'delivered');
insert into orders_history values('Eric Wilson',3,'keyboard', getdate()-9,10, 50.0, 500.0,'canceled');
insert into orders_history values('Richard Thomas',1,'laptop', getdate()-12,1, 900.0, 900.0,'delivered');

drop table if exists tmp_qc_framework_examples_last.orders_history cascade;
select * into tmp_qc_framework_examples_last.orders_history from orders_history;

drop table if exists tmp_qc_framework_examples_last.orders cascade;
select * into tmp_qc_framework_examples_last.orders from orders_history;

drop table if exists orders cascade;
create table orders (like orders_history);
insert into orders values('Jack Simpson',1,'headphones', getdate()-1,1, 200.0,200.0,'paid');
insert into orders values('Jack Simpson',1,'keyboard', getdate()-3,1, 50.0,50.0,'delivered');
insert into orders values('Jack Simpson',1,'mouse', getdate()-5,1, 25.0,25.0,'canceled');
insert into orders values('Nick Johnson',2,'headphones', getdate()-3,1, 200.0,200.0,'delivered');
insert into orders values('Mary Brown',2,'headphones', getdate(),1, 200.0,200.0,'paid');
insert into orders values('Danniel Miller',3,'laptop', getdate()-4,1, 1000.0,1000.0,'delivered');
insert into orders_history (select * from orders);

drop view if exists v_anual_sales cascade;
create or replace view v_anual_sales as
select to_char(order_time,'yyyy') order_year, sum(order_total) order_year_total
from tmp_qc_framework_examples_current.orders_history
group by to_char(order_time,'yyyy');
drop view if exists tmp_qc_framework_examples_last.v_anual_sales cascade;
create or replace view tmp_qc_framework_examples_last.v_anual_sales as
select to_char(order_time,'yyyy') order_year, sum(order_total) order_year_total
from tmp_qc_framework_examples_last.orders_history
group by to_char(order_time,'yyyy');

SET search_path TO {{currentschema}};

delete from test_results where test_group = 'examples';

CALL proc_qc_not_null(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','cities','city_id, city_name',0,null);

CALL proc_qc_uniqueness(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','cities','city_name',0,null);

CALL proc_qc_primary_key(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','cities','city_id',0,null);

call proc_qc_allowed_increment(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders_history', null,0);

CALL proc_qc_window_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders_history','product_name',0, 'city_id in (''2'',''3'')','order_time',7,'same');

CALL proc_qc_window_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders_history','product_name',0, 'city_id = ''1''','order_time',7,'no-drops');

call proc_qc_prior_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders','product_name',0, 'city_id in (''2'',''3'')','tmp_qc_framework_examples_last','same');

call proc_qc_prior_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders','product_name',0,'city_id = ''1''', 'tmp_qc_framework_examples_last','no-drops');

call proc_qc_value_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders','order_status',0,'order_time > getdate()-365', '''delivered'',''paid'', ''canceled''', 'in');

call proc_qc_value_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders','order_status',0, 'order_time > getdate()-365', 'null,''n/a''', 'not in');

call proc_qc_value_increment(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','v_anual_sales','order_year',0,null,'tmp_qc_framework_examples_last','order_year_total');

call proc_qc_data_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders','product_name',0,null,
'tmp_qc_framework_examples_current','orders_history','product_name',null,'same');

call proc_qc_data_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders_history','customer',0,null,
'tmp_qc_framework_examples_current','orders','customer',null,'no-drops');

call proc_qc_data_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders','city_id',0,null,
'tmp_qc_framework_examples_current','cities','city_id',null,'foreign key');

call proc_qc_condition_check(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders','order_time, product_name, product_price, product_amount, order_total',0,null,'product_price * product_amount = order_total');

call proc_qc_aggregate_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders_history','count(distinct product_name)',0,'order_time < getdate()-28','city_id',null,
'tmp_qc_framework_examples_current','orders_history','count(distinct product_name)','order_time < getdate()-28','city_id',null,'same');

call proc_qc_aggregate_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders_history','count(distinct product_name)',0,'order_time < getdate()-28','city_id',null,
'tmp_qc_framework_examples_current','orders_history','count(distinct product_name)','order_time < getdate()-28','city_id',null,'no-drops');

call proc_qc_custom_query(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders','city_id',0,
'select c.city_id as error_data from tmp_qc_framework_examples_current.cities c left join (select * from tmp_qc_framework_examples_current.orders_history where order_time > getdate()-28) o on c.city_id = o.city_id group by c.city_id having sum(nvl(o.order_total,0)) = 0');

call proc_qc_aggregate_match(
'examples','','','{{now}}','{{CurrentOrderRunId}}',
'tmp_qc_framework_examples_current','orders_history','sum(order_total)',0,null,'city_id',null, 'tmp_qc_framework_examples_current','orders_history','sum(order_total)',null,'city_id',null,'numeric increment');

--drop schema if exists tmp_qc_framework_examples_current cascade;
--drop schema if exists tmp_qc_framework_examples_last cascade;