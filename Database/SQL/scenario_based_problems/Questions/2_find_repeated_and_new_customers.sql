/*
We have purchase details, Find the repeated and new customer each day

input:
=====
order_id,customer_id,order_date,order_amount
1	100	2022-01-01	2000
2	200	2022-01-01	2500
3	300	2022-01-01	2100
4	100	2022-01-02	2000
5	400	2022-01-02	2200
6	500	2022-01-02	2700
7	100	2022-01-03	3000
8	400	2022-01-03	1000
9	600	2022-01-03	3000


Expected ouput:
===============
order_date,New_customers,Repeated_customers
2022-01-01	3	0
2022-01-02	2	1
2022-01-03	1	2

*/
create table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
);


insert into customer_orders values(1,100,cast('2022-01-01' as date),2000),
  (2,200,cast('2022-01-01' as date),2500),
  (3,300,cast('2022-01-01' as date),2100),
  (4,100,cast('2022-01-02' as date),2000),
  (5,400,cast('2022-01-02' as date),2200),
  (6,500,cast('2022-01-02' as date),2700),
  (7,100,cast('2022-01-03' as date),3000),
  (8,400,cast('2022-01-03' as date),1000),
  (9,600,cast('2022-01-03' as date),3000);

select * from customer_orders
