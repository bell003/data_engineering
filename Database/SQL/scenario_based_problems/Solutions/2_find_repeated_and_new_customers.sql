--First identify the first order date for each customer and join with original table

with first_date_of_customer as (
  select customer_id,
         min(order_date) as first_date
  from customer_orders
  group by customer_id
),
value_flag as (
  select c.*,
         f.first_date,
         case when c.order_date = f.first_date then 1 else 0 end as new_customers,
         case when c.order_date != f.first_date then 1 else 0 end as repeated_customer
  from customer_orders c
  join first_date_of_customer f on f.customer_id = c.customer_id
)
select order_date,
       sum(new_customers) as new_customer,
       sum(repeated_customer) as repeated_customer
from value_flag
group by order_date;
