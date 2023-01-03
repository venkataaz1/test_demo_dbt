--seed ref
with customer as (
     select * from {{ref('Input_customer')}}
),
orders as (
      select * from {{ref('Input_orders')}}
),
nation as (
      select * from {{ref('Input_nation')}}
),

region as (
      select * from {{ref('Input_region')}}
),
final as (
select n_name as Nation,
       r_name as Region,
       c_name as Customer,
       count(o_orderkey) as Total_no_of_orders,
       max(o_orderdate) as Most_recent_order,
       sum(o_totalprice) as Total_price_of_orders
from customer cust
inner join orders ord on cust.c_custkey = ord.o_custkey
inner join nation nat on cust.c_nationkey = nat.n_nationkey
inner join region reg on nat.n_regionkey = reg.r_regionkey
group by n_name,r_name,c_name
       
)

select * 
from final