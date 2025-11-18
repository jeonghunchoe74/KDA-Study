
select c.category_id, c.category_name
from northwind.products p , northwind.categories c 
where p.category_id = c.category_id 

select p.category_id , c.category_name , count(*) as product_cnt,
       count(case when discontinued = 0 then discontinued end) as discontinued0,
       count(case when discontinued = 1 then discontinued end) as discontinued1
from northwind.products p , northwind.categories c 
where p.category_id = c.category_id
group by 1,2
order by 4 desc;

select category_id , category_name, product_cnt,
		(select count(*) from products 
		where category_id = a.category_id and discontinued = 0) as discontinued0,
		(select count(*) from products 
		where category_id = a.category_id and discontinued = 1) as discontinued1
from(
	select p.category_id , c.category_name , count(*) as product_cnt
	from northwind.products p , northwind.categories c 
	where p.category_id = c.category_id
	group by 1,2
	order by 3 desc
)a
order by 4 desc;

select max(unit_price), min(unit_price), avg(unit_price), 
	   percentile_cont(0.5) within group (order by unit_price) as median
from northwind.products p , northwind.categories c 
where p.category_id = c.category_id 

WITH
order_info as (
select o.order_id , o.order_date , 
	   od.unit_price , od.quantity , od.discount ,
	   to_char(o.order_date, 'YYYY') as year,
	   to_char(o.order_date, 'MM') as month,
	   to_char(o.order_date, 'DD') as day,
	   date_part('dow',o.order_date) as dow, 
	   date_part('quarter',o.order_date) as quarter, 
	   od.unit_price * od.quantity * (1-od.discount) as amount
from northwind.orders o , northwind.order_details od 
where o.order_id = od.order_id 
order by 1
)
select *,
	   concat(year,'-',month) as year_month,
	   lag(sum_amount) over() as pre_amount,
	   sum_amount-lag(sum_amount) over() as increase
from(
	select year, month, sum(amount) sum_amount
	from order_info
	group by 1,2
	order by 1,2
)a;



WITH
product_order_info as (
select o.order_id , o.order_date , od.product_id ,p.product_name, p.category_id , c.category_name ,	   
	   od.unit_price , od.quantity , od.discount , p.discontinued,
	   to_char(o.order_date, 'YYYY') as year,
	   to_char(o.order_date, 'MM') as month,
	   to_char(o.order_date, 'DD') as day,
	   date_part('dow',o.order_date) as dow, 
	   date_part('quarter',o.order_date) as quarter, 
	   od.unit_price * od.quantity * (1-od.discount) as amount
from northwind.orders o , northwind.order_details od , northwind.products p , northwind.categories c 
where o.order_id = od.order_id 
and od.product_id = p.product_id 
and p.category_id = c.category_id 
order by 1
)
select *
from(
	select *
		, rank() over(partition by category_id order by sum_amount desc) as rank
	from(
		select category_id, category_name, product_id, product_name, discontinued, 
			sum(amount) as sum_amount 
		from product_order_info
		group by 1,2,3,4,5
	)a
)b
where rank<=2;

WITH
customer_order_info as (
select o.order_id , o.order_date , c.customer_id, od.product_id ,
	   od.unit_price , od.quantity , od.discount ,
	   to_char(o.order_date, 'YYYY') as year,
	   to_char(o.order_date, 'MM') as month,
	   to_char(o.order_date, 'DD') as day,
	   date_part('dow',o.order_date) as dow, 
	   date_part('quarter',o.order_date) as quarter, 
	   od.unit_price * od.quantity * (1-od.discount) as amount
from northwind.orders o , northwind.order_details od, northwind.customers c  
where o.order_id = od.order_id 
and o.customer_id = c.customer_id 
order by 1
)
select  customer_id, 
	    string_agg(distinct product_id::varchar(2),',') as product_ids, 
	    sum(amount) as sum_amount
from customer_order_info
group by 1
order by 3 desc;

WITH
product_order_info as (
select o.order_id , o.order_date , od.product_id ,p.product_name, p.category_id , c.category_name ,	   
	   od.unit_price , od.quantity , od.discount , p.discontinued,
	   to_char(o.order_date, 'YYYY') as year,
	   to_char(o.order_date, 'MM') as month,
	   to_char(o.order_date, 'DD') as day,
	   date_part('dow',o.order_date) as dow, 
	   date_part('quarter',o.order_date) as quarter, 
	   od.unit_price * od.quantity * (1-od.discount) as amount
from northwind.orders o , northwind.order_details od , northwind.products p , northwind.categories c 
where o.order_id = od.order_id 
and od.product_id = p.product_id 
and p.category_id = c.category_id 
order by 1
)
select * from product_order_info;

WITH
employee_order_info as (
select o.order_id , o.order_date , o.employee_id, e.hire_date , e.country , e.city ,
	   od.unit_price , od.quantity , od.discount ,
	   to_char(o.order_date, 'YYYY') as year,
	   to_char(o.order_date, 'MM') as month,
	   to_char(o.order_date, 'DD') as day,
	   date_part('dow',o.order_date) as dow, 
	   date_part('quarter',o.order_date) as quarter, 
	   od.unit_price * od.quantity * (1-od.discount) as amount
from northwind.orders o , northwind.order_details od , northwind.employees e
where o.order_id = od.order_id 
and o.employee_id = e.employee_id 
order by 1
)
select * ,
	   sum(amount_rate) over(rows between UNBOUNDED preceding and current row) as rate_cumsum
from(
	select *,
		rank() over(order by  sum_amount desc) as rank,
		sum(sum_amount) over() total_amount,
		sum_amount/sum(sum_amount) over()*100 as amount_rate
	from(
		select employee_id , hire_date, country, city, sum(amount) sum_amount
		from employee_order_info
		group by 1,2,3,4
	)a
	order by 6
)b;

WITH
supplier_order_info as (
select o.order_id , o.order_date , o.employee_id, 
	   od.unit_price , od.quantity , od.discount ,s.supplier_id , s.city, s.country ,
	   to_char(o.order_date, 'YYYY') as year,
	   to_char(o.order_date, 'MM') as month,
	   to_char(o.order_date, 'DD') as day,
	   date_part('dow',o.order_date) as dow, 
	   date_part('quarter',o.order_date) as quarter, 
	   od.unit_price * od.quantity * (1-od.discount) as amount
from northwind.orders o , northwind.order_details od , 
	 northwind.products p, northwind.suppliers s
where o.order_id = od.order_id 
and od.product_id = p.product_id  and p.supplier_id = s.supplier_id 
order by 1
)
select *,
	sum(amount_rate) over(rows between unbounded preceding and current row) as rate_cumsum
from (
	select *,
		   sum(sum_amount) over() as total_amount,
		   sum_amount/sum(sum_amount) over()*100 as amount_rate
	from(
		select supplier_id, city, country, sum(amount) as sum_amount
		from supplier_order_info
		group by 1,2,3
		order by 4 desc
	)b
)c;



select * from northwind.suppliers