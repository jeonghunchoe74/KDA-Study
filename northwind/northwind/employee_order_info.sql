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

