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