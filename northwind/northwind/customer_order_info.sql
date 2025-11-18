WITH
customer_order_info as (
	select o.order_id , o.order_date , c.customer_id, od.product_id ,
		   od.unit_price , od.quantity , od.discount , c.country, c.city,
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
select coalesce(country,'---'), coalesce(city,'---')
     , sum(amount)
from customer_order_info
group by rollup(country, city, customer_id)
order by 1,2;


-- 고객별 첫 구매월
, ces_first_order_date as(
select customer_id
     , date_trunc('quarter', min(order_date))::date as first_order_date
from customer_order_info
group by 1
)
-- 월별 첫 구매 고객 수 비율
select to_char(first_order_date,'YYYY-MM'), customer_cnt
	  ,sum(customer_cnt) over() as tot_customer_cnt
	  ,round(customer_cnt/sum(customer_cnt) over()*100,2) as customer_pct
from(
	select first_order_date
	     , count(distinct customer_id) as customer_cnt
	from ces_first_order_date
	group by first_order_date
	order by 1
) a;



, ces_max_date as(
	select *
	     , (select max(order_date) from customer_order_info) as max_date
	from(
		select customer_id, max(order_date) as customer_max_date
		from customer_order_info
		group by 1
	) a
)
, ces_date_diff as(
select customer_id
     , max_date::timestamp - customer_max_date::timestamp as diff
from ces_max_date
)
, ces_churn_yn as(
select *
     , case when diff>'90 days' then 'Y' else 'N' end as churn_yn
from ces_date_diff
)
select b.country, count(*)
from ces_churn_yn a, northwind.customers b
where a.customer_id = b.customer_id
and churn_yn='Y'
group by country
order by 2 desc


select round(churn_cnt/total_cnt::numeric*100,2)::varchar(10)||'%' as churn_rate from (
	select distinct count(*) over() as total_cnt
	     , count(case when churn_yn='Y' then 1 end) over() as churn_cnt
	from ces_churn_yn
) a




select a.*, b.* 
from customer_order_info a, northwind.products b
where a.product_id = b.product_id 





, retention as(
select distinct a.country
	 , a.customer_id as customer1
     , to_char(a.order_date,'YYYY') as date1
     , b.customer_id as customer2
     , to_char(b.order_date,'YYYY') as date2
from customer_order_info a left join customer_order_info b
     						on a.customer_id = b.customer_id
     						and to_char(a.order_date, 'YYYY') = to_char(b.order_date::timestamp+'-1 year','YYYY')
order by 1,2
)
select country, date1
	 , count(customer1) as cnt1
	 , count(customer2) as cnt2
	 , round(count(customer2)/count(customer1)::numeric*100,2) as retention_rate
from retention
group by country, date1
having date1!='1998'
order by 1,2



, country_city_order_info as(
select country, city
     , sum(amount) as sum_amount
     , count(distinct order_id) as order_cnt
     , count(distinct customer_id) as customer_cnt
     , sum(amount)/count(distinct order_id) as avg_order_amount
     , sum(amount)/count(distinct customer_id) as avg_customer_amount
from customer_order_info 
group by 1,2
order by 3 desc
)
select * from country_city_order_info


select *
	 , rank() over(order by avg_order_amount desc) as avg_order_amount_rank
from country_city_order_info 
order by 3 desc

select max(order_date), min(order_date) from northwind.orders

select * from northwind.customers

, country_city_order_info as()

, country_order_info as(
select country
	, sum(amount) as sum_amount
	, count(distinct order_id) as order_cnt
	, count(distinct customer_id) as customer_cnt
	, case 
	 	when lower(country) in ('usa', 'canada', 'mexico') then 'NorthAmerica'
	 	when lower(country) in ('brazil', 'venezuela', 'argentina') then 'SouthAmerica'
	 	else 'Europe'
	 end as country_group
from customer_order_info
group by 1
order by 2 desc
)


, country_group_order_info as(
	select country_group 
		 , sum(sum_amount) as sum_amount
	     , sum(order_cnt) as order_cnt
	     , sum(customer_cnt) as customer_cnt
	from country_order_info
	group by 1
	order by 2 desc
)
select * 
     --, sum(sum_amount) over() as total_amount
     --, sum(order_cnt) over() as total_order_cnt
     --, sum(customer_cnt) over() as total_customer_cnt
     , sum_amount/sum(sum_amount) over() as amount_ratio
     , order_cnt/sum(order_cnt) over() as cnt_ratio
     , customer_cnt/sum(customer_cnt) over() as cnt_ratio
from country_group_order_info
     county_group_order_info;



, country_group_order_info as(
select country_group
	 , sum(sum_amount) as sum_amount
	 , sum(order_cnt) as order_cnt
	 , sum(customer_cnt) as customer_cnt
from country_order_info
group by 1
order by 2 desc
)
select * 
     --, sum(sum_amount) over() as total_amount
     --, sum(order_cnt) over() as total_order_cnt
     --, sum(customer_cnt) over() as total_customer_cnt
     , sum_amount/sum(sum_amount) over() as amount_ratio
     , order_cnt/sum(order_cnt) over() as cnt_ratio
     , customer_cnt/sum(customer_cnt) over() as cnt_ratio
from country_group_order_info;


select * from 


select *
     , sum_amount/order_cnt as avg_order_amount
     , sum_amount/customer_cnt as avg_customer_amount
from
(
	select country
		 , sum(amount) as sum_amount
		 , count(distinct order_id) as order_cnt
		 , count(distinct customer_id) as customer_cnt
	from customer_order_info
	group by 1
	order by 2 desc
)a;

select country, count(customer_id)
from northwind.customers
group by 1


select to_char(min_date,'YYYY-MM') as MIN_MONTH, count(customer_id) as cnt 
from(
	select customer_id, min(order_date) min_date
	from customer_order_info
	group by 1
)a
group by 1
order by 1;


select product_id , count(product_id)
from order_details 
group by 1
order by 2 asc







select * from best_seller_2;


select country
	 , sum(amount) as sum_amount
	 , count(distinct order_id) as order_count
from customer_order_info
group by 1
order by 2 desc;


, customer_rfm as(
	select *
		, max(recent_date) over() max_date
		, max(recent_date) over() - recent_date as recency
	from (
		select customer_id
			 , max(order_date) as recent_date
			 , count(distinct order_id) as frequency
			 , sum(amount) as monetary
		from customer_order_info
		group by 1
	)a
)



select country
	 , sum(amount) as sum_amount
	 , count(distinct order_id) as order_count
from customer_order_info
group by 1
order by 2 desc;

select * from customers;


, customer_rfm_score as(
select * 
	 , case 
	 	when recency<=14 then 5
	 	when recency<=28 then 4
	 	when recency<=60 then 3
	 	when recency<=90 then 2
	 	else 1 	 	
	 end as R	
	 , case
	 	when frequency>=20 then 5
	 	when frequency>=15 then 4
	 	when frequency>=10 then 3
	 	when frequency>=5 then 2
	 	else 1
	 end as F	
	 , case
	 	when monetary>=20000 then 5
	 	when monetary>=10000 then 4
	 	when monetary>=5000 then 3
	 	when monetary>=3000 then 2
	 	else 1 
	 end as m
from customer_rfm
)
select r+f+m as total_score
     , count(*) as cnt
from customer_rfm_score
group by 1,2,3,4
order by total_score desc



select concat('r',r) as r_rank
	 , count(case when f=5 then 1 end) as f1
	 , count(case when f=4 then 1 end) as f2
	 , count(case when f=3 then 1 end) as f3
	 , count(case when f=4 then 1 end) as f4
	 , count(case when f=5 then 1 end) as f5
from customer_rfm_rank
group by r
order by 1 desc



, customer_rfm_total_rank as (
	select customer_id
		 , r+f+m as total_rank
		 , r, f, m 
	from customer_rfm_rank
)
, customer_rfm_total_rank_cnt as(
select total_rank, r, f, m
	 , count(*) as cnt
from customer_rfm_total_rank
group by 1,2,3,4
order by 1 desc, 2,3,4
)
select * from customer_rfm_total_rank_cnt




select total_rank
	 , count(*) as cnt
from customer_rfm_total_rank_cnt
group by 1
order by 1 desc



-- decil
, customer_amount as(
	select customer_id 
		 , sum(amount) as sum_amount
	from customer_order_info
	group by 1
	order by 2 desc
)
, customer_amount_ntile as(
	select *
		 , ntile(10) over(order by sum_amount desc) as decil
	from customer_amount
)
, decil_amount as(
select decil
	 , sum(sum_amount) as decil_sum_amount
from customer_amount_ntile
group by 1
order by 1
)
, decil_amount_rate as(
	select * 
		, sum(decil_sum_amount) over() as total
		, decil_sum_amount/sum(decil_sum_amount) over()*100 as decil_sum_amount_rate
	from decil_amount
)
select *
	 , sum(decil_sum_amount_rate) over(order by decil) as rate_cumsum
from decil_amount_rate



, customer_amount_rate as(
	select * 
		 , sum(sum_amount) over() as total
		 , sum_amount/sum(sum_amount) over()*100 as amount_rate
	from customer_amount
)
, customer_amount_rate_cumsum as(
select * 
	 , sum(amount_rate) over(order by amount_rate desc) as rate_cumsum
from customer_amount_rate
)
select  customer_id , sum_amount, amount_rate, rate_cumsum
	, case 
		when rate_cumsum<=70 then 'A' 
		when rate_cumsum<=90 then 'B'
		else 'C'
	end as grade
from customer_amount_rate_cumsum

