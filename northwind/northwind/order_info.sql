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
select *
	 , sum(sum_amount) over(partition by substr(year_month,1,4) order by year_month) as aggregate_amount
from 
(
	select to_char(order_date,'YYYY-MM') as year_month
	     , sum(amount) as sum_amount
	from order_info
	group by 1
	order by 1
)a;

, previos_comparision as (
	select *
	     , lag(sum_amount) over() as last_sum_amount
	from(
		select concat(year,'-',quarter) as year_quarter
		     , sum(amount) as sum_amount
		from order_info
		group by 1
		order by 1
	)a
)
select * 
     , sum_amount - last_sum_amount as increase
     , case 
     	when (sum_amount - last_sum_amount) > 0 then '+'
     	when (sum_amount - last_sum_amount) < 0 then '-'
     end as increase_yn
     , round(sum_amount/last_sum_amount*100)::varchar(10)||'%' as previos_ratio
from previos_comparision;




-- 매출액, 주문건수, 평균구매액
, order_info_01 as(
select year, month
	, sum(amount) as sum_amount
	, count(distinct order_id) as order_cnt
	, sum(amount)/count(distinct order_id)*100 as avg_amount
from order_info
group by 1,2
)
-- 전월매출액, 매출증감, 누적매출액(연단위), 이동연계
select *
	, lag(sum_amount) over(order by year, month) as last_month_amount -- 전월매출액
	, sum_amount - lag(sum_amount) over(order by year, month) as amount_increase -- 매출증감
	, sum(sum_amount) over(partition by year order by year, month) as amount_cumsum -- 누적매출액(연단위)
	, sum(sum_amount) over(rows between 11 preceding and current row) as amount_12month -- 이동연계
	, lag(sum_amount, 12) over() as last_year_amount -- 작년매출액
	, sum_amount/lag(sum_amount, 12) over() as last_year_rate -- 작대비
from order_info_01
order by 1,2

-- Z차트(199706~199804)
, monthly_amount as(
select year, month, 
	   sum(amount) as sum_amount 
from order_info
group by 1,2
order by 1,2
)
, monthly_amount_base as(
select * 
	, case when concat(year,month) 
	  between '199706' and '199804' 
	  then sum_amount else 0 end as base_amount
from monthly_amount
)
, monthly_amount_cum_11month as(
select * 
	, sum(base_amount) over(order by year, month) as sum_amount_cum
	, sum(sum_amount) over(order by year, month rows between 10 preceding and current row) as sum_11month
from monthly_amount_base
)
select * from monthly_amount_cum_11month
where concat(year,month) between '199706' and '199804';



select min(order_date), max(order_date)
from orders

-- 분기별 작대비
,quater_amount as(
select year, quarter, sum(amount) as sum_amount
from order_info
group by 1,2
order by 1,2
),
quater_amount_lastyear as(
select *, lag(sum_amount, 4) over(order by year, quarter) as last_year_amount
from quater_amount
)
select * ,
	concat(year,'-',quarter) as year_quater,
	round(sum_amount::numeric/last_year_amount::numeric*100,2) as last_year_amount_rate
from quater_amount_lastyear
where last_year_amount is not null;


-- 월별 작대비
montly_amount as(
select year, month, sum(amount) as sum_amount
from order_info
group by 1,2
order by 1,2
),
montly_amount_lastyear as(
select *, lag(sum_amount, 12) over(order by year, month) as last_year_amount
from montly_amount
)
select * ,
	concat(year,'-',month) as year_month,
	round(sum_amount::numeric/last_year_amount::numeric*100,2) as last_year_amount_rate
from montly_amount_lastyear
where last_year_amount is not null;

-- 5일 이동평균
select order_date, sum(amount) as sum_amount,
	avg(sum(amount)) 
	over(order by order_date rows between 4 preceding and CURRENT ROW) as five_day_avg1,
	case when 
		count(*) over(order by order_date rows between 4 preceding and CURRENT ROW) = 5 then 
			avg(sum(amount)) 
			over(order by order_date rows between 4 preceding and CURRENT ROW) end as five_day_avg2 
from order_info
group by 1
order by 1;

-- 월별 누적 매출액
select year_month, sum_amount
	, sum(sum_amount) over(order by year_month) as cumsum
from(
	select to_char(order_date,'YYYY-MM') year_month, 
		   sum(amount) as sum_amount
	from order_info
	group by 1
)a
group by 1,2;


-- 연도별/월별 누적 매출액
select *,
	sum(sum_amount) over(partition by year order by year_month)
from(
	select year, month, to_char(order_date,'YYYY-MM') year_month, 
		   sum(amount) as sum_amount
	from order_info
	group by 1,2,3
)a
order by 3 

-- 월별 누적 매출액
select year_month, sum_amount
	, sum(sum_amount) over(order by year_month) as cumsum
from(
	select to_char(order_date,'YYYY-MM') year_month, 
		   sum(amount) as sum_amount
	from order_info
	group by 1
)a
group by 1,2;

-- 월별 매출액
select to_char(order_date,'YYYY-MM'), 
	   sum(amount) as sum_amount
from order_info
group by 1
order by 1

select * from order_info;
