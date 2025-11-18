WITH
product_order_info as (
select o.order_id , o.order_date, o.customer_id, p.category_id , c.category_name , od.product_id ,p.product_name ,	p.discontinued, 
	   to_char(o.order_date, 'YYYY') as year,
	   to_char(o.order_date, 'MM') as month,
	   to_char(o.order_date, 'DD') as day,
	   date_part('dow',o.order_date) as dow, 
	   date_part('quarter',o.order_date) as quarter, 
	   od.unit_price , od.quantity , od.discount , 
	   od.unit_price * od.quantity * (1-od.discount) as amount 
from northwind.orders o , northwind.order_details od , northwind.products p , northwind.categories c 
where o.order_id = od.order_id 
and od.product_id = p.product_id 
and p.category_id = c.category_id 
order by 1
)
, ces_montly_order_quantity as(
select product_id
     , date_trunc('month', order_date)::date as date
     , sum(quantity) as sum_quantity
from product_order_info
group by 1,2
)
select * 
from ces_montly_order_quantity a left join ces_montly_order_quantity b
  on a.product_id = b.product_id and a.date = b.date::timestamp+'+1 month';


, ces_period_rank as(
	select concat(year,'-',month) as period, product_name
	     , row_number() over(partition by concat(year,'-',month) order by sum(quantity) desc, sum(amount) desc) as rank
	from product_order_info
	group by 1,2
	order by 1,3 
)
, ces_pre_period as(
select * 
     , to_char((concat(period,'-','01')::timestamp) + '-1 months','YYYY-MM') as pre_period
from ces_period_rank
)
, rank_diff as (
select a.*, b.rank as rank2
     , coalesce((b.rank-a.rank)::varchar(10),'new') as rank_diff
from ces_pre_period a left join ces_pre_period b
on a.pre_period = b.period and a.product_name = b.product_name
)
select rank
     , max(case when period='1997-01' then product_name end) as m9701
     , max(case when period='1997-01' then rank_diff end) as diff1
     , max(case when period='1997-02' then product_name end) as m9702
     , max(case when period='1997-02' then rank_diff end) as diff2
     , max(case when period='1997-03' then product_name end) as m9703
     , max(case when period='1997-03' then rank_diff end) as diff3
     , max(case when period='1997-04' then product_name end) as m9704
     , max(case when period='1997-04' then rank_diff end) as diff4
     , max(case when period='1997-05' then product_name end) as m9705
     , max(case when period='1997-05' then rank_diff end) as diff5
     , max(case when period='1997-06' then product_name end) as m9706
     , max(case when period='1997-06' then rank_diff end) as diff6
     , max(case when period='1997-07' then product_name end) as m9707
     , max(case when period='1997-07' then rank_diff end) as diff7
     , max(case when period='1997-08' then product_name end) as m9708
     , max(case when period='1997-08' then rank_diff end) as diff8
     , max(case when period='1997-09' then product_name end) as m9709
     , max(case when period='1997-09' then rank_diff end) as diff9
     , max(case when period='1997-10' then product_name end) as m9710
     , max(case when period='1997-10' then rank_diff end) as diff10
     , max(case when period='1997-11' then product_name end) as m9711
     , max(case when period='1997-11' then rank_diff end) as diff11
     , max(case when period='1997-12' then product_name end) as m9712
     , max(case when period='1997-12' then rank_diff end) as diff12
from rank_diff
group by rank
having rank<=10
order by rank


, ces_period_rank2 as(
select period, product_name, rank 
from ces_period_rank
)
, ces_pre_period as(
	select period, product_name, rank
	     , case 
	     	when pre_quarter = 0 then concat((year-1)::varchar(4),'-','4') 
	     	else concat(year::varchar(4),'-',pre_quarter::varchar(2))
	     end as pre_period
	from (
		select *
		     , substr(period,1,4)::int as year
		     , substr(period,6,1)::int-1 as pre_quarter    
		from ces_period_rank2 
	)a
)
, ces_pre_period_rank_diff as(
select a.*, b.rank as rank2
     , coalesce((b.rank-a.rank)::varchar(10),'new') as rank_diff
from ces_pre_period a left join ces_pre_period b
on a.pre_period = b.period
and a.product_name = b.product_name
)
select rank
     , max(case when period='1997-1' then product_name end) as q9701
     , max(case when period='1997-1' then rank_diff end) as 순위변화
     , max(case when period='1997-2' then product_name end) as q9702
     , max(case when period='1997-2' then rank_diff end) as 순위변화
     , max(case when period='1997-3' then product_name end) as q9703
     , max(case when period='1997-3' then rank_diff end) as 순위변화
     , max(case when period='1997-4' then product_name end) as q9704
     , max(case when period='1997-4' then rank_diff end) as 순위변화
from ces_pre_period_rank_diff
group by rank
having rank<=10
order by 1



, ces_period_rank_diff as(
	select a.period, a.product_name, a.rank
	     , coalesce((b.rank-a.rank)::varchar(2),'new') as rank_diff
	from ces_period_rank a left join ces_period_rank b 
	  on concat(a.period,'-01')::timestamp = concat(b.period,'-01')::timestamp+'+1 months'
	  and a.product_name = b.product_name
	where a.period between '1997-01' and '1997-12'
	  and a.rank <= 10
	order by 1,3
)
select rank 
     , max(case when period='1997-01' then product_name end) as m9701
     , max(case when period='1997-01' then rank_diff end) as r1
     , max(case when period='1997-02' then product_name end) as m9702
     , max(case when period='1997-02' then rank_diff end) as r2
     , max(case when period='1997-03' then product_name end) as m9703
     , max(case when period='1997-03' then rank_diff end) as r3
     , max(case when period='1997-04' then product_name end) as m9704
     , max(case when period='1997-04' then rank_diff end) as r4
     , max(case when period='1997-05' then product_name end) as m9705
     , max(case when period='1997-05' then rank_diff end) as r5
     , max(case when period='1997-06' then product_name end) as m9706
     , max(case when period='1997-06' then rank_diff end) as r6
     , max(case when period='1997-07' then product_name end) as m9707
     , max(case when period='1997-07' then rank_diff end) as r7
     , max(case when period='1997-08' then product_name end) as m9708
     , max(case when period='1997-08' then rank_diff end) as r8
     , max(case when period='1997-09' then product_name end) as m9709
     , max(case when period='1997-09' then rank_diff end) as r9
     , max(case when period='1997-10' then product_name end) as m9700
     , max(case when period='1997-10' then rank_diff end) as r10
     , max(case when period='1997-11' then product_name end) as m9711
     , max(case when period='1997-11' then rank_diff end) as r11
     , max(case when period='1997-12' then product_name end) as m9712
     , max(case when period='1997-12' then rank_diff end) as r12
from ces_period_rank_diff
group by rank
having rank <= 10
order by 1
  
 


select rank 
     , max(case when period='1997-01' then product_name end) as m9701
     , max(case when period='1997-02' then product_name end) as m9702
     , max(case when period='1997-03' then product_name end) as m9703
     , max(case when period='1997-04' then product_name end) as m9704
     , max(case when period='1997-05' then product_name end) as m9705
     , max(case when period='1997-06' then product_name end) as m9706
     , max(case when period='1997-07' then product_name end) as m9707
     , max(case when period='1997-08' then product_name end) as m9708
     , max(case when period='1997-09' then product_name end) as m9709
     , max(case when period='1997-10' then product_name end) as m9700
     , max(case when period='1997-11' then product_name end) as m9711
     , max(case when period='1997-12' then product_name end) as m9712
from ces_period_rank
group by rank
having rank <= 10
order by 1




, ces_sum_quantity_rank as(
select b.country, a.product_name
     , sum(a.quantity) as sum_quantity
     , sum(a.amount) as sum_amount
     -- 동일 순위인 경우 매출액 높은순
     , row_number() over(partition by country order by sum(a.quantity) desc, sum(a.amount) desc) as sum_quantity_rank
from product_order_info a, northwind.customers b
where a.customer_id = b.customer_id
group by 1,2
)
, ces_country_sum_amount_rank as(
	select *
	     , dense_rank() over(order by country_sum_amount desc) country_sum_amount_rank
	from(
		select * 
		     , sum(sum_amount) over(partition by country) as country_sum_amount
		from ces_sum_quantity_rank
	)a
)
select quantity_rank1, count(*) from(
select country_sum_amount_rank as no, country 
	 , max(case when sum_quantity_rank=1 then product_name end) quantity_rank1
	 , max(case when sum_quantity_rank=2 then product_name end) quantity_rank2
	 , max(case when sum_quantity_rank=3 then product_name end) quantity_rank3
	 , max(case when sum_quantity_rank=4 then product_name end) quantity_rank4
	 , max(case when sum_quantity_rank=5 then product_name end) quantity_rank5
from ces_country_sum_amount_rank
group by 1,2
order by 1
) x
group by 1
order by 2 desc


, ces_product_category as(
	select c.category_name , p.product_name as pname
	from northwind.products p, northwind.categories c 
	where p.category_id = c.category_id 
)
select country_sum_amount_rank as no, country 
	 , max(case when sum_quantity_rank=1 then 
	 	(select category_name from ces_product_category where product_name=pname) end) quantity_rank1
	 , max(case when sum_quantity_rank=2 then 
	 	(select category_name from ces_product_category where product_name=pname) end) quantity_rank2
	 , max(case when sum_quantity_rank=3 then 
	 	(select category_name from ces_product_category where product_name=pname) end) quantity_rank3
	 , max(case when sum_quantity_rank=4 then 
	 	(select category_name from ces_product_category where product_name=pname) end) quantity_rank4
	 , max(case when sum_quantity_rank=5 then 
	 	(select category_name from ces_product_category where product_name=pname) end) quantity_rank5
from ces_country_sum_amount_rank
group by 1,2
order by 1


select country_sum_amount_rank as no, country 
	 , max(case when sum_quantity_rank=1 then product_name end) quantity_rank1
	 , max(case when sum_quantity_rank=2 then product_name end) quantity_rank2
	 , max(case when sum_quantity_rank=3 then product_name end) quantity_rank3
	 , max(case when sum_quantity_rank=4 then product_name end) quantity_rank4
	 , max(case when sum_quantity_rank=5 then product_name end) quantity_rank5
from ces_country_sum_amount_rank
group by 1,2
order by 1


select country_sum_amount_rank as no, country 
	 , max(case when sum_quantity_rank=1 then product_name end) quantity_rank1
	 , max(case when sum_quantity_rank=2 then product_name end) quantity_rank2
	 , max(case when sum_quantity_rank=3 then product_name end) quantity_rank3
	 , max(case when sum_quantity_rank=4 then product_name end) quantity_rank4
	 , max(case when sum_quantity_rank=5 then product_name end) quantity_rank5
from ces_country_sum_amount_rank
group by 1,2
order by 1







-- 국가-제품별 판매수량, 매출액
, country_product_quantity_amount as(
select c.country, a.product_name 
	 , sum(quantity) as sum_quantity
	 , sum(amount) as sum_amount
from product_order_info a, northwind.orders b, customers c
where a.order_id = b.order_id 
  and b.customer_id = c.customer_id
group by 1,2
order by 1
)
-- 국가별 매출액
, country_amount as(
select *
	 , sum(sum_amount) over (partition by country) as country_amount 
	 from country_product_quantity_amount
)
, country_quantity_rank as(
	select * 
	from (
		select * 
		     , dense_rank() over(order by country_amount desc) as country_rank
		     , dense_rank() over(partition by country order by sum_quantity desc) as quantity_rank
		from country_amount
		order by country_rank, quantity_rank
	) x
	where quantity_rank <= 5
)
select country, country_rank
	, max(case when quantity_rank=1 then product_name end) as rank1
	, max(case when quantity_rank=2 then product_name end) as rank2
	, max(case when quantity_rank=3 then product_name end) as rank3
	, max(case when quantity_rank=4 then product_name end) as rank4
	, max(case when quantity_rank=5 then product_name end) as rank5
from country_quantity_rank
group by 1,2
order by 2



, bestseller as(
select a.product_id, a.product_name, a.category_name
	 , b.unit_price as master_unit_price
     , avg(a.unit_price) as sale_unit_price
     , sum(a.quantity) as sum_quantity
     , sum(a.amount) as sum_amount   
     , rank() over(order by sum(a.quantity) desc) as rank
from product_order_info a, northwind.products b
where a.product_id = b.product_id
group by 1,2,3,4
order by 6 desc
)



, bestseller_category as(
	select *
	from (
		select category_name, product_name, sum_quantity 
			 , rank() over(partition by category_name order by sum_quantity desc) as rank
		from bestseller 
		order by 1, 4
	) x
	where rank in (1,2,3)
)
select rank
	 , max(case when category_name='Beverages' then product_name end) as Beverages
	 , max(case when category_name='Condiments' then product_name end) as Condiments
	 , max(case when category_name='Confections' then product_name end) as Confections
	 , max(case when category_name='Dairy Products' then product_name end) as DairyProducts
	 , max(case when category_name='Grains/Cereals' then product_name end) as GrainsCereals
	 , max(case when category_name='Meat/Poultry' then product_name end) as MeatPoultry
	 , max(case when category_name='Produce' then product_name end) as Produce
	 , max(case when category_name='Seafood' then product_name end) as Seafood
from bestseller_category
group by rank

select * from northwind.categories

select rank, product_id, product_name, sum_quantity, sale_unit_price, sum_amount, master_unit_price, category_name
from bestseller
where rank<=10



-- 월별, 카테고리별 매출액
, month_category_amount as(
select year, month, concat(year,'-',month) as year_month
	 , category_name, sum(amount) as sum_amount
from product_order_info
group by 1,2,3,4
order by 1,2,3
)
-- 월별, 카테고리별 매출 순위
, month_category_amount_rank as(
select * 
	, rank() over(partition by year_month order by sum_amount desc)
from month_category_amount
order by 1,2,4
)
, month_category_amount_rank_pivot as(
select year_month
	, max(case when category_name='Beverages' then rank end) as Beverages
	, max(case when category_name='Condiments' then rank end) as Condiments
	, max(case when category_name='Confections' then rank end) as Confections
	, max(case when category_name='Dairy Products' then rank end) as DairyProducts
	, max(case when category_name='Grains/Cereals' then rank end) as GrainsCereals
	, max(case when category_name='Meat/Poultry' then rank end) as MeatPoultry
	, max(case when category_name='Produce' then rank end) as Produce
	, max(case when category_name='Seafood' then rank end) as Seafood
from month_category_amount_rank
group by 1
order by 1
)
, month_category_amount_rank_pivot_avg as(
select 'average' as year_month
	, (select avg(Beverages) from month_category_amount_rank_pivot) as Beverages
	, (select avg(Condiments) from month_category_amount_rank_pivot) as Condiments
	, (select avg(Confections) from month_category_amount_rank_pivot) as Confections
	, (select avg(DairyProducts) from month_category_amount_rank_pivot) as DairyProducts
	, (select avg(GrainsCereals) from month_category_amount_rank_pivot) as GrainsCereals
	, (select avg(MeatPoultry) from month_category_amount_rank_pivot) as MeatPoultry
	, (select avg(Produce) from month_category_amount_rank_pivot) as Produce
	, (select avg(Seafood) from month_category_amount_rank_pivot) as Seafood
)
select * from month_category_amount_rank_pivot


select * from categories c 

select rank 
	, max(case when year_month='1997-01' then category_name end) as "1997-01"
	, max(case when year_month='1997-02' then category_name end) as "1997-02"
	, max(case when year_month='1997-03' then category_name end) as "1997-03"
	, max(case when year_month='1997-04' then category_name end) as "1997-04"
	, max(case when year_month='1997-05' then category_name end) as "1997-05"
	, max(case when year_month='1997-06' then category_name end) as "1997-06"
	, max(case when year_month='1997-07' then category_name end) as "1997-07"
	, max(case when year_month='1997-08' then category_name end) as "1997-08"
	, max(case when year_month='1997-09' then category_name end) as "1997-09"
	, max(case when year_month='1997-10' then category_name end) as "1997-10"
	, max(case when year_month='1997-11' then category_name end) as "1997-11"
	, max(case when year_month='1997-12' then category_name end) as "1997-12"
from month_category_amount_rank
group by 1
order by 1

select year_month
	, max(case when rank=1 then category_name end) as rank1
	, max(case when rank=2 then category_name end) as rank2
	, max(case when rank=3 then category_name end) as rank3
	, max(case when rank=4 then category_name end) as rank4
	, max(case when rank=5 then category_name end) as rank5
	, max(case when rank=6 then category_name end) as rank6
	, max(case when rank=7 then category_name end) as rank7
	, max(case when rank=8 then category_name end) as rank8
from month_category_amount_rank
group by 1
order by 1

-- 월별, 카테고리별 매출액
select year, month, concat(year,'-',month) as year_month
	 , category_name, sum(amount) as sum_amount
from product_order_info
group by 1,2,3,4
order by 1,2,3

-- 카테고리별 제품 매출액
, category_product_amount as(
select category_name, product_name, 
	sum(amount) as sum_amount
from product_order_info
group by 1,2
order by 1,3 desc
)
-- 카테고리별 제품 매출 순위
, category_product_amount_rank as(
select * 
	, rank() over(partition by category_name order by sum_amount desc) as rank
from category_product_amount
)
select rank
	, max(case when category_name='Beverages' then product_name end) as Beverages
	, max(case when category_name='Condiments' then product_name end) as Condiments
	, max(case when category_name='Confections' then product_name end) as Confections
	, max(case when category_name='Dairy Products' then product_name end) as DairyProducts
	, max(case when category_name='Grains/Cereals' then product_name end) as GrainsCereals
	, max(case when category_name='Meat/Poultry' then product_name end) as MeatPoultry
	, max(case when category_name='Produce' then product_name end) as Produce
	, max(case when category_name='Seafood' then product_name end) as Seafood
from category_product_amount_rank
group by rank
order by rank


-- 카테고리별 제품 매출 순위 1,2,3등 피벗
select category_name
	, max(case when rank=1 then product_name end) as rank1
	, max(case when rank=2 then product_name end) as rank2
	, max(case when rank=3 then product_name end) as rank3	
from category_product_amount_rank
group by 1;


select rank
	, max(case when category_name='Beverages' then product_name end) as Beverages
	, max(case when category_name='Condiments' then product_name end) as Condiments
	, max(case when category_name='Confections' then product_name end) as Confections
	, max(case when category_name='Dairy Products' then product_name end) as DairyProducts
	, max(case when category_name='Grains/Cereals' then product_name end) as GrainsCereals
	, max(case when category_name='Meat/Poultry' then product_name end) as MeatPoultry
	, max(case when category_name='Produce' then product_name end) as Produce
	, max(case when category_name='Seafood' then product_name end) as Seafood
from category_product_amount_rank
group by rank
order by rank


select * from categories

-- 카테고리별 제품 매출 순위 1,2,3등 피벗
select category_name
	, max(case when rank=1 then product_name end) as rank1
	, max(case when rank=2 then product_name end) as rank2
	, max(case when rank=3 then product_name end) as rank3	
from category_product_amount_rank
group by 1;

-- 카테고리별 지표
select category_id , category_name
	   , round(sum(amount)) as sum_amount 								-- 매출액 
	   , count(distinct order_id) as order_cnt							-- 주문횟수
	   , sum(quantity) as sum_quantity
	   , round(avg(unit_price)) as avg_unit_price
from product_order_info
group by 1,2
order by 3 desc


-- 제품별 매출액 지표
select product_id , product_name
	   , round(sum(amount)) as sum_amount 								-- 매출액 
	   , count(distinct order_id) as order_cnt							-- 주문횟수
	   , sum(quantity) as sum_quantity
	   , round(avg(unit_price)) as avg_unit_price
from product_order_info
group by 1,2
order by 3 desc




select product_id , product_name , discontinued, unit_price
	, sum(amount) as sum_amount
	, count(distinct order_id) as order_cnt
	, sum(quantity) as sum_quantity
from product_order_info
group by 1,2,3,4


-- 카테고리별 ABC 분석
-- 카테고리별 매출액
, category_sum_amount as(
select category_id, category_name, sum(amount) as sum_amount
from product_order_info  
group by 1,2
order by 3 desc
)
-- 구성비
, category_amount_rate as(
select * 
	, sum(sum_amount) over() as total_amount
	, sum_amount/sum(sum_amount) over()*100 as amount_rate
from category_sum_amount
)
-- 구성비 누계
, category_amount_rate_cum as(
select * 
	, sum(amount_rate) over(order by sum_amount desc) as amount_rate_cum
from category_amount_rate
)
-- 등급
select * 
	, case 
		when amount_rate_cum <=70 then 'A'
		when amount_rate_cum <=90 then 'B'
	else 'C'
	end	as product_grade
from category_amount_rate_cum;


-- 제품별 ABC 분석
-- 제품별 매출액
, product_sum_amount as(
select product_id, product_name, sum(amount) as sum_amount
from product_order_info  
group by 1,2
order by 3 desc
)
-- 구성비
, prduct_amount_rate as(
select * 
	, sum(sum_amount) over() as total_amount
	, sum_amount/sum(sum_amount) over()*100 as amount_rate
from product_sum_amount
)
-- 구성비 누계
, product_amount_rate_cum as(
select * 
	, sum(amount_rate) over(order by sum_amount desc) as amount_rate_cum
from prduct_amount_rate
)
-- 등급
select * 
	, case 
		when amount_rate_cum <=70 then 'A'
		when amount_rate_cum <=90 then 'B'
		when amount_rate_cum <=100 then 'C'		
	end	as product_grade
from product_amount_rate_cum;



-- 월별_카테고리별 매출액
select year, month, category_id ,category_name , 
	sum(amount) as sum_amount 
from product_order_info
group by 1,2,3,4
order by 1,2,3;




-- 카테고리-제품 매출 순위 1,2,3등 피벗
select category_id, category_name,
	max(case when rank=1 then product_id::varchar(2)||'_'||product_name end) as rank1,
	max(case when rank=2 then product_id::varchar(2)||'_'||product_name end) as rank2,
	max(case when rank=3 then product_id::varchar(2)||'_'||product_name end) as rank3
from(
	select *, rank() over(partition by category_id order by sum_amount desc)
	from(
		select category_id, category_name, product_id, product_name, discontinued, sum(amount) as sum_amount
		from product_order_info
		group by 1,2,3,4,5
	)a
)b
group by 1,2
order by 1;


-- 카테고리-제품 매출 순위
select *, rank() over(partition by category_id order by sum_amount desc)
from(
	select category_id, category_name, product_id, product_name, discontinued, sum(amount) as sum_amount
	from product_order_info
	group by 1,2,3,4,5
)a;


-- 카테고리-제품 매출액
select category_id, category_name, product_id, product_name, sum(amount) as sum_amount
from product_order_info
group by 1,2,3,4;

-- 카테고리-월별 매출액 피벗(1997년)
select category_id, category_name,
		max(case when month='01' then sum_amount end) as m01,
		max(case when month='02' then sum_amount end) as m02,
		max(case when month='03' then sum_amount end) as m03,
		max(case when month='04' then sum_amount end) as m04,
		max(case when month='05' then sum_amount end) as m05,
		max(case when month='06' then sum_amount end) as m06,
		max(case when month='07' then sum_amount end) as m07,
		max(case when month='08' then sum_amount end) as m08,
		max(case when month='09' then sum_amount end) as m09,
		max(case when month='10' then sum_amount end) as m10,
		max(case when month='11' then sum_amount end) as m11,
		max(case when month='12' then sum_amount end) as m12
from(
	select *,
		rank() over(partition by year, month order by sum_amount desc)
	from(
	select year, month, category_id , category_name , 
	sum(amount) as sum_amount 
	from product_order_info
	group by 1,2,3,4
	order by 1,2,3
	)a
	order by 1,2,3
)b
where year='1997'
group by 1,2
order by category_id;

