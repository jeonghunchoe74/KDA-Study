-- 기본 리텐션
select *
	 , first_value(cohort_retained) over(order by period) as cohort_size
	 , cohort_retained::numeric/first_value(cohort_retained) over(order by period) as pct_retained
from 
(
	select date_part('year',age(b.term_start, a.first_term)) as period
	     , count(distinct b.id_bioguide) as cohort_retained
	from 
	(
		select id_bioguide 
		     , min(term_start) as first_term
		from analysis.legislators_terms lt 
		group by 1
	) a join analysis.legislators_terms b on a.id_bioguide = b.id_bioguide 
	group by 1
	order by 1
) aa;

with date_dim as(
select generate_series::date as date
from generate_series('1770-12-31','2020-12-31', interval '1 year')
)
select * 
from 
(
	select date_part('year', age(c.date, a.first_term))
		 , count(*)
	from(
		select id_bioguide, min(term_start) as first_term
		from analysis.legislators_terms lt
		group by 1
	)  a join analysis.legislators_terms b on a.id_bioguide = b.id_bioguide
	  	left join date_dim c on  c.date between b.term_start and b.term_end
	group by date_part('year', age(c.date, a.first_term))
	order by 1
) aa;


