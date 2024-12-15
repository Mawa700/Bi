/* download this script and the dataset, import the dataset in MySQL database and import the script to your 
editor and run the scripts to view the analysis. in my case am using the my_portfolio database where my data is.*/

use my_portfolio;

select * from train;
describe train;

-- 1. CUSTOMER BEHAVIOUR ANALYSIS

-- types of customers
select distinct segment
from train;

/*we have 3 types of customers, 
1. consumer
2. corporate 
3. Home office*/

-- number of customers in each segment 
select segment , count(*) as total
from train
group by segment;

-- sales per segment
select segment, sum(sales) as total_sales
from train 
group by segment 
order by total_sales desc;

-- ranking customers by number of orders
select `customer id`, `customer name`, `segment`, count(`order id`) as total_orders
from train
group by `customer id`, `customer name`, `segment`
order by total_orders desc;

-- ranking customers by sales 
select `customer id`, `customer name`, `segment`, sum(`sales`) as total_sales
from train
group by `customer id`, `customer name`, `segment`
order by total_sales desc;

-- Who are our best customers 
-- RFM analysis for customer segmentation
/*Recency-Frequency-Monetary (RFM), its an indexing technique that uses past purchasing 
behaviour to segment customers. It uses three key metrics(recency, frequency and monetary value)*/

create temporary table rfm1
with rfm as
(
select tb.*,
	max_order_date - last_order_date as recency
from 
(
	select `customer name`, 
		sum(sales) as revenue,
		count(`customer name`) as frequency,
		max(`order date`) as last_order_date,
		(select max(`order date`) from train) as max_order_date
	from train
	group by `customer name`)tb
),
a as
(
select *,
	ntile(4) over (order by recency desc) as rfm_recency,
    ntile(4) over (order by frequency) as rfm_frequency,
    ntile(4) over (order by revenue) as rfm_monetary
from rfm
),
b as 
(
select *,
	rfm_frequency + rfm_recency + rfm_monetary as rfm_cell,
    concat(cast(rfm_recency as char), cast(rfm_frequency as char), cast(rfm_monetary as char)) as ref
from a
)
select * from b;

-- customer segment 
select `customer name`, rfm_recency, rfm_frequency, rfm_monetary, 
case
	when ref in (111, 112, 121, 122, 123, 132, 113, 211, 212, 214, 114, 141) then 'lost_customers'
    when ref in (133, 134, 143, 244, 334, 343, 344, 243, 324, 323) then 'slipping_away'
    when ref in (311, 411, 331) then 'new_customer'
    when ref in (222, 223, 233, 322, 321, 234) then 'potential_churner'
    when ref in (433, 434, 443, 444, 431, 424) then 'active'
end as rfm_segment
from rfm1;


-- 2. PRODUCT ANALYSIS

-- our products
select distinct category 
from train;

select distinct `sub-category`
from train;

-- total number of products in each category
select category, count(distinct `sub-category`)
from train
group by category;

-- top performing products 
select category, `sub-category`, sum(sales) as total_sales
from train
group by category, `sub-category`
order by total_sales desc;

-- Which products are mostly bought together

select distinct `order id`,
(
	select group_concat( `product id` separator ',')
	from train  as p
	where `order id` in 
	(
		select order_id
		from 
		(
			select `order id` as order_id , count(*) total_orders
			from train
			group by order_id
		)m 
		where total_orders = '3') 
        and p.`order id` = s.`order id`
) as products
from train as s
order by products desc;

-- 3. TIME SERIES ANALYSIS (SALES TREND)

create temporary table sales_trend
select d.*,
	extract(year from order_date) as order_year
from 
(
	select str_to_date(`order date`, '%d/%m/%Y') as order_date, sales
	from train
)d;

-- overall trend
select order_year,
	sum(sales)
from sales_trend
group by order_year 
order by 1;

-- 2016 quaterly sales 
select quarter(order_date) as quaters, 
	sum(sales)
 from sales_trend
 where order_year = '2016'
 group by quaters
 order by 1;
 
 -- 2016 monthly sales
 select extract(month from order_date) as months_2016,
	sum(sales)
 from sales_trend
 where order_year = '2016'
 group by months_2016
 order by 1;
 
 -- 4. GEOGRAPHICAL ANALYSIS
 #orders by state
 select state, count(`order id`) as total_orders
 from train
 group by state
 order by 2 desc;
 
 #total sales per state
select state, sum(sales) as total_sales
 from train
 group by state
 order by 2 desc;
 
 # total sales and orders in states and cities
select state, city, sum(sales) as total_sales
 from train
 group by state, city
 order by 3 desc;
 
 -- 5. BONUS
 -- COHORT retention analysis
 /* 
 cohort analysis is an analysis of several different cohorts to get a better understanding of behaviours,
 patterns and trends. we are going to look at the customers behaviour after their first purchase. 
 */
create temporary table df
select `customer id` as customerID, 
		`product id` as productID,
        `order id` as orderID,
        `sales` as sales,
        `state` as state,
        `city` as city,        
		str_to_date(`order date`, '%d/%m/%Y') as order_date
from train;


-- getting initial startdate (first order date) 
create temporary table cohort
select customerid,
	min(order_date) as first_order,
    date_format(min(order_date), '%Y-%m-01') as cohort_date
from df 
group by customerid;

-- creating index ie number of months that passed since the customers first order
create temporary table cohort_retention
select 
	mm.*,
    year_diff*12 + month_diff + 1 as cohort_index
from 
(
	select 
		m.*,
		order_year - cohort_year as year_diff,
		order_month - cohort_month as month_diff
	from
		(
		select d.*, 
				c.cohort_date,
				extract(year from d.order_date) as order_year,
				extract(month from d.order_date) as order_month,
				extract(year from c.cohort_date) as cohort_year,
				extract(month from c.cohort_date) as cohort_month
		from df d
		left join cohort c
		on d.customerid = c.customerid
		)m
	)mm;
/* 
1 in cohort_index column means that the customer made thier 2nd purchase in the same month, 2 means 
the customer made the 2nd purchase in the 2nd month 
*/
with pivot_data as
(
select distinct 
	customerid,
    cohort_date,
    cohort_index
from cohort_retention
)
select cohort_date,
	count(case when cohort_index = '1' then customerid else 0 end) as '1',
    count(case when cohort_index = '6' then customerid else 0 end) as '6',
    count(case when cohort_index = '25' then customerid else 0 end) as '25',
    count(case when cohort_index = '32' then customerid else 0 end) as '32'
from pivot_data
group by cohort_date
order by 1
;