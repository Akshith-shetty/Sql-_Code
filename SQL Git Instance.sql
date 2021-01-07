CREATE TABLE [dbo].[dim_payment$](
	[payment_id] [float] NULL,
	[payment_method] [nvarchar](255) NULL
) 

CREATE TABLE [dbo].[dim_platform$](
	[platform_id] [float] NULL,
	[platform_name] [nvarchar](255) NULL
) 

CREATE TABLE [dbo].[dim_restaurant$](
	[restaurant_id] [float] NULL,
	[restaurant_name] [nvarchar](255) NULL,
	[chain_id] [float] NULL,
	[city] [nvarchar](255) NULL
)

CREATE TABLE [dbo].[fct_orders$](
	[order_id] [float] NULL,
	[order_date] [datetime] NULL,
	[customer_id] [float] NULL,
	[restaurant_id] [float] NULL,
	[payment_id] [float] NULL,
	[platform_id] [float] NULL,
	[is_canceled] [bit] NOT NULL,
	[paid_amount] [float] NULL
)

CREATE TABLE [dbo].[fct_rating$](
	[order_id] [float] NULL,
	[created_at] [datetime] NULL,
	[rating] [nvarchar](255) NULL
);



--Cte will count grouped by customer_id and payment_method 
--Filtered when cnt is less than or equal to 2 and order_date ranges from 2018 till today
--Null values and cancelled orders will be filtered

with count_cust_paymnt as (Select customer_id, payment_method, count(*) as cnt
from fct_orders$ as o
join dim_payment$ as p
on o.payment_id = p.payment_id
where customer_id is not null and payment_method is not null
and order_date between '2018-01-01' and GETDATE() 
and is_canceled = 'FALSE'
group by customer_id, payment_method
having count(*) <= 2)
 
--case statement will count same payment method 
-- sum(cnt) will count the overall payment_method
Select payment_method, SUM(case when cnt = 2 then 1 else 0 end)*100.0/ SUM(cnt) as pct_2order
from count_cust_paymnt
group by payment_method;


--Dense rank will rank the city on total orders
--City with 5th largest order will be filtered
--sum by paid amount and partition will find the total order value per customer_id
--ordered by total_paid_eur desc and top 1 to select the highest customer_id spend value
 
Select Top 1 city, customer_id,SUM(paid_amount)over(partition by customer_id)  as total_paid_eur
from fct_orders$ as o
join dim_restaurant$ as r
on o.restaurant_id = r.restaurant_id
where city = (
Select city from 
(Select city, DENSE_RANK() OVER( order by count(city) desc) as ran1
from fct_orders$ as o
join dim_restaurant$ as r
on o.restaurant_id = r.restaurant_id
where is_canceled = 'FALSE' and YEAR(order_date) = 2018
group by city) as assd
where ran1 =5)
order by 3 desc



--Left join to consider all order from fct_orders$
--lead to get next order date of customer
--Datediff to find the date between rating date and next order date

with nxt_order as (Select customer_id, rating, DATEDIFF(DD,created_at, lead(order_date)over(partition by customer_id order by order_date)) as days_nxt_order
--,lead(order_date)over(partition by customer_id order by order_date) as nxt_ordr_date, o.order_id,order_date, created_at 
from fct_orders$ as o
Left join fct_rating$ as r
on o.order_id = r.order_id
where is_canceled = 'FALSE' and year(order_date) = 2019
)
--count all order where the rating and order day is less than 45 and rating is positive by total orders
 
Select (COUNT(*)*100.0/(Select COUNT(*) from nxt_order)) as positive_feedback_45d_return
from nxt_order
where days_nxt_order <=45 and rating = 'POSITIVE'

--datename to get name of order month
--Grouping on month and restaurant_id and distinct customer_id will fetch unique customer_id
 
Select datename(MM, order_date) as 'month', o.restaurant_id, count(Distinct customer_id) as customers
from fct_orders$ as o
join dim_restaurant$ as r
on o.restaurant_id = r.restaurant_id
where city = 'innsbruck' and 
order_date between '2017-01-01' and GETDATE() and is_canceled = 'FALSE'
group by datename(MM, order_date),  o.restaurant_id

Select count(customer_id) as customers
from fct_orders$ as o
join dim_platform$ as p
on o.platform_id = p.platform_id
where platform_name = 'android'
and YEAR(order_date) = 2017
and is_canceled = 'FALSE'


