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

-----------------------------------------------------------------------------------------------------------------------------------------------

--Ratio of the customers placing another order within 45 days after an order they left positive feedback for by the total orders in the table.

/*
CTE
• Assuming no duplicates and null values
• cte query “nxt_order”: To calculate the days between created_at and next order date per customer_id
• Lead function to get next order date of customer_id
• Datediff to find the days between created_at date and next order date
• Left join to consider all orders from fct_orders$
• Filtered to non-cancelled orders and year 2019
Main Query:
• To calculate percentage of all orders where the days between created_at and next order_date is less than 45 and rating is positive to the total number of orders
*/

with nxt_order as (Select customer_id, rating, DATEDIFF(DD,created_at, lead(order_date)over(partition by customer_id order by order_date)) as days_nxt_order
from fct_orders$ as o
Left join fct_rating$ as r
on o.order_id = r.order_id
where is_canceled = 'FALSE' and year(order_date) = 2019
)
Select (COUNT(*)*100.0/(Select COUNT(*) from nxt_order)) as positive_feedback_45d_return
from nxt_order
where days_nxt_order <=45 and rating = 'POSITIVE'

-----------------------------------------------------------------------------------------------------------------------------------------------------
--The count of customers who used only android for all their orders in the year 2017


/*
Cte:
• Assuming no duplicates and null values
• case statement to check if customer_id used android and return 1 if true in column “only_android”
• cte query “android_count”: Will have column only_android and total_count per customer_id and is filtered to year 2017 and not cancelled orders
Main Query:
• Inner query will sum the only_android column grouped by customer_id and total_order
• Having clause filters the customers_id where the total_order equal to total_android to check if the total orders per customer is equal to the total number of android orders
• Outer query counts the total customers
*/


with android_count as (Select customer_id, platform_name, (case when platform_name = 'android' then 1 else 0 end) as only_android, COUNT(*)over(PARTITION by customer_id) as total_order
from fct_orders$ as o
join dim_platform$ as p
on o.platform_id = p.platform_id
where YEAR(order_date) = 2017 and is_canceled = 'FALSE')

Select COUNT(*) as customers from(
Select customer_id, SUM(only_android) as total_android, total_order from android_count
group by customer_id, total_order
having SUM(only_android) = total_order
) as inner_query
------------------------------------------------------------------------------------------------------------------------------------------------

--To find the customer that spent the most money only in the 5th largest city (largest as in total number of orders in whole 2018)

/*
• Assuming no duplicates and null values
• Dense rank will rank the city on total orders
• City with 5th largest order will be filtered
• Sum by paid amount and partition will find the total order value per customer_id
• Ordered by total_paid_eur desc and top 1 to select the highest customer_id by spend value
*/

Select Top 1 customer_id, city, SUM(paid_amount)over(partition by customer_id) as total_paid_eur
from fct_orders$ as o
join dim_restaurant$ as r
on o.restaurant_id = r.restaurant_id
where city = (
Select city from
(Select city, DENSE_RANK() OVER( order by count(city) desc) as rank
from fct_orders$ as o
join dim_restaurant$ as r
on o.restaurant_id = r.restaurant_id
where is_canceled = 'FALSE' and YEAR(order_date) = 2018
group by city) as city_rank
where rank =5)
order by 3 desc


---------------------------------------------------------------------------------------------------------------------------------------------


--Customer who has ordered since 2018 and have 2 orders and more, what is the percentage of customer 
--who used the same consecutive payment method as the previous payment method divided by the total previous payment method
/*
CTE:
• Assuming no duplicates and null values
• Filter is applied to not cancelled orders and fetched date since 2018
• Query “nxt_pymnt_method”: To calculate the next payment method I used lead function on payment_method partition by customer_id
• Query “customer_payment_count”: I am fetching the customer_id with consecutive same payment method and who has more than 2 orders.
To extract same consecutive payment, I am applying the where clause
payment_method = next_payment
• Rank function is performed to avoid duplicates while calculating the percentage of customer for a particular payment method.
• Sum the rank to get the total number of customers for a particular payment method
Main Query:
• To calculate the percentage of customer who used consecutive same payment method ie the percentage of customers who used the same payment method for their second order, split by the first payment method, I applied the below formula:
sum of customers having same consecutive payment method *100.0 / sum of first payment method
*/

with cte as (
Select payment_method, SUM(rank_per_payment) as total_pymnt_mthod from (
Select customer_id, payment_method, COUNT(*) as payment_count, rank()over(PARTITION by customer_id order by customer_id) as rank_per_payment from (
Select customer_id, payment_method, row_number()over(partition by customer_id order by order_date ) as row,
LEAD(payment_method)over(partition by customer_id order by customer_id) as next_payment
from fct_orders$ as o
join dim_payment$ as p
on o.payment_id = p.payment_id
where order_date between '2018-01-01' and GETDATE() and is_canceled = 'FALSE'
group by customer_id, payment_method, order_date) as nxt_pymnt_method
where row >= 2 and payment_method = next_payment
group by payment_method, customer_id) as customer_payment_count
group by payment_method
)
Select cte.payment_method, total_pymnt_mthod*100.0/overall_payment_count as pct_2order
from cte
join
(Select payment_method, COUNT(payment_method) as overall_payment_count
from fct_orders$ as o
Inner join dim_payment$ as p
on o.payment_id = p.payment_id
group by payment_method) as dd
on cte.payment_method = dd.payment_method



--------------------------------------------------------------------------------------------------------------------------------------------------
--From 2017 till today, count of distinct customers for every month in each restaurant for the city “krakow”

/*
• Datename to get name of order month
• Filtered to city 'innsbruck' and order date from 2017 to today
• Grouping on month and restaurant_id and distinct customer_id will fetch unique customer_id
*/

Select datename(MM, order_date) as 'month', o.restaurant_id, count(Distinct customer_id) as customers
from fct_orders$ as o
join dim_restaurant$ as r
on o.restaurant_id = r.restaurant_id
where city = 'krakow' and order_date between '2017-01-01' and GETDATE() and is_canceled = 'FALSE'
group by datename(MM, order_date), o.restaurant_id;



