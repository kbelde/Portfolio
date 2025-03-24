CREATE OR REPLACE TABLE gold_layer.Sales_Fact AS
SELECT 
TO_CHAR(to_date(left((event_time),20)), 'YYYYMMDD') as DATEKEY,
    event_time, 
    event_type,
    user_id, 
    product_id, 
    category_id, 
    brand, 
    user_session AS session_id,
    price, 
    1 AS quantity  -- Assume each row represents 1 purchase
FROM raw_sales_data
;

--truncate table gold_layer.Sales_Fact;

CREATE TABLE gold_layer.User_Dim AS
SELECT 
    user_id, 
    COUNT(*) AS total_purchases, 
    SUM(price) AS total_spent, 
    AVG(price) AS avg_order_value, 
    MIN(event_time) AS first_purchase_date, 
    MAX(event_time) AS last_purchase_date
FROM raw_sales_data
GROUP BY user_id;

CREATE TABLE gold_layer.Product_Dim AS
SELECT 
    product_id, 
    category_id, 
    brand, 
    AVG(price) AS avg_price, 
    COUNT(*) AS total_sold
FROM raw_sales_data
GROUP BY product_id, category_id, brand;


CREATE TABLE gold_layer.Category_Dim AS
SELECT 
    category_id, 
    category_code, 
    SUM(price) AS total_sales, 
    COUNT(*) AS total_orders
FROM raw_sales_data
GROUP BY category_id, category_code;

CREATE OR REPLACE TABLE gold_layer.Session_Dim AS
SELECT 
    DISTINCT user_session AS session_id, 
    user_id, 
    MIN(to_timestamp(left((event_time),20))) AS session_start, 
    MAX(to_timestamp(left((event_time),20))) AS session_end, 
    TIMESTAMPDIFF(MINUTE, MIN(to_timestamp(left((event_time),20))), MAX(to_timestamp(left((event_time),20)))) AS duration
FROM raw_sales_data
GROUP BY user_session, user_id;

select to_timestamp(left((event_time),20))
from raw_sales_data;


with row_num as (
select *, 
row_number() over(partition by EVENT_TIME,EVENT_TYPE,
PRODUCT_ID
,CATEGORY_ID
,CATEGORY_CODE
,BRAND
,PRICE
,USER_ID
,USER_SESSION order by user_session) as rn
from raw_sales_data
)
select * from 
row_num
where rn > 1 ;

CREATE OR REPLACE TABLE WORKING_VERSION_RAW_SALES_DATA AS 
with row_num as (
select *, 
row_number() over(partition by EVENT_TIME
,EVENT_TYPE
,PRODUCT_ID
,CATEGORY_ID
,CATEGORY_CODE
,BRAND
,PRICE
,USER_ID
,USER_SESSION order by user_session) as rn
from raw_sales_data
)
select EVENT_TIME
,EVENT_TYPE
,PRODUCT_ID
,CATEGORY_ID
,CATEGORY_CODE
,BRAND
,PRICE
,USER_ID
,USER_SESSION
from 
row_num
where rn = 1;



CREATE OR REPLACE TABLE GOLD_LAYER.DIM_DATE AS
SELECT distinct
TO_CHAR(to_date(left((event_time),20)), 'YYYYMMDD') as DATEKEY,
to_date(left((event_time),20)) as date,
YEAR(to_date(left((event_time),20))) AS YEAR
,MONTH(to_date(left((event_time),20))) AS MONTH
,MONTHNAME(to_date(left((event_time),20))) AS MONTHNAME
,DAY(to_date(left((event_time),20))) AS DAY
,DAYNAME(to_date(left((event_time),20))) AS DAYNAME
,QUARTER(to_date(left((event_time),20))) AS QUARTER
,WEEK(to_date(left((event_time),20))) AS WEEK
from raw_sales_data
group by event_time;


SELECT TO_CHAR(to_date(left((event_time),20)), 'YYYYMMDD')
from raw_sales_data;

SELECT TO_CHAR(TO_DATE('20200710', 'YYYYMMDD'), 'YYYYMMDD');

CASE 
    WHEN EVENT_TIME = 'year' THEN date_trunc('year', date)
    WHEN EVENT_TIME = 'quarter' THEN date_trunc('quarter', date)
    WHEN EVENT_TIME= 'month' THEN date_trunc('month', date)
    WHEN EVENT_TIME = 'week' THEN date_trunc('week', date)
    ELSE date_trunc('day', date)  -- Default to 'day'
  END AS date
calendar_date AS DATE
,YEAR(calendar_date) AS YEAR
,MONTH(calendar_date) AS MONTH
,MONTHNAME(calendar_date) AS MONTHNAME
,DAY(calendar_date) AS DAY
,DAYNAME(calendar_date) AS DAYNAME
,QUARTER(calendar_date) AS QUARTER
,WEEK(calendar_date) AS WEEK


SELECT to_date(left((event_time),20))
FROM RAW_SALES_DATA;
