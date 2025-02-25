---------------- Q1 ------------------
CREATE OR REPLACE VIEW CODE_CHALLENGE_THI44K287NGA.SOURCE.position_usd AS
WITH 
-- This CTE is meant to provide the needed values of the price and position table as well as auxiliary values that will help fill missing values
position_price AS
(
SELECT 
price.company_id,
price.close_usd, 
price.volume,
price.date,
COALESCE(position.shares, 0) as shares,
-- Get previous non-null shares
COALESCE(LAG(position.shares) OVER (PARTITION BY price.company_id ORDER BY price.date),0) AS prev_shares,
-- Get next non-null shares
COALESCE(LEAD(position.shares) OVER (PARTITION BY price.company_id ORDER BY price.date),0) AS next_shares,
-- Get last known value
COALESCE(LAST_VALUE(shares IGNORE NULLS) OVER (PARTITION BY price.company_id ORDER BY price.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS shares_forward_fill
-- get values from the price table table to have the daily price of each company, and other values that may be usefull for future transformations
FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.PRICE price
-- Left join with position table to get the values of shares per company per date, so that the position can be transformed to position USD in the following step. A left join was made to also identify unmatch rows to be filled on the next step
LEFT JOIN CODE_CHALLENGE_THI44K287NGA.SOURCE.POSITION position 
    ON position.company_id = price.company_id 
    AND position.date = price.date
)
SELECT 
    company.ID AS company_id, 
    company.ticker AS ticker, 
    company.sector_name AS sector_name,
    position_price.close_usd as close_usd, 
    position_price.volume as volume,
    position_price.shares as shares, 
    -- If there is no value of shares for the combination company and date between the tables position and price, either calculate the average between the last and next not null values of the column shares; if there is no value for either the previous or next not null shares value, pick the latest not null value as a replacement
    CASE 
        WHEN position_price.shares = 0 and position_price.prev_shares > 0 and position_price.next_shares > 0 THEN position_price.close_usd * ((position_price.prev_shares + position_price.next_shares) / 2)
        WHEN position_price.shares = 0 and shares_forward_fill > 0 then position_price.close_usd * shares_forward_fill
        ELSE position_price.close_usd * position_price.shares 
    END AS position_USD, 
    position_price.date AS date
FROM position_price
-- Join to the company table to get the descriptive values of each company. Because we want to depict the postion of each company we only want to match the companies that exist on the company table, as such a (inner) join condition is used
JOIN CODE_CHALLENGE_THI44K287NGA.SOURCE.company 
    ON company.id = position_price.company_id
ORDER BY company.ticker, position_price.date;

---------------- Q2 ------------------
CREATE OR REPLACE VIEW CODE_CHALLENGE_THI44K287NGA.SOURCE.top_avg_position
as
WITH
-- First CTE to preform the overall aggregaton per company and year and calculate the average position in USD
agg_position_usd_year as
(
select 
company_id, 
ticker, 
sector_name, 
-- formating the date to just the year, because in this anlysis we only want to consider the yearly result
year(date) as year, 
-- aggregate function to calculate the average position in USD
avg(position_USD) as avg_position_usd
from position_usd
-- Groupping by company_id, ticker, sector_name, year(date) to then calculate the aveage position in USD for all combinations of these variables. I used  the combination of the three company variables, company_id, ticker, sector_name, so I could also select them on the final query. However, the average result of the position in USD would be the same as just using company_id, because company_id is already a primary key in this table.
group by company_id, ticker, sector_name, year(date)
),
-- CTE to rank the companies by distribuiting them in a given order per buckets
rank_companies as
(
SELECT 
company_id, 
ticker, 
sector_name, 
avg_position_usd,
-- Distributes rows ordered by avg_position_usd in 4 buckets, so the highst 25% values will be saved on the first bucket
NTILE(4) OVER(order by avg_position_usd) as position_rank
FROM agg_position_usd_year
where year = year(current_date()) - 1
)

select rank_companies.company_id, ticker, sector_name, avg_position_usd 
from rank_companies 
-- Filtering of the previous CTE to only return the records on the first bucket, that represent the top 25% position in USD companies
where position_rank = 1;

---------------- Q3 ------------------
CREATE OR REPLACE VIEW CODE_CHALLENGE_THI44K287NGA.SOURCE.sector_position
AS
select 
sector_name, 
date, 
-- Aggregate function that sums the daily position in USD of each company within each sector
round(sum(position_usd),0) as 
position_usd
-- Use the view created in the first exercise, that contains the daily position in USD per company and respective sector name
from position_usd
-- Group by sector name and date to combine the data from the individual companies within each sector
group by sector_name, date;
