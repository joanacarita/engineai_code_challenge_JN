---------------- Q1 ------------------
CREATE OR REPLACE VIEW CODE_CHALLENGE_THI44K287NGA.SOURCE.position_usd AS
SELECT 
    company.ID AS company_id, 
    company.ticker AS ticker, 
    company.sector_name AS sector_name,
    price.close_usd, 
    price.volume,
    position.shares, 
    CASE 
        WHEN position.shares IS NULL THEN price.close_usd * price.volume
        ELSE price.close_usd * position.shares 
    END AS position_USD, 
    price.date AS date
FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.PRICE price
LEFT JOIN CODE_CHALLENGE_THI44K287NGA.SOURCE.company company 
    ON company.id = price.company_id
LEFT JOIN CODE_CHALLENGE_THI44K287NGA.SOURCE.POSITION position 
    ON position.company_id = price.company_id 
    AND position.date = price.date
ORDER BY company.ticker, price.date;

select * from position_usd
where position_USD is null and year(date) = 2023 

---------------- Q2 ------------------
CREATE OR REPLACE VIEW CODE_CHALLENGE_THI44K287NGA.SOURCE.top_avg_position
as
WITH
agg_position_usd_year as
(
select company_id, ticker, year(date) as year, avg(position_USD) as avg_position_usd
from position_usd
group by company_id, ticker, year(date)
),
rank_companies as
(
SELECT company_id, ticker, avg_position_usd,
NTILE(4) OVER(order by avg_position_usd) as position_rank
FROM agg_position_usd_year
where year = year(current_date()) - 1
)

select company_id, ticker, avg_position_usd 
from rank_companies 
where position_rank = 1

---------------- Q3 ------------------
CREATE OR REPLACE VIEW CODE_CHALLENGE_THI44K287NGA.SOURCE.sector_position
AS
select sector_name, date, sum(position_usd) as position_usd
from position_usd
group by sector_name, date

