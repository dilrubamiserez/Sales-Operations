-- ============================================================
-- YTD WON AND LOST VALUES BY SALES REP
-- ============================================================

-- 1. YTD won vs lost by sales rep
SELECT
    Sales_Rep,
    Region,
    COUNT(CASE WHEN Deal_Status = 'Won' THEN 1 END) AS won_deals,
    COUNT(CASE WHEN Deal_Status = 'Lost' THEN 1 END) AS lost_deals,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS ytd_won_value,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN Deal_Size ELSE 0 END) AS ytd_lost_value,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END)
        - SUM(CASE WHEN Deal_Status = 'Lost' THEN Deal_Size ELSE 0 END) AS net_value,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS win_rate_pct,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) * 100.0
        / NULLIF(SUM(Deal_Size), 0), 1
    ) AS won_value_pct
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status IN ('Won', 'Lost')
  AND YEAR(STR_TO_DATE(Close_Date, '%Y-%m-%d')) = YEAR(CURDATE())
GROUP BY Sales_Rep, Region
ORDER BY ytd_won_value DESC;


-- 2. YTD won vs lost by rep with quota attainment
SELECT
    Sales_Rep,
    Region,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS ytd_won_value,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN Deal_Size ELSE 0 END) AS ytd_lost_value,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Quota_Contribution ELSE 0 END) AS ytd_quota_contribution,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status = 'Won' THEN Quota_Contribution ELSE 0 END), 0), 1
    ) AS quota_attainment_pct,
    AVG(CASE WHEN Deal_Status = 'Won' THEN Deal_Size END) AS avg_won_deal_size,
    AVG(CASE WHEN Deal_Status = 'Lost' THEN Deal_Size END) AS avg_lost_deal_size,
    AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END) AS avg_won_cycle_days,
    AVG(CASE WHEN Deal_Status = 'Lost' THEN Sales_Cycle_Days END) AS avg_lost_cycle_days
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status IN ('Won', 'Lost')
  AND YEAR(STR_TO_DATE(Close_Date, '%Y-%m-%d')) = YEAR(CURDATE())
GROUP BY Sales_Rep, Region
ORDER BY ytd_won_value DESC;


-- 3.  Summary by sales rep (deals in stage>7)
SELECT
    Sales_Rep,
    Region,
    COUNT(*) AS stuck_deals,
    SUM(Deal_Size) AS total_stuck_value,
    AVG(Days_in_Stage) AS avg_days_stuck,
    GROUP_CONCAT(DISTINCT Stage ORDER BY Stage SEPARATOR ', ') AS stages_affected
FROM retail_sales.`sales pipeline funnel nested`
WHERE Days_in_Stage > 7
  AND Deal_Status = 'Open'
GROUP BY Sales_Rep, Region
ORDER BY stuck_deals DESC;

-- 3. YTD monthly progression by rep
SELECT
    Sales_Rep,
    DATE_FORMAT(STR_TO_DATE(Close_Date, '%Y-%m-%d'), '%Y-%m') AS close_month,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS won_value,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN Deal_Size ELSE 0 END) AS lost_value,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won_deals,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) AS lost_deals
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status IN ('Won', 'Lost')
  AND YEAR(STR_TO_DATE(Close_Date, '%Y-%m-%d')) = YEAR(CURDATE())
  AND Close_Date IS NOT NULL
  AND Close_Date != ''
GROUP BY Sales_Rep, close_month
ORDER BY Sales_Rep, close_month;


-- 4. Summary: which months hit the peak and which were lowest
WITH monthly_deals AS (
    SELECT
        DATE_FORMAT(STR_TO_DATE(Created_Date, '%Y-%m-%d'), '%Y-%m') AS created_month,
        COUNT(*) AS deals_created,
        SUM(Deal_Size) AS total_pipeline_added
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Created_Date IS NOT NULL
      AND Created_Date != ''
    GROUP BY created_month
)
SELECT
    created_month,
    deals_created,
    total_pipeline_added,
    RANK() OVER (ORDER BY deals_created DESC) AS rank_by_count,
    CASE
        WHEN deals_created = (SELECT MAX(deals_created) FROM monthly_deals) THEN 'PEAK'
        WHEN deals_created = (SELECT MIN(deals_created) FROM monthly_deals) THEN 'LOWEST'
        ELSE ''
    END AS flag
FROM monthly_deals
ORDER BY deals_created DESC;



-- 5. YTD rep leaderboard with rank
WITH rep_ytd AS (
    SELECT
        Sales_Rep,
        Region,
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS ytd_won_value,
        SUM(CASE WHEN Deal_Status = 'Lost' THEN Deal_Size ELSE 0 END) AS ytd_lost_value,
        COUNT(CASE WHEN Deal_Status = 'Won' THEN 1 END) AS won_deals,
        COUNT(CASE WHEN Deal_Status = 'Lost' THEN 1 END) AS lost_deals
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status IN ('Won', 'Lost')
      AND YEAR(STR_TO_DATE(Close_Date, '%Y-%m-%d')) = YEAR(CURDATE())
    GROUP BY Sales_Rep, Region
)
SELECT
    RANK() OVER (ORDER BY ytd_won_value DESC) AS leaderboard_rank,
    Sales_Rep,
    Region,
    won_deals,
    lost_deals,
    ytd_won_value,
    ytd_lost_value,
    ytd_won_value - ytd_lost_value AS net_value,
    ROUND(won_deals * 100.0 / NULLIF(won_deals + lost_deals, 0), 1) AS win_rate_pct
FROM rep_ytd
ORDER BY leaderboard_rank;
