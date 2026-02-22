-- ============================================================
-- ADVANCED LEAD SOURCE ANALYSIS
-- ============================================================


-- ============================================================
-- 1. MULTI-TOUCH ATTRIBUTION
--    Which lead sources appear in winning deals at each stage?
-- ============================================================

-- 1.1 Lead source performance by current stage (where deals land)
SELECT
    Lead_Source,
    Stage,
    COUNT(*) AS deal_count,
    SUM(Deal_Size) AS total_value,
    AVG(Win_Probability) AS avg_win_probability,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) AS lost,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
    ) AS stage_win_rate_pct
FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Lead_Source, Stage
ORDER BY Lead_Source, Stage;


-- 1.2 Stage conversion matrix: how far do leads from each source get?
SELECT
    Lead_Source,
    COUNT(*) AS total_deals,
    SUM(CASE WHEN Stage IN ('Qualification', 'Discovery', 'Proposal', 'Negotiation', 'Contract', 'Closing', 'Closed Won')
        THEN 1 ELSE 0 END) AS past_qualification,
    SUM(CASE WHEN Stage IN ('Proposal', 'Negotiation', 'Contract', 'Closing', 'Closed Won')
        THEN 1 ELSE 0 END) AS past_proposal,
    SUM(CASE WHEN Stage IN ('Negotiation', 'Contract', 'Closing', 'Closed Won')
        THEN 1 ELSE 0 END) AS past_negotiation,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS closed_won,
    ROUND(
        SUM(CASE WHEN Stage IN ('Proposal', 'Negotiation', 'Contract', 'Closing', 'Closed Won') THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS pct_reach_proposal,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS pct_closed_won
FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Lead_Source
ORDER BY pct_closed_won DESC;


-- ============================================================
-- 2. LEAD SOURCE VELOCITY
--    How fast do deals from each source move through pipeline?
-- ============================================================

-- 2.1 Average sales cycle by lead source (won vs lost)
SELECT
    Lead_Source,
    Deal_Status,
    COUNT(*) AS deal_count,
    AVG(Sales_Cycle_Days) AS avg_cycle_days,
    MIN(Sales_Cycle_Days) AS fastest_cycle,
    MAX(Sales_Cycle_Days) AS slowest_cycle,
    AVG(Deal_Size) AS avg_deal_size
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status IN ('Won', 'Lost')
GROUP BY Lead_Source, Deal_Status
ORDER BY Lead_Source, Deal_Status;


-- 2.2 Velocity scorecard: speed + value combined
SELECT
    Lead_Source,
    COUNT(CASE WHEN Deal_Status = 'Won' THEN 1 END) AS won_deals,
    AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END) AS avg_won_cycle_days,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS total_won_value,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) * 1.0
        / NULLIF(AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END), 0), 0
    ) AS revenue_per_day,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
    ) AS win_rate_pct,
    RANK() OVER (ORDER BY
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) * 1.0
        / NULLIF(AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END), 0) DESC
    ) AS velocity_rank
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status IN ('Won', 'Lost')
GROUP BY Lead_Source
ORDER BY velocity_rank;


-- 2.3 Cycle length distribution by lead source
SELECT
    Lead_Source,
    CASE
        WHEN Sales_Cycle_Days BETWEEN 0 AND 15 THEN '01. 0-15 days'
        WHEN Sales_Cycle_Days BETWEEN 16 AND 30 THEN '02. 16-30 days'
        WHEN Sales_Cycle_Days BETWEEN 31 AND 60 THEN '03. 31-60 days'
        WHEN Sales_Cycle_Days BETWEEN 61 AND 90 THEN '04. 61-90 days'
        ELSE '05. 90+ days'
    END AS cycle_bucket,
    COUNT(*) AS deal_count,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS win_rate_pct
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status IN ('Won', 'Lost')
GROUP BY Lead_Source, cycle_bucket
ORDER BY Lead_Source, cycle_bucket;


-- ============================================================
-- 3. LEAD SOURCE DECAY RATE
--    What % of deals from each source go stale?
-- ============================================================

-- 3.1 Stall and decay rate by lead source
SELECT
    Lead_Source,
    COUNT(*) AS total_open_deals,
    SUM(CASE WHEN Is_Stalled = 'TRUE' THEN 1 ELSE 0 END) AS stalled_deals,
    ROUND(
        SUM(CASE WHEN Is_Stalled = 'TRUE' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS stall_rate_pct,
    SUM(CASE WHEN Is_Stalled = 'TRUE' THEN Deal_Size ELSE 0 END) AS stalled_value,
    AVG(CASE WHEN Is_Stalled = 'TRUE' THEN Days_in_Stage END) AS avg_days_stuck,
    AVG(
        DATEDIFF(CURDATE(), STR_TO_DATE(Last_Activity_Date, '%Y-%m-%d'))
    ) AS avg_days_since_activity
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status = 'Open'
GROUP BY Lead_Source
ORDER BY stall_rate_pct DESC;


-- 3.2 Decay funnel: created vs still active vs stalled vs lost
SELECT
    Lead_Source,
    COUNT(*) AS total_created,
    SUM(CASE WHEN Deal_Status = 'Open' AND Is_Stalled = 'FALSE' THEN 1 ELSE 0 END) AS active_open,
    SUM(CASE WHEN Deal_Status = 'Open' AND Is_Stalled = 'TRUE' THEN 1 ELSE 0 END) AS stalled,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) AS lost,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Open' AND Is_Stalled = 'TRUE' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS pct_decayed,
    ROUND(
        (SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END)
        + SUM(CASE WHEN Deal_Status = 'Open' AND Is_Stalled = 'TRUE' THEN 1 ELSE 0 END)) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS total_waste_pct
FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Lead_Source
ORDER BY total_waste_pct DESC;


-- ============================================================
-- 4. LEAD SOURCE TO DEAL TYPE FIT
--    New business vs upsell/cross-sell by source
-- ============================================================

SELECT
    Lead_Source,
    Deal_Type,
    COUNT(*) AS deal_count,
    SUM(Deal_Size) AS total_value,
    AVG(Deal_Size) AS avg_deal_size,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
    ) AS win_rate_pct,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY Lead_Source), 1
    ) AS pct_of_source_total
FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Lead_Source, Deal_Type
ORDER BY Lead_Source, deal_count DESC;


-- ============================================================
-- 5. LEAD SOURCE BY ACCOUNT TIER
--    Are premium sources landing enterprise or small accounts?
-- ============================================================

-- 5.1 Account tier distribution by lead source
SELECT
    Lead_Source,
    Account_Tier,
    COUNT(*) AS deal_count,
    SUM(Deal_Size) AS total_value,
    AVG(Deal_Size) AS avg_deal_size,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
    ) AS win_rate_pct,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY Lead_Source), 1
    ) AS pct_of_source
FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Lead_Source, Account_Tier
ORDER BY Lead_Source, Account_Tier;


-- 5.2 Source quality score: weighted by tier value
SELECT
    Lead_Source,
    COUNT(*) AS total_deals,
    SUM(CASE WHEN Account_Tier = 'Enterprise' THEN 1 ELSE 0 END) AS enterprise_deals,
    SUM(CASE WHEN Account_Tier = 'Mid-Market' THEN 1 ELSE 0 END) AS midmarket_deals,
    SUM(CASE WHEN Account_Tier = 'SMB' THEN 1 ELSE 0 END) AS smb_deals,
    ROUND(
        SUM(CASE WHEN Account_Tier = 'Enterprise' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS pct_enterprise,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0), 0
    ) AS revenue_per_lead,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' AND Account_Tier = 'Enterprise' THEN Deal_Size ELSE 0 END) * 1.0
        / NULLIF(SUM(CASE WHEN Account_Tier = 'Enterprise' THEN 1 ELSE 0 END), 0), 0
    ) AS revenue_per_enterprise_lead
FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Lead_Source
ORDER BY revenue_per_lead DESC;


-- ============================================================
-- 6. LEAD SOURCE CAMPAIGN ROI
--    Which campaigns from each source deliver the best returns?
-- ============================================================

-- 6.1 Campaign performance by lead source
SELECT
    Lead_Source,
    Campaign_Name,
    COUNT(*) AS total_deals,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) AS lost,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
    ) AS win_rate_pct,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS won_value,
    AVG(CASE WHEN Deal_Status = 'Won' THEN Deal_Size END) AS avg_won_deal_size,
    AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END) AS avg_won_cycle_days,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0), 0
    ) AS revenue_per_deal_generated
FROM retail_sales.`sales pipeline funnel nested`
WHERE Campaign_Name IS NOT NULL
  AND Campaign_Name != ''
GROUP BY Lead_Source, Campaign_Name
ORDER BY won_value DESC;


-- 6.2 Top 10 campaigns by total won value
SELECT
    Campaign_Name,
    Lead_Source,
    COUNT(*) AS total_deals,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
    ) AS win_rate_pct,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS total_won_value,
    GROUP_CONCAT(DISTINCT Account_Tier ORDER BY Account_Tier SEPARATOR ', ') AS tiers_reached
FROM retail_sales.`sales pipeline funnel nested`
WHERE Campaign_Name IS NOT NULL
  AND Campaign_Name != ''
  AND Deal_Status IN ('Won', 'Lost')
GROUP BY Campaign_Name, Lead_Source
ORDER BY total_won_value DESC
LIMIT 10;


-- ============================================================
-- 7. LEAD SOURCE SEASONALITY
--    Do certain sources perform better in specific periods?
-- ============================================================

-- 7.1 Monthly deal creation by lead source
SELECT
    Lead_Source,
    DATE_FORMAT(STR_TO_DATE(Created_Date, '%Y-%m-%d'), '%Y-%m') AS created_month,
    COUNT(*) AS deals_created,
    SUM(Deal_Size) AS pipeline_added,
    AVG(Deal_Size) AS avg_deal_size
FROM retail_sales.`sales pipeline funnel nested`
WHERE Created_Date IS NOT NULL
  AND Created_Date != ''
GROUP BY Lead_Source, created_month
ORDER BY Lead_Source, created_month;


-- 7.2 Quarterly win rate by lead source (spot seasonal patterns)
SELECT
    Lead_Source,
    CONCAT(YEAR(STR_TO_DATE(Close_Date, '%Y-%m-%d')), '-Q',
           QUARTER(STR_TO_DATE(Close_Date, '%Y-%m-%d'))) AS close_quarter,
    COUNT(*) AS closed_deals,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS win_rate_pct,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS won_value
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status IN ('Won', 'Lost')
  AND Close_Date IS NOT NULL
  AND Close_Date != ''
GROUP BY Lead_Source, close_quarter
ORDER BY Lead_Source, close_quarter;


-- 7.3 Best and worst month per lead source
WITH monthly_performance AS (
    SELECT
        Lead_Source,
        DATE_FORMAT(STR_TO_DATE(Created_Date, '%Y-%m-%d'), '%m') AS month_num,
        DATE_FORMAT(STR_TO_DATE(Created_Date, '%Y-%m-%d'), '%M') AS month_name,
        COUNT(*) AS deals_created,
        SUM(Deal_Size) AS pipeline_added
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Created_Date IS NOT NULL
      AND Created_Date != ''
    GROUP BY Lead_Source, month_num, month_name
),
ranked AS (
    SELECT
        Lead_Source,
        month_name,
        deals_created,
        pipeline_added,
        RANK() OVER (PARTITION BY Lead_Source ORDER BY deals_created DESC) AS best_rank,
        RANK() OVER (PARTITION BY Lead_Source ORDER BY deals_created ASC) AS worst_rank
    FROM monthly_performance
)
SELECT
    Lead_Source,
    month_name,
    deals_created,
    pipeline_added,
    CASE
        WHEN best_rank = 1 THEN 'BEST MONTH'
        WHEN worst_rank = 1 THEN 'WORST MONTH'
    END AS label
FROM ranked
WHERE best_rank = 1 OR worst_rank = 1
ORDER BY Lead_Source, best_rank;


-- ============================================================
-- 8. COMPETITIVE EXPOSURE BY SOURCE
--    Which sources bring deals that end in competitive losses?
-- ============================================================

-- 8.1 Competitive loss rate by lead source
SELECT
    Lead_Source,
    COUNT(CASE WHEN Deal_Status = 'Lost' THEN 1 END) AS total_losses,
    COUNT(CASE WHEN Deal_Status = 'Lost' AND Competitor_Name IS NOT NULL AND Competitor_Name != '' THEN 1 END) AS competitive_losses,
    ROUND(
        COUNT(CASE WHEN Deal_Status = 'Lost' AND Competitor_Name IS NOT NULL AND Competitor_Name != '' THEN 1 END) * 100.0
        / NULLIF(COUNT(CASE WHEN Deal_Status = 'Lost' THEN 1 END), 0), 1
    ) AS competitive_loss_rate_pct,
    SUM(CASE WHEN Deal_Status = 'Lost' AND Competitor_Name IS NOT NULL AND Competitor_Name != ''
        THEN Deal_Size ELSE 0 END) AS competitive_lost_value,
    GROUP_CONCAT(DISTINCT
        CASE WHEN Deal_Status = 'Lost' AND Competitor_Name IS NOT NULL AND Competitor_Name != ''
        THEN Competitor_Name END
        ORDER BY Competitor_Name SEPARATOR ', '
    ) AS competitors_faced
FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Lead_Source
ORDER BY competitive_loss_rate_pct DESC;


-- 8.2 Lead source x competitor matrix
SELECT
    Lead_Source,
    Competitor_Name,
    COUNT(*) AS losses_to_competitor,
    SUM(Deal_Size) AS lost_value,
    AVG(Deal_Size) AS avg_lost_deal_size,
    AVG(Sales_Cycle_Days) AS avg_cycle_days,
    GROUP_CONCAT(DISTINCT Reason_Lost ORDER BY Reason_Lost SEPARATOR ', ') AS loss_reasons
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status = 'Lost'
  AND Competitor_Name IS NOT NULL
  AND Competitor_Name != ''
GROUP BY Lead_Source, Competitor_Name
ORDER BY Lead_Source, losses_to_competitor DESC;


-- ============================================================
-- 9. LEAD SOURCE COMPOSITE SCORECARD
--    One-view summary ranking each source across all dimensions
-- ============================================================

SELECT
    Lead_Source,

    -- Volume
    COUNT(*) AS total_deals,

    -- Win Rate
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
    ) AS win_rate_pct,

    -- Revenue
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS total_won_value,

    -- Revenue per Lead
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0), 0
    ) AS revenue_per_lead,

    -- Velocity (avg won cycle)
    AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END) AS avg_won_cycle_days,

    -- Revenue per Day (velocity-adjusted value)
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) * 1.0
        / NULLIF(AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END), 0), 0
    ) AS revenue_per_day,

    -- Decay Rate
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Open' AND Is_Stalled = 'TRUE' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status = 'Open' THEN 1 ELSE 0 END), 0), 1
    ) AS stall_rate_pct,

    -- Enterprise %
    ROUND(
        SUM(CASE WHEN Account_Tier = 'Enterprise' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 1
    ) AS pct_enterprise,

    -- Competitive Loss %
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Lost' AND Competitor_Name IS NOT NULL AND Competitor_Name != '' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END), 0), 1
    ) AS competitive_loss_rate_pct,

    -- Overall Grade
    CASE
        WHEN ROUND(
                SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
             ) >= 60
             AND ROUND(
                SUM(CASE WHEN Deal_Status = 'Open' AND Is_Stalled = 'TRUE' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(SUM(CASE WHEN Deal_Status = 'Open' THEN 1 ELSE 0 END), 0), 1
             ) < 30
        THEN 'A - HIGH PERFORMER'
        WHEN ROUND(
                SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
             ) >= 40
        THEN 'B - SOLID'
        WHEN ROUND(
                SUM(CASE WHEN Deal_Status = 'Open' AND Is_Stalled = 'TRUE' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(SUM(CASE WHEN Deal_Status = 'Open' THEN 1 ELSE 0 END), 0), 1
             ) >= 50
        THEN 'D - HIGH DECAY'
        ELSE 'C - AVERAGE'
    END AS source_grade

FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Lead_Source
ORDER BY revenue_per_lead DESC;
