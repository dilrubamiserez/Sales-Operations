-- ============================================================
-- PIPELINE COVERAGE & PIPELINE AMOUNT BY TIME BUCKET PER REP
-- ============================================================

-- 1. Pipeline amount split into 0-30 days and 31-90 days per sales rep
SELECT
    Sales_Rep,
    Region,
    COUNT(*) AS total_open_deals,
    SUM(Deal_Size) AS total_pipeline,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 30
        THEN Deal_Size ELSE 0
    END) AS pipeline_0_30_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 30
        THEN 1 ELSE 0
    END) AS deals_0_30_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 31 AND 90
        THEN Deal_Size ELSE 0
    END) AS pipeline_31_90_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 31 AND 90
        THEN 1 ELSE 0
    END) AS deals_31_90_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) > 90
        THEN Deal_Size ELSE 0
    END) AS pipeline_90_plus_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) < 0
        THEN Deal_Size ELSE 0
    END) AS pipeline_overdue
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status = 'Open'
GROUP BY Sales_Rep, Region
ORDER BY total_pipeline DESC;


-- 2. Pipeline coverage per sales rep (pipeline vs quota)
--    Coverage = Total Pipeline / Quota Target
--    Weighted Coverage = Weighted Pipeline / Quota Target
SELECT
    Sales_Rep,
    Region,
    SUM(Deal_Size) AS total_pipeline,
    SUM(Deal_Size * Win_Probability / 100.0) AS weighted_pipeline,
    SUM(Quota_Contribution) AS quota_target,
    ROUND(
        SUM(Deal_Size) / NULLIF(SUM(Quota_Contribution), 0), 2
    ) AS pipeline_coverage_ratio,
    ROUND(
        SUM(Deal_Size * Win_Probability / 100.0) / NULLIF(SUM(Quota_Contribution), 0), 2
    ) AS weighted_coverage_ratio,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 30
        THEN Deal_Size ELSE 0
    END) AS pipeline_0_30_days,
    ROUND(
        SUM(CASE
            WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 30
            THEN Deal_Size ELSE 0
        END) / NULLIF(SUM(Quota_Contribution), 0), 2
    ) AS coverage_0_30_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 31 AND 90
        THEN Deal_Size ELSE 0
    END) AS pipeline_31_90_days,
    ROUND(
        SUM(CASE
            WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 31 AND 90
            THEN Deal_Size ELSE 0
        END) / NULLIF(SUM(Quota_Contribution), 0), 2
    ) AS coverage_31_90_days
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status = 'Open'
GROUP BY Sales_Rep, Region
ORDER BY pipeline_coverage_ratio DESC;


-- 3. Pipeline coverage by forecast category per rep
SELECT
    Sales_Rep,
    Region,
    Forecast_Category,
    COUNT(*) AS deal_count,
    SUM(Deal_Size) AS pipeline_value,
    SUM(Deal_Size * Win_Probability / 100.0) AS weighted_value,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 30
        THEN Deal_Size ELSE 0
    END) AS value_0_30_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 31 AND 90
        THEN Deal_Size ELSE 0
    END) AS value_31_90_days
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status = 'Open'
GROUP BY Sales_Rep, Region, Forecast_Category
ORDER BY Sales_Rep, Forecast_Category;


-- 4. Pipeline coverage summary by region
SELECT
    Region,
    COUNT(DISTINCT Sales_Rep) AS rep_count,
    SUM(Deal_Size) AS total_pipeline,
    SUM(Deal_Size * Win_Probability / 100.0) AS weighted_pipeline,
    SUM(Quota_Contribution) AS total_quota,
    ROUND(
        SUM(Deal_Size) / NULLIF(SUM(Quota_Contribution), 0), 2
    ) AS region_coverage_ratio,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 30
        THEN Deal_Size ELSE 0
    END) AS pipeline_0_30_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 31 AND 90
        THEN Deal_Size ELSE 0
    END) AS pipeline_31_90_days,
    SUM(CASE
        WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) > 90
        THEN Deal_Size ELSE 0
    END) AS pipeline_90_plus_days
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status = 'Open'
GROUP BY Region
ORDER BY region_coverage_ratio DESC;


-- 5. Reps with low pipeline coverage (under 3x quota — flag at risk)
WITH rep_coverage AS (
    SELECT
        Sales_Rep,
        Region,
        SUM(Deal_Size) AS total_pipeline,
        SUM(Quota_Contribution) AS quota_target,
        ROUND(
            SUM(Deal_Size) / NULLIF(SUM(Quota_Contribution), 0), 2
        ) AS coverage_ratio,
        SUM(CASE
            WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 30
            THEN Deal_Size ELSE 0
        END) AS pipeline_0_30_days,
        SUM(CASE
            WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 31 AND 90
            THEN Deal_Size ELSE 0
        END) AS pipeline_31_90_days
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status = 'Open'
    GROUP BY Sales_Rep, Region
)
SELECT
    Sales_Rep,
    Region,
    total_pipeline,
    quota_target,
    coverage_ratio,
    pipeline_0_30_days,
    pipeline_31_90_days,
    CASE
        WHEN coverage_ratio < 1 THEN 'CRITICAL'
        WHEN coverage_ratio < 2 THEN 'AT RISK'
        WHEN coverage_ratio < 3 THEN 'BELOW TARGET'
        ELSE 'HEALTHY'
    END AS coverage_health
FROM rep_coverage
ORDER BY coverage_ratio ASC;
