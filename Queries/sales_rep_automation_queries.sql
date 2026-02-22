-- ============================================================
-- SALES REP AUTOMATION QUERIES
-- Reduce manual work through data-driven automation
-- ============================================================


-- ============================================================
-- 1. AUTOMATED DEAL HEALTH SCORING
--    Score every open deal 0-100 based on risk signals
-- ============================================================

-- 1.1 Deal health score with weighted risk factors
WITH stage_benchmarks AS (
    SELECT
        Stage,
        AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END) AS avg_won_cycle,
        AVG(CASE WHEN Deal_Status = 'Lost' THEN Sales_Cycle_Days END) AS avg_lost_cycle,
        AVG(CASE WHEN Deal_Status = 'Won' THEN Days_in_Stage END) AS avg_won_days_in_stage
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status IN ('Won', 'Lost')
    GROUP BY Stage
)
SELECT
    d.Opportunity_ID,
    d.Opportunity_Name,
    d.Sales_Rep,
    d.Stage,
    d.Deal_Size,
    d.Win_Probability,
    d.Days_in_Stage,
    d.Sales_Cycle_Days,
    d.Is_Stalled,
    DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) AS days_since_activity,
    DATEDIFF(STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d'), CURDATE()) AS days_to_expected_close,

    -- Health Score: start at 100, deduct for risk signals
    GREATEST(0, LEAST(100,
        100
        -- Stalled penalty (-25)
        - (CASE WHEN d.Is_Stalled = 'TRUE' THEN 25 ELSE 0 END)
        -- Overdue expected close (-20)
        - (CASE WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE() THEN 20 ELSE 0 END)
        -- Exceeds avg lost cycle for stage (-20)
        - (CASE WHEN d.Sales_Cycle_Days > COALESCE(b.avg_lost_cycle, 999) THEN 20 ELSE 0 END)
        -- Days in stage exceeds 2x won average (-15)
        - (CASE WHEN d.Days_in_Stage > COALESCE(b.avg_won_days_in_stage * 2, 999) THEN 15 ELSE 0 END)
        -- No activity in 14+ days (-10)
        - (CASE WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > 14 THEN 10 ELSE 0 END)
        -- No activity in 7-14 days (-5)
        - (CASE WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) BETWEEN 7 AND 14 THEN 5 ELSE 0 END)
        -- Low win probability (-10)
        - (CASE WHEN d.Win_Probability < 30 THEN 10 ELSE 0 END)
    )) AS health_score,

    -- Human-readable risk summary
    CONCAT_WS(', ',
        CASE WHEN d.Is_Stalled = 'TRUE' THEN 'STALLED' END,
        CASE WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE() THEN 'OVERDUE' END,
        CASE WHEN d.Sales_Cycle_Days > COALESCE(b.avg_lost_cycle, 999) THEN 'EXCEEDS LOST CYCLE AVG' END,
        CASE WHEN d.Days_in_Stage > COALESCE(b.avg_won_days_in_stage * 2, 999) THEN 'STUCK IN STAGE' END,
        CASE WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > 14 THEN 'NO RECENT ACTIVITY' END,
        CASE WHEN d.Win_Probability < 30 THEN 'LOW PROBABILITY' END
    ) AS risk_flags,

    -- Action category
    CASE
        WHEN (CASE WHEN d.Is_Stalled = 'TRUE' THEN 25 ELSE 0 END)
           + (CASE WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE() THEN 20 ELSE 0 END)
           + (CASE WHEN d.Sales_Cycle_Days > COALESCE(b.avg_lost_cycle, 999) THEN 20 ELSE 0 END)
           >= 40
        THEN 'INTERVENE NOW'
        WHEN (CASE WHEN d.Days_in_Stage > COALESCE(b.avg_won_days_in_stage * 2, 999) THEN 15 ELSE 0 END)
           + (CASE WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > 14 THEN 10 ELSE 0 END)
           >= 15
        THEN 'NEEDS ATTENTION'
        ELSE 'MONITOR'
    END AS action_category

FROM retail_sales.`sales pipeline funnel nested` d
LEFT JOIN stage_benchmarks b ON d.Stage = b.Stage
WHERE d.Deal_Status = 'Open'
ORDER BY health_score ASC, d.Deal_Size DESC;


-- 1.2 Health score distribution per rep (manager view)
WITH scored_deals AS (
    SELECT
        d.Sales_Rep,
        d.Region,
        d.Deal_Size,
        GREATEST(0, LEAST(100,
            100
            - (CASE WHEN d.Is_Stalled = 'TRUE' THEN 25 ELSE 0 END)
            - (CASE WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE() THEN 20 ELSE 0 END)
            - (CASE WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > 14 THEN 10 ELSE 0 END)
            - (CASE WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) BETWEEN 7 AND 14 THEN 5 ELSE 0 END)
            - (CASE WHEN d.Win_Probability < 30 THEN 10 ELSE 0 END)
        )) AS health_score
    FROM retail_sales.`sales pipeline funnel nested` d
    WHERE d.Deal_Status = 'Open'
)
SELECT
    Sales_Rep,
    Region,
    COUNT(*) AS open_deals,
    ROUND(AVG(health_score), 1) AS avg_health_score,
    SUM(CASE WHEN health_score < 40 THEN 1 ELSE 0 END) AS critical_deals,
    SUM(CASE WHEN health_score BETWEEN 40 AND 69 THEN 1 ELSE 0 END) AS at_risk_deals,
    SUM(CASE WHEN health_score >= 70 THEN 1 ELSE 0 END) AS healthy_deals,
    SUM(CASE WHEN health_score < 40 THEN Deal_Size ELSE 0 END) AS critical_value,
    ROUND(
        SUM(CASE WHEN health_score < 40 THEN Deal_Size ELSE 0 END) * 100.0
        / NULLIF(SUM(Deal_Size), 0), 1
    ) AS pct_pipeline_critical
FROM scored_deals
GROUP BY Sales_Rep, Region
ORDER BY avg_health_score ASC;


-- ============================================================
-- 2. SMART PRIORITY QUEUE
--    Daily ranked action list per rep
-- ============================================================

SELECT
    d.Sales_Rep,
    d.Opportunity_ID,
    d.Opportunity_Name,
    d.Stage,
    d.Deal_Size,
    d.Account_Tier,
    d.Days_in_Stage,
    DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) AS days_since_activity,
    DATEDIFF(STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d'), CURDATE()) AS days_to_close,
    d.Is_Stalled,
    d.Forecast_Category,

    -- Priority Score (higher = more urgent)
    (
        -- Overdue deals get highest priority
        (CASE WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE() THEN 50 ELSE 0 END)
        -- Closing within 7 days
        + (CASE WHEN DATEDIFF(STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 7 THEN 40 ELSE 0 END)
        -- Stalled deals
        + (CASE WHEN d.Is_Stalled = 'TRUE' THEN 30 ELSE 0 END)
        -- No activity 7+ days
        + (CASE WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > 7 THEN 20 ELSE 0 END)
        -- Commit deals prioritized
        + (CASE WHEN d.Forecast_Category = 'Commit' THEN 15 ELSE 0 END)
        -- Enterprise tier priority
        + (CASE WHEN d.Account_Tier = 'Enterprise' THEN 10 ELSE 0 END)
        -- Deal size weight (normalize large deals)
        + LEAST(d.Deal_Size / 10000, 10)
    ) AS priority_score,

    -- Suggested action
    CASE
        WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE() AND d.Is_Stalled = 'TRUE'
            THEN 'ESCALATE: Overdue & stalled — involve manager'
        WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE()
            THEN 'URGENT: Update close date or push to close'
        WHEN DATEDIFF(STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 7
            THEN 'CLOSING: Final push — confirm next steps'
        WHEN d.Is_Stalled = 'TRUE' AND d.Stage IN ('Negotiation', 'Proposal', 'Contract')
            THEN 'RE-ENGAGE: Late-stage stall — schedule call'
        WHEN d.Is_Stalled = 'TRUE'
            THEN 'REVIVE: Send value-add content or re-qualify'
        WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > 14
            THEN 'FOLLOW UP: No activity in 2+ weeks'
        WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > 7
            THEN 'CHECK IN: No activity in 1+ week'
        ELSE 'ON TRACK: Continue current cadence'
    END AS suggested_action,

    ROW_NUMBER() OVER (PARTITION BY d.Sales_Rep ORDER BY
        (CASE WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE() THEN 50 ELSE 0 END)
        + (CASE WHEN DATEDIFF(STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 7 THEN 40 ELSE 0 END)
        + (CASE WHEN d.Is_Stalled = 'TRUE' THEN 30 ELSE 0 END)
        + (CASE WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > 7 THEN 20 ELSE 0 END)
        + (CASE WHEN d.Forecast_Category = 'Commit' THEN 15 ELSE 0 END)
        DESC
    ) AS daily_rank

FROM retail_sales.`sales pipeline funnel nested` d
WHERE d.Deal_Status = 'Open'
ORDER BY d.Sales_Rep, daily_rank;


-- ============================================================
-- 3. AUTO-GENERATED FORECAST SUGGESTION
--    Suggest forecast category based on historical patterns
-- ============================================================

WITH historical_rates AS (
    SELECT
        Stage,
        CASE
            WHEN Deal_Size < 10000 THEN 'Small'
            WHEN Deal_Size BETWEEN 10000 AND 49999 THEN 'Mid'
            WHEN Deal_Size BETWEEN 50000 AND 99999 THEN 'Large'
            ELSE 'Enterprise'
        END AS size_tier,
        ROUND(
            SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
        ) AS historical_win_rate
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status IN ('Won', 'Lost')
    GROUP BY Stage, size_tier
)
SELECT
    d.Opportunity_ID,
    d.Opportunity_Name,
    d.Sales_Rep,
    d.Stage,
    d.Deal_Size,
    d.Win_Probability,
    d.Forecast_Category AS current_forecast,
    d.Days_in_Stage,
    d.Is_Stalled,
    h.historical_win_rate,

    -- Suggested forecast category based on data
    CASE
        WHEN d.Is_Stalled = 'TRUE' AND d.Days_in_Stage > 14 THEN 'Omit'
        WHEN STR_TO_DATE(d.Expected_Close_Date, '%Y-%m-%d') < CURDATE() AND d.Is_Stalled = 'TRUE' THEN 'Omit'
        WHEN h.historical_win_rate >= 70 AND d.Is_Stalled = 'FALSE'
             AND d.Stage IN ('Negotiation', 'Contract', 'Closing') THEN 'Commit'
        WHEN h.historical_win_rate >= 50 AND d.Is_Stalled = 'FALSE' THEN 'Best Case'
        WHEN h.historical_win_rate >= 30 THEN 'Upside'
        ELSE 'Pipeline'
    END AS suggested_forecast,

    -- Flag mismatches
    CASE
        WHEN d.Forecast_Category = 'Commit' AND d.Is_Stalled = 'TRUE'
            THEN 'MISMATCH: Committed but stalled'
        WHEN d.Forecast_Category = 'Commit' AND h.historical_win_rate < 40
            THEN 'MISMATCH: Committed but low historical win rate'
        WHEN d.Forecast_Category IN ('Pipeline', 'Upside') AND h.historical_win_rate >= 70
             AND d.Stage IN ('Negotiation', 'Contract', 'Closing')
            THEN 'UNDER-CALLED: Consider upgrading to Commit'
        ELSE ''
    END AS forecast_flag

FROM retail_sales.`sales pipeline funnel nested` d
LEFT JOIN historical_rates h
    ON d.Stage = h.Stage
    AND (CASE
            WHEN d.Deal_Size < 10000 THEN 'Small'
            WHEN d.Deal_Size BETWEEN 10000 AND 49999 THEN 'Mid'
            WHEN d.Deal_Size BETWEEN 50000 AND 99999 THEN 'Large'
            ELSE 'Enterprise'
        END) = h.size_tier
WHERE d.Deal_Status = 'Open'
ORDER BY d.Sales_Rep, d.Stage;


-- ============================================================
-- 4. PIPELINE GAP ALERTS
--    Flag reps whose mid-term pipeline is dangerously thin
-- ============================================================

WITH rep_pipeline AS (
    SELECT
        Sales_Rep,
        Region,
        SUM(Deal_Size) AS total_pipeline,
        SUM(CASE
            WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 0 AND 30
            THEN Deal_Size ELSE 0
        END) AS pipeline_0_30,
        SUM(CASE
            WHEN DATEDIFF(STR_TO_DATE(Expected_Close_Date, '%Y-%m-%d'), CURDATE()) BETWEEN 31 AND 90
            THEN Deal_Size ELSE 0
        END) AS pipeline_31_90,
        SUM(Quota_Contribution) AS quota_target
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status = 'Open'
    GROUP BY Sales_Rep, Region
),
ytd_won AS (
    SELECT
        Sales_Rep,
        SUM(Deal_Size) AS ytd_won_value
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status = 'Won'
      AND YEAR(STR_TO_DATE(Close_Date, '%Y-%m-%d')) = YEAR(CURDATE())
    GROUP BY Sales_Rep
)
SELECT
    p.Sales_Rep,
    p.Region,
    p.total_pipeline,
    p.pipeline_0_30,
    p.pipeline_31_90,
    p.quota_target,
    COALESCE(w.ytd_won_value, 0) AS ytd_won,
    ROUND(p.total_pipeline / NULLIF(p.quota_target, 0), 2) AS coverage_ratio,

    -- Alert type
    CASE
        WHEN p.pipeline_31_90 = 0 AND p.pipeline_0_30 = 0
            THEN 'CRITICAL: No pipeline in next 90 days'
        WHEN p.pipeline_31_90 = 0
            THEN 'ALERT: No pipeline for days 31-90 — next quarter at risk'
        WHEN p.pipeline_31_90 < p.quota_target * 0.5
            THEN 'WARNING: Mid-term pipeline below 50% of quota'
        WHEN p.pipeline_0_30 < p.quota_target * 0.3
            THEN 'WARNING: Short-term pipeline below 30% of quota'
        ELSE 'OK'
    END AS pipeline_alert,

    -- Recommended action
    CASE
        WHEN p.pipeline_31_90 = 0
            THEN 'Prioritize prospecting and outbound immediately'
        WHEN p.pipeline_31_90 < p.quota_target * 0.5
            THEN 'Increase outbound activity and work marketing leads'
        WHEN p.pipeline_0_30 < p.quota_target * 0.3
            THEN 'Focus on accelerating existing mid-stage deals'
        ELSE 'Maintain current activity levels'
    END AS recommended_action

FROM rep_pipeline p
LEFT JOIN ytd_won w ON p.Sales_Rep = w.Sales_Rep
ORDER BY FIELD(pipeline_alert,
    'CRITICAL: No pipeline in next 90 days',
    'ALERT: No pipeline for days 31-90 — next quarter at risk',
    'WARNING: Mid-term pipeline below 50% of quota',
    'WARNING: Short-term pipeline below 30% of quota',
    'OK'),
    p.total_pipeline ASC;


-- ============================================================
-- 5. STALE DEAL AUTO-REMINDERS
--    Flag deals with no recent activity, thresholds per stage
-- ============================================================

WITH stage_thresholds AS (
    -- Define acceptable inactivity window per stage
    SELECT 'Prospecting' AS Stage, 5 AS max_inactive_days UNION ALL
    SELECT 'Qualification', 5 UNION ALL
    SELECT 'Discovery', 7 UNION ALL
    SELECT 'Proposal', 7 UNION ALL
    SELECT 'Negotiation', 5 UNION ALL
    SELECT 'Contract', 3 UNION ALL
    SELECT 'Closing', 3
)
SELECT
    d.Sales_Rep,
    d.Opportunity_ID,
    d.Opportunity_Name,
    d.Stage,
    d.Deal_Size,
    d.Account_Name,
    d.Last_Activity_Date,
    DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) AS days_since_activity,
    t.max_inactive_days AS stage_threshold,
    DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) - t.max_inactive_days AS days_overdue,

    -- Reminder urgency
    CASE
        WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > t.max_inactive_days * 3
            THEN 'CRITICAL: 3x overdue — likely dead'
        WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > t.max_inactive_days * 2
            THEN 'URGENT: 2x overdue — re-engage immediately'
        WHEN DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > t.max_inactive_days
            THEN 'REMINDER: Past activity threshold'
        ELSE 'OK'
    END AS reminder_level,

    -- Suggested outreach
    CASE
        WHEN d.Stage IN ('Prospecting', 'Qualification')
            THEN 'Send re-engagement email or reassign lead'
        WHEN d.Stage = 'Discovery'
            THEN 'Schedule discovery call follow-up'
        WHEN d.Stage = 'Proposal'
            THEN 'Follow up on proposal — ask for feedback'
        WHEN d.Stage IN ('Negotiation', 'Contract')
            THEN 'Call directly — resolve blockers'
        WHEN d.Stage = 'Closing'
            THEN 'Escalate — involve manager to close'
        ELSE 'Review and update deal status'
    END AS suggested_outreach

FROM retail_sales.`sales pipeline funnel nested` d
JOIN stage_thresholds t ON d.Stage = t.Stage
WHERE d.Deal_Status = 'Open'
  AND DATEDIFF(CURDATE(), STR_TO_DATE(d.Last_Activity_Date, '%Y-%m-%d')) > t.max_inactive_days
ORDER BY d.Sales_Rep,
    FIELD(reminder_level, 'CRITICAL: 3x overdue — likely dead', 'URGENT: 2x overdue — re-engage immediately', 'REMINDER: Past activity threshold'),
    d.Deal_Size DESC;


-- ============================================================
-- 6. LOSS PATTERN COACHING PROMPTS
--    Surface coaching tips when a deal matches a losing pattern
-- ============================================================

-- 6.1 Build loss pattern profiles
WITH loss_patterns AS (
    SELECT
        Lead_Source,
        Industry,
        CASE
            WHEN Deal_Size < 10000 THEN 'Small'
            WHEN Deal_Size BETWEEN 10000 AND 49999 THEN 'Mid'
            WHEN Deal_Size BETWEEN 50000 AND 99999 THEN 'Large'
            ELSE 'Enterprise'
        END AS size_tier,
        COUNT(*) AS total_closed,
        SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) AS lost,
        ROUND(
            SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 1
        ) AS loss_rate_pct,
        GROUP_CONCAT(DISTINCT
            CASE WHEN Deal_Status = 'Lost' AND Reason_Lost IS NOT NULL AND Reason_Lost != ''
            THEN Reason_Lost END
            ORDER BY Reason_Lost SEPARATOR ', '
        ) AS common_loss_reasons,
        GROUP_CONCAT(DISTINCT
            CASE WHEN Deal_Status = 'Lost' AND Competitor_Name IS NOT NULL AND Competitor_Name != ''
            THEN Competitor_Name END
            ORDER BY Competitor_Name SEPARATOR ', '
        ) AS competitors
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status IN ('Won', 'Lost')
    GROUP BY Lead_Source, Industry, size_tier
    HAVING COUNT(*) >= 3
)
SELECT * FROM loss_patterns
WHERE loss_rate_pct >= 50
ORDER BY loss_rate_pct DESC, total_closed DESC;


-- 6.2 Match open deals against losing patterns with coaching tips
WITH loss_patterns AS (
    SELECT
        Lead_Source,
        Industry,
        CASE
            WHEN Deal_Size < 10000 THEN 'Small'
            WHEN Deal_Size BETWEEN 10000 AND 49999 THEN 'Mid'
            WHEN Deal_Size BETWEEN 50000 AND 99999 THEN 'Large'
            ELSE 'Enterprise'
        END AS size_tier,
        ROUND(
            SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 1
        ) AS loss_rate_pct,
        GROUP_CONCAT(DISTINCT
            CASE WHEN Deal_Status = 'Lost' AND Reason_Lost IS NOT NULL AND Reason_Lost != ''
            THEN Reason_Lost END
            ORDER BY Reason_Lost SEPARATOR ', '
        ) AS common_loss_reasons
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status IN ('Won', 'Lost')
    GROUP BY Lead_Source, Industry, size_tier
    HAVING COUNT(*) >= 3
       AND ROUND(
            SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 1
       ) >= 50
)
SELECT
    d.Opportunity_ID,
    d.Opportunity_Name,
    d.Sales_Rep,
    d.Stage,
    d.Deal_Size,
    d.Lead_Source,
    d.Industry,
    lp.loss_rate_pct AS pattern_loss_rate,
    lp.common_loss_reasons,

    -- Coaching prompt
    CONCAT(
        'WARNING: Deals from ', d.Lead_Source,
        ' in ', d.Industry,
        ' (', (CASE
            WHEN d.Deal_Size < 10000 THEN 'Small'
            WHEN d.Deal_Size BETWEEN 10000 AND 49999 THEN 'Mid'
            WHEN d.Deal_Size BETWEEN 50000 AND 99999 THEN 'Large'
            ELSE 'Enterprise'
        END), ' tier)',
        ' historically lose ', lp.loss_rate_pct, '% of the time.',
        ' Common reasons: ', COALESCE(lp.common_loss_reasons, 'Unknown'),
        '. Consider adjusting strategy early.'
    ) AS coaching_prompt

FROM retail_sales.`sales pipeline funnel nested` d
JOIN loss_patterns lp
    ON d.Lead_Source = lp.Lead_Source
    AND d.Industry = lp.Industry
    AND (CASE
            WHEN d.Deal_Size < 10000 THEN 'Small'
            WHEN d.Deal_Size BETWEEN 10000 AND 49999 THEN 'Mid'
            WHEN d.Deal_Size BETWEEN 50000 AND 99999 THEN 'Large'
            ELSE 'Enterprise'
        END) = lp.size_tier
WHERE d.Deal_Status = 'Open'
ORDER BY lp.loss_rate_pct DESC, d.Deal_Size DESC;


-- ============================================================
-- 7. END-OF-WEEK AUTO-SUMMARY PER REP
--    Weekly digest: what happened this week
-- ============================================================

-- 7.1 Deals created this week
SELECT
    Sales_Rep,
    Region,
    COUNT(*) AS deals_created_this_week,
    SUM(Deal_Size) AS new_pipeline_value,
    AVG(Deal_Size) AS avg_new_deal_size,
    GROUP_CONCAT(Opportunity_Name ORDER BY Deal_Size DESC SEPARATOR ', ') AS new_deals
FROM retail_sales.`sales pipeline funnel nested`
WHERE STR_TO_DATE(Created_Date, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)
GROUP BY Sales_Rep, Region
ORDER BY new_pipeline_value DESC;


-- 7.2 Deals closed this week (won & lost)
SELECT
    Sales_Rep,
    Region,
    SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) AS won_this_week,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN 1 ELSE 0 END) AS lost_this_week,
    SUM(CASE WHEN Deal_Status = 'Won' THEN Deal_Size ELSE 0 END) AS won_value,
    SUM(CASE WHEN Deal_Status = 'Lost' THEN Deal_Size ELSE 0 END) AS lost_value
FROM retail_sales.`sales pipeline funnel nested`
WHERE Deal_Status IN ('Won', 'Lost')
  AND STR_TO_DATE(Close_Date, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)
GROUP BY Sales_Rep, Region
ORDER BY won_value DESC;


-- 7.3 Deals that became stalled this week
SELECT
    Sales_Rep,
    Region,
    COUNT(*) AS newly_stalled,
    SUM(Deal_Size) AS stalled_value,
    GROUP_CONCAT(
        CONCAT(Opportunity_Name, ' (', Stage, ')') ORDER BY Deal_Size DESC SEPARATOR '; '
    ) AS stalled_deals
FROM retail_sales.`sales pipeline funnel nested`
WHERE Is_Stalled = 'TRUE'
  AND Deal_Status = 'Open'
  AND DATEDIFF(CURDATE(), STR_TO_DATE(Last_Activity_Date, '%Y-%m-%d')) BETWEEN 7 AND 14
GROUP BY Sales_Rep, Region
ORDER BY stalled_value DESC;


-- 7.4 Full weekly digest per rep
SELECT
    r.Sales_Rep,
    r.Region,

    -- Created
    COALESCE(c.deals_created, 0) AS deals_created,
    COALESCE(c.new_pipeline, 0) AS new_pipeline_value,

    -- Won
    COALESCE(w.won_count, 0) AS deals_won,
    COALESCE(w.won_value, 0) AS won_value,

    -- Lost
    COALESCE(l.lost_count, 0) AS deals_lost,
    COALESCE(l.lost_value, 0) AS lost_value,

    -- Open pipeline snapshot
    r.open_deals,
    r.open_pipeline,
    r.stalled_deals,
    r.stalled_value

FROM (
    SELECT
        Sales_Rep,
        Region,
        COUNT(*) AS open_deals,
        SUM(Deal_Size) AS open_pipeline,
        SUM(CASE WHEN Is_Stalled = 'TRUE' THEN 1 ELSE 0 END) AS stalled_deals,
        SUM(CASE WHEN Is_Stalled = 'TRUE' THEN Deal_Size ELSE 0 END) AS stalled_value
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status = 'Open'
    GROUP BY Sales_Rep, Region
) r

LEFT JOIN (
    SELECT Sales_Rep, COUNT(*) AS deals_created, SUM(Deal_Size) AS new_pipeline
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE STR_TO_DATE(Created_Date, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)
    GROUP BY Sales_Rep
) c ON r.Sales_Rep = c.Sales_Rep

LEFT JOIN (
    SELECT Sales_Rep, COUNT(*) AS won_count, SUM(Deal_Size) AS won_value
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status = 'Won'
      AND STR_TO_DATE(Close_Date, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)
    GROUP BY Sales_Rep
) w ON r.Sales_Rep = w.Sales_Rep

LEFT JOIN (
    SELECT Sales_Rep, COUNT(*) AS lost_count, SUM(Deal_Size) AS lost_value
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status = 'Lost'
      AND STR_TO_DATE(Close_Date, '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)
    GROUP BY Sales_Rep
) l ON r.Sales_Rep = l.Sales_Rep

ORDER BY r.Sales_Rep;


-- ============================================================
-- 8. SMART DEAL ROUTING
--    Match new deals to best-fit reps based on performance data
-- ============================================================

-- 8.1 Rep capability profile (used for routing decisions)
SELECT
    Sales_Rep,
    Region,
    Rep_Tenure_Months,

    -- Volume capacity
    COUNT(CASE WHEN Deal_Status = 'Open' THEN 1 END) AS current_open_deals,
    SUM(CASE WHEN Deal_Status = 'Open' THEN Deal_Size ELSE 0 END) AS current_pipeline,

    -- Win rate by size tier
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' AND Deal_Size < 10000 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') AND Deal_Size < 10000 THEN 1 ELSE 0 END), 0), 1
    ) AS small_deal_win_rate,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' AND Deal_Size BETWEEN 10000 AND 49999 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') AND Deal_Size BETWEEN 10000 AND 49999 THEN 1 ELSE 0 END), 0), 1
    ) AS mid_deal_win_rate,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' AND Deal_Size BETWEEN 50000 AND 99999 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') AND Deal_Size BETWEEN 50000 AND 99999 THEN 1 ELSE 0 END), 0), 1
    ) AS large_deal_win_rate,
    ROUND(
        SUM(CASE WHEN Deal_Status = 'Won' AND Deal_Size >= 100000 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') AND Deal_Size >= 100000 THEN 1 ELSE 0 END), 0), 1
    ) AS enterprise_deal_win_rate,

    -- Industry strengths
    GROUP_CONCAT(DISTINCT
        CASE WHEN Deal_Status = 'Won' THEN Industry END
        ORDER BY Industry SEPARATOR ', '
    ) AS industries_won,

    -- Avg cycle
    AVG(CASE WHEN Deal_Status = 'Won' THEN Sales_Cycle_Days END) AS avg_won_cycle,

    -- Capacity score (lower open deals + higher win rate = more capacity)
    ROUND(
        (1.0 / NULLIF(COUNT(CASE WHEN Deal_Status = 'Open' THEN 1 END), 0)) * 100
        * COALESCE(
            SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 1.0
            / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0)
        , 0), 2
    ) AS routing_score

FROM retail_sales.`sales pipeline funnel nested`
GROUP BY Sales_Rep, Region, Rep_Tenure_Months
ORDER BY routing_score DESC;


-- 8.2 Deal-to-rep matching recommendations
--     For each open unassigned-style deal, find the best-fit rep
WITH rep_profiles AS (
    SELECT
        Sales_Rep,
        Region,
        Rep_Tenure_Months,
        COUNT(CASE WHEN Deal_Status = 'Open' THEN 1 END) AS open_deals,
        ROUND(
            SUM(CASE WHEN Deal_Status = 'Won' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(SUM(CASE WHEN Deal_Status IN ('Won', 'Lost') THEN 1 ELSE 0 END), 0), 1
        ) AS overall_win_rate,
        GROUP_CONCAT(DISTINCT
            CASE WHEN Deal_Status = 'Won' THEN Industry END
            ORDER BY Industry SEPARATOR ','
        ) AS won_industries
    FROM retail_sales.`sales pipeline funnel nested`
    GROUP BY Sales_Rep, Region, Rep_Tenure_Months
),
deal_needs AS (
    SELECT
        Opportunity_ID,
        Opportunity_Name,
        Region,
        Industry,
        Deal_Size,
        CASE
            WHEN Deal_Size < 10000 THEN 'Small'
            WHEN Deal_Size BETWEEN 10000 AND 49999 THEN 'Mid'
            WHEN Deal_Size BETWEEN 50000 AND 99999 THEN 'Large'
            ELSE 'Enterprise'
        END AS size_tier
    FROM retail_sales.`sales pipeline funnel nested`
    WHERE Deal_Status = 'Open'
      AND Stage = 'Prospecting'
)
SELECT
    dn.Opportunity_ID,
    dn.Opportunity_Name,
    dn.Region,
    dn.Industry,
    dn.Deal_Size,
    dn.size_tier,
    rp.Sales_Rep AS recommended_rep,
    rp.Rep_Tenure_Months,
    rp.overall_win_rate,
    rp.open_deals AS current_workload,

    -- Fit reason
    CONCAT_WS('; ',
        CASE WHEN rp.Region = dn.Region THEN 'Same region' END,
        CASE WHEN FIND_IN_SET(dn.Industry, rp.won_industries) > 0 THEN 'Industry experience' END,
        CASE WHEN dn.size_tier = 'Enterprise' AND rp.Rep_Tenure_Months >= 12 THEN 'Senior for enterprise' END,
        CASE WHEN rp.open_deals < 10 THEN 'Has capacity' END
    ) AS fit_reasons

FROM deal_needs dn
JOIN rep_profiles rp ON rp.Region = dn.Region
WHERE (
    -- Enterprise deals go to experienced reps
    (dn.size_tier = 'Enterprise' AND rp.Rep_Tenure_Months >= 12)
    OR
    -- Other deals can go to anyone in region
    (dn.size_tier != 'Enterprise')
)
ORDER BY dn.Opportunity_ID, rp.overall_win_rate DESC, rp.open_deals ASC;
