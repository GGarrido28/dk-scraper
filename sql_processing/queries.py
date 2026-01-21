def get_query(sport="MLB", query_type="all", query_date=None):
    if query_type == "all":
        return f"""
                SELECT 
                    t1.draft_group_id
                FROM (
                    SELECT 
                        dg.draft_group_id, 
                        count(c.contest_id) AS total_contests,
                        sum(CASE WHEN c.is_downloaded = TRUE THEN 1 ELSE 0 END) AS downloaded_contests
                    FROM draft_groups dg
                    INNER JOIN contests c ON dg.draft_group_id = c.draft_group_id
                    WHERE 
                        UPPER(dg.sport) = '{sport.upper()}'
                        AND CAST(start_date_est as timestamp) < CAST(current_timestamp AS timestamp)
                        AND COALESCE(dg.is_etl, FALSE) = FALSE
                        AND COALESCE (is_ignored,FALSE) = FALSE
                        AND COALESCE(is_cancelled, FALSE) = FALSE
                    GROUP BY dg.draft_group_id
                    ) AS t1 
                WHERE 
                    t1.total_contests = t1.downloaded_contests
            """
    else:
        return f"""
                SELECT 
                    t1.draft_group_id
                FROM (
                    SELECT 
                        dg.draft_group_id, 
                        count(c.contest_id) AS total_contests,
                        sum(CASE WHEN c.is_downloaded = TRUE THEN 1 ELSE 0 END) AS downloaded_contests
                    FROM draft_groups dg
                    INNER JOIN contests c ON dg.draft_group_id = c.draft_group_id
                    WHERE 
                        UPPER(dg.sport) = '{sport.upper()}'
                        AND CAST(start_date_est as timestamp) < CAST(current_timestamp AS timestamp)
                        AND COALESCE(dg.is_etl, FALSE) = FALSE
                        AND COALESCE (is_ignored,FALSE) = FALSE
                        AND COALESCE(is_cancelled, FALSE) = FALSE
                        AND CAST(dg.start_date_est as date) = '{query_date}'
                    GROUP BY dg.draft_group_id
                    ) AS t1 
                WHERE 
                    t1.total_contests = t1.downloaded_contests
            """
