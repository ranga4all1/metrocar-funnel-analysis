/*
Metrocar funnel data extraction for dashboarding and analysis in other tools e. g. Tableau

-- postgres://Test:bQNxVzJL4g6u@ep-noisy-flower-846766-pooler.us-east-2.aws.neon.tech/Metrocar
*/

WITH
-- user related CTE's
  app_download AS (
    SELECT
      COUNT(DISTINCT ad.app_download_key) AS total_users_app_downloaded,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt
    FROM app_downloads AS ad
    LEFT JOIN signups AS s ON ad.app_download_key = s.session_id
    GROUP BY ad.platform, s.age_range, ad.download_ts::DATE
  ),
  sign_ups AS (
    SELECT
      COUNT(DISTINCT s.user_id) AS total_users_signed_up,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt
    FROM signups AS s
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    GROUP BY ad.platform, s.age_range, ad.download_ts::DATE
  ),
  user_ride_status AS (
    SELECT
      rr.user_id,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt,
      MAX(CASE WHEN rr.accept_ts IS NOT NULL THEN 1 ELSE 0 END) AS ride_accepted,
      MAX(CASE WHEN rr.dropoff_ts IS NOT NULL THEN 1 ELSE 0 END) AS ride_completed
    FROM ride_requests AS rr
    LEFT JOIN signups AS s ON rr.user_id = s.user_id
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    GROUP BY rr.user_id, ad.platform, s.age_range, ad.download_ts::DATE
  ),
  payment_status AS (
    SELECT
      rr.user_id,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt,
      COUNT(*) AS total_rides_with_payment
    FROM transactions AS t
    LEFT JOIN ride_requests AS rr ON t.ride_id = rr.ride_id
    LEFT JOIN signups AS s ON rr.user_id = s.user_id
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    WHERE charge_status = 'Approved'
    GROUP BY rr.user_id, ad.platform, s.age_range, ad.download_ts::DATE
  ),
  review_status AS (
    SELECT
      rv.user_id,
      COUNT(*) AS total_reviews_per_user,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt
    FROM reviews AS rv
    LEFT JOIN signups AS s ON rv.user_id = s.user_id
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    GROUP BY rv.user_id, ad.platform, s.age_range, ad.download_ts::DATE
  ),

  -- steps related CTE's
  steps AS (
    SELECT
      1 AS funnel_step,
      'app_download' AS funnel_name,
      total_users_app_downloaded AS user_count,
      platform,
      age_range,
    	download_dt
    FROM app_download
    UNION
    SELECT
      2 AS funnel_step,
      'sign_up' AS funnel_name,
      total_users_signed_up AS user_count,
      platform,
      age_range,
    	download_dt
    FROM sign_ups
    UNION
    SELECT
      3 AS funnel_step,
      'ride_requested' AS funnel_name,
      COUNT(*) AS user_count,
      platform,
      age_range,
    	download_dt
    FROM user_ride_status
    GROUP BY platform, age_range, download_dt
    UNION
    SELECT
      4 AS funnel_step,
      'ride_accepted' AS funnel_name,
      SUM(ride_accepted) AS user_count,
      platform,
      age_range,
    	download_dt
    FROM user_ride_status
    GROUP BY platform, age_range, download_dt
    UNION
    SELECT
      5 AS funnel_step,
      'ride_completed' AS funnel_name,
      SUM(ride_completed) AS user_count,
      platform,
      age_range,
    	download_dt
    FROM user_ride_status
    GROUP BY platform, age_range, download_dt
    UNION
    SELECT
      6 AS funnel_step,
      'payment' AS funnel_name,
      COUNT(*) AS user_count,
      platform,
      age_range,
    	download_dt
    FROM payment_status
    GROUP BY platform, age_range, download_dt
    UNION
    SELECT
      7 AS funnel_step,
      'review' AS funnel_name,
      COUNT(*) AS user_count,
      platform,
      age_range,
    	download_dt
    FROM review_status
    GROUP BY platform, age_range, download_dt
  ),

-- ride related CTE's
  requested_rides AS (
    SELECT COUNT(*) AS total_rides_requested,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt
    FROM ride_requests AS rr
    LEFT JOIN signups AS s ON rr.user_id = s.user_id
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    GROUP BY ad.platform, s.age_range, ad.download_ts::DATE
  ),
  accepted_rides AS (
    SELECT COUNT(*) AS total_rides_accepted,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt
    FROM ride_requests AS rr
    LEFT JOIN signups AS s ON rr.user_id = s.user_id
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    WHERE rr.accept_ts IS NOT NULL
    GROUP BY ad.platform, s.age_range, ad.download_ts::DATE
  ),
  completed_rides AS (
    SELECT COUNT(*) AS total_rides_completed,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt
    FROM ride_requests AS rr
    LEFT JOIN signups AS s ON rr.user_id = s.user_id
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    WHERE rr.dropoff_ts IS NOT NULL
    GROUP BY ad.platform, s.age_range, ad.download_ts::DATE
  ),
  payment_rides AS (
    SELECT COUNT(*) AS total_rides_with_payment,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt
    FROM transactions AS t
    LEFT JOIN ride_requests AS rr ON t.ride_id = rr.ride_id
    LEFT JOIN signups AS s ON rr.user_id = s.user_id
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    WHERE charge_status = 'Approved'
    GROUP BY ad.platform, s.age_range, ad.download_ts::DATE
  ),
  review_rides AS (
    SELECT COUNT(*) AS total_rides_with_review,
      ad.platform,
      COALESCE(s.age_range, 'Not Specified') AS age_range,
    	ad.download_ts::DATE AS download_dt
    FROM reviews AS rv
    LEFT JOIN signups AS s ON rv.user_id = s.user_id
    LEFT JOIN app_downloads AS ad ON s.session_id = ad.app_download_key
    GROUP BY ad.platform, s.age_range, ad.download_ts::DATE
  )

-- Main Query
SELECT
  funnel_step,
  funnel_name,
  platform,
  age_range,
  download_dt,
  user_count,
  CASE
    WHEN funnel_name = 'ride_requested' THEN (SELECT total_rides_requested FROM requested_rides WHERE requested_rides.platform = steps.platform AND requested_rides.age_range = steps.age_range AND requested_rides.download_dt = steps.download_dt)
    WHEN funnel_name = 'ride_accepted' THEN (SELECT total_rides_accepted FROM accepted_rides WHERE accepted_rides.platform = steps.platform AND accepted_rides.age_range = steps.age_range AND accepted_rides.download_dt = steps.download_dt)
    WHEN funnel_name = 'ride_completed' THEN (SELECT total_rides_completed FROM completed_rides WHERE completed_rides.platform = steps.platform AND completed_rides.age_range = steps.age_range AND completed_rides.download_dt = steps.download_dt)
    WHEN funnel_name = 'payment' THEN (SELECT total_rides_with_payment FROM payment_rides WHERE payment_rides.platform = steps.platform AND payment_rides.age_range = steps.age_range AND payment_rides.download_dt = steps.download_dt)
    WHEN funnel_name = 'review' THEN (SELECT total_rides_with_review FROM review_rides WHERE review_rides.platform = steps.platform AND review_rides.age_range = steps.age_range AND review_rides.download_dt = steps.download_dt)
    ELSE NULL
  END AS ride_count
FROM steps
ORDER BY funnel_step, platform, age_range, download_dt ASC
;

-- results in aggregated dataset with 27886 rows
--------------------------------------
--------------------------------------