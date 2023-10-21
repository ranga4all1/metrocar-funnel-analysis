/*
MetroCar - Funnel Analysis

-- postgres://Test:bQNxVzJL4g6u@ep-noisy-flower-846766-pooler.us-east-2.aws.neon.tech/Metrocar

OUTLINE:

1) User Funnel using 'Percent of Previous' metric
2) User Funnel using 'Percent of Top' metric
3) Rides Funnel using 'Percent of Previous' metric
4) Rides Funnel using 'Percent of Top' metric
5) segment contribution

*/

-- 1) User Funnel using 'Percent of Previous' metric

-- app_download : (DEFINES THE GROUP WE FOLLOW THROUGH THE FUNNEL)
WITH app_download AS (
    SELECT
        COUNT(DISTINCT app_download_key) AS total_users_app_downloaded
    FROM app_downloads
    ),

-- sign_ups (FROM THE app_download ABOVE)
    sign_ups AS (
        SELECT
            COUNT(DISTINCT user_id) total_users_signed_up
    FROM signups
    ),

-- user_ride_status (FROM THE sign_ups ABOVE)
    user_ride_status AS (
        SELECT
            user_id,
            MAX(
                CASE
                    WHEN accept_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_accepted,
            MAX(
                CASE
                    WHEN dropoff_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_completed
        FROM ride_requests
        GROUP BY user_id
    ),

-- payment_status
    payment_status AS (
        SELECT
            r.user_id,
            COUNT(*) AS total_rides_with_payment
        FROM transactions AS t
        LEFT JOIN ride_requests AS r
        ON t.ride_id = r.ride_id
      	WHERE charge_status = 'Approved'
        GROUP BY r.user_id
    ),

-- review_status
    review_status AS (
        SELECT
            user_id,
            COUNT(*) AS total_reviews_per_user
        FROM reviews
        GROUP BY user_id
    ),

-- steps
    steps AS (
        SELECT
            1 AS funnel_step,
            'app_download' AS funnel_name,
            total_users_app_downloaded AS user_count
        FROM app_download
        UNION
        SELECT
            2 AS funnel_step,
            'sign_up' AS funnel_name,
            total_users_signed_up AS user_count
        FROM sign_ups
        UNION
        SELECT
            3 AS funnel_step,
            'ride_requested' AS funnel_name,
            COUNT(*) AS user_count   --total_users_ride_requested
        FROM user_ride_status
        UNION
        SELECT
            4 AS funnel_step,
            'ride_accepted' AS funnel_name,
            SUM(ride_accepted) AS user_count   --total_users_ride_accepted
        FROM user_ride_status
        UNION
        SELECT
            5 AS funnel_step,
            'ride_completed' AS funnel_name,
            SUM(ride_completed) AS user_count     --total_users_ride_completed
        FROM user_ride_status
        UNION
        SELECT
            6 AS funnel_step,
            'payment' AS funnel_name,
            COUNT(*) AS user_count
        FROM payment_status
      	UNION
        SELECT
            7 AS funnel_step,
      			'review' AS funnel_name,
            COUNT(*) AS user_count
        FROM review_status
        )


SELECT
    funnel_step,
    funnel_name,
    user_count,
    lag(user_count, 1) OVER (ORDER BY funnel_step),
    (lag(user_count, 1) OVER (ORDER BY funnel_step)) - user_count AS diff,
    ROUND(user_count::numeric / lag(user_count, 1) OVER (ORDER BY funnel_step), 4) AS conversion_rate,
    ROUND((1.0 - user_count::numeric / lag(user_count, 1) OVER (ORDER BY funnel_step)), 4) AS dropoff_percent
FROM steps
ORDER BY funnel_step ASC
;
/*
| funnel_step | funnel_name    | user_count | lag   | diff | conversion_rate | dropoff_percent |
| ----------- | -------------- | ---------- | ----- | ---- | --------------- | --------------- |
| 1           | app_download   | 23608      |       |      |                 |                 |
| 2           | sign_up        | 17623      | 23608 | 5985 | 0.7465          | 0.2535          |
| 3           | ride_requested | 12406      | 17623 | 5217 | 0.7040          | 0.2960          |
| 4           | ride_accepted  | 12278      | 12406 | 128  | 0.9897          | 0.0103          |
| 5           | ride_completed | 6233       | 12278 | 6045 | 0.5077          | 0.4923          |
| 6           | payment        | 6233       | 6233  | 0    | 1.0000          | 0.0000          |
| 7           | review         | 4348       | 6233  | 1885 | 0.6976          | 0.3024          |
*/


--------------------------------
--------------------------------

-- 2) User Funnel using 'Percent of Top' metric

-- app_download : (DEFINES THE GROUP WE FOLLOW THROUGH THE FUNNEL)
WITH app_download AS (
    SELECT
        COUNT(DISTINCT app_download_key) AS total_users_app_downloaded
    FROM app_downloads
    ),

-- sign_ups (FROM THE app_download ABOVE)
    sign_ups AS (
        SELECT
            COUNT(DISTINCT user_id) total_users_signed_up
    FROM signups
    ),

-- user_ride_status (FROM THE sign_ups ABOVE)
    user_ride_status AS (
        SELECT
            user_id,
            MAX(
                CASE
                    WHEN accept_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_accepted,
            MAX(
                CASE
                    WHEN dropoff_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_completed
        FROM ride_requests
        GROUP BY user_id
    ),

-- payment_status
    payment_status AS (
        SELECT
            r.user_id,
            COUNT(*) AS total_rides_with_payment
        FROM transactions AS t
        LEFT JOIN ride_requests AS r
        ON t.ride_id = r.ride_id
      	WHERE charge_status = 'Approved'
        GROUP BY r.user_id
    ),

-- review_status
    review_status AS (
        SELECT
            user_id,
            COUNT(*) AS total_reviews_per_user
        FROM reviews
        GROUP BY user_id
    ),

-- steps
    steps AS (
        SELECT
            1 AS funnel_step,
            'app_download' AS funnel_name,
            total_users_app_downloaded AS user_count
        FROM app_download
        UNION
        SELECT
            2 AS funnel_step,
            'sign_up' AS funnel_name,
            total_users_signed_up AS user_count
        FROM sign_ups
        UNION
        SELECT
            3 AS funnel_step,
            'ride_requested' AS funnel_name,
            COUNT(*) AS user_count   --total_users_ride_requested
        FROM user_ride_status
        UNION
        SELECT
            4 AS funnel_step,
            'ride_accepted' AS funnel_name,
            SUM(ride_accepted) AS user_count   --total_users_ride_accepted
        FROM user_ride_status
        UNION
        SELECT
            5 AS funnel_step,
            'ride_completed' AS funnel_name,
            SUM(ride_completed) AS user_count     --total_users_ride_completed
        FROM user_ride_status
        UNION
        SELECT
            6 AS funnel_step,
            'payment' AS funnel_name,
            COUNT(*) AS user_count
        FROM payment_status
      	UNION
        SELECT
            7 AS funnel_step,
      			'review' AS funnel_name,
            COUNT(*) AS user_count
        FROM review_status
        )

SELECT
    funnel_step,
    funnel_name,
    user_count,
    FIRST_VALUE(user_count) OVER (ORDER BY funnel_step) AS first_value,
    (FIRST_VALUE(user_count) OVER (ORDER BY funnel_step)) - user_count AS diff,
    ROUND(user_count::numeric / FIRST_VALUE(user_count) OVER (ORDER BY funnel_step), 4) AS conversion_rate,
    ROUND((1.0 - user_count::numeric / FIRST_VALUE(user_count) OVER (ORDER BY funnel_step)), 4) AS dropoff_percent
FROM steps
;
/*
| funnel_step | funnel_name    | user_count | first_value | diff  | conversion_rate | dropoff_percent |
| ----------- | -------------- | ---------- | ----------- | ----- | --------------- | --------------- |
| 1           | app_download   | 23608      | 23608       | 0     | 1.0000          | 0.0000          |
| 2           | sign_up        | 17623      | 23608       | 5985  | 0.7465          | 0.2535          |
| 3           | ride_requested | 12406      | 23608       | 11202 | 0.5255          | 0.4745          |
| 4           | ride_accepted  | 12278      | 23608       | 11330 | 0.5201          | 0.4799          |
| 5           | ride_completed | 6233       | 23608       | 17375 | 0.2640          | 0.7360          |
| 6           | payment        | 6233       | 23608       | 17375 | 0.2640          | 0.7360          |
| 7           | review         | 4348       | 23608       | 19260 | 0.1842          | 0.8158          |
*/


--------------------------------
--------------------------------

-- 3) Rides Funnel using 'Percent of Previous' metric

-- ride_status
WITH user_ride_status AS (
        SELECT
            ride_id,
            MAX(
                CASE
                    WHEN accept_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_accepted,
            MAX(
                CASE
                    WHEN dropoff_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_completed
        FROM ride_requests
        GROUP BY ride_id
    ),

-- payment_status
    payment_status AS (
        SELECT
            r.ride_id,
            COUNT(*) AS total_rides_with_payment
        FROM transactions AS t
        LEFT JOIN ride_requests AS r
        ON t.ride_id = r.ride_id
      	WHERE charge_status = 'Approved'
        GROUP BY r.ride_id
    ),

-- review_status
    review_status AS (
        SELECT
            ride_id,
            COUNT(*) AS total_reviews_per_ride
        FROM reviews
        GROUP BY ride_id
    ),

-- steps
    steps AS (
        SELECT
            1 AS funnel_step,
            'app_download' AS funnel_name,
            0 AS ride_count
        UNION
        SELECT
            2 AS funnel_step,
            'sign_up' AS funnel_name,
            0 AS ride_count
        UNION
        SELECT
            3 AS funnel_step,
            'ride_requested' AS funnel_name,
            COUNT(*) AS ride_count   --total_users_ride_requested
        FROM user_ride_status
        UNION
        SELECT
            4 AS funnel_step,
            'ride_accepted' AS funnel_name,
            SUM(ride_accepted) AS ride_count   --total_users_ride_accepted
        FROM user_ride_status
        UNION
        SELECT
            5 AS funnel_step,
            'ride_completed' AS funnel_name,
            SUM(ride_completed) AS ride_count     --total_users_ride_completed
        FROM user_ride_status
        UNION
        SELECT
            6 AS funnel_step,
            'payment' AS funnel_name,
            COUNT(*) AS ride_count
        FROM payment_status
      	UNION
        SELECT
            7 AS funnel_step,
      			'review' AS funnel_name,
            COUNT(*) AS ride_count
        FROM review_status
        )


SELECT
    funnel_step,
    funnel_name,
    ride_count,
    lag(ride_count, 1) OVER (ORDER BY funnel_step),
    (lag(ride_count, 1) OVER (ORDER BY funnel_step)) - ride_count AS diff,
    ROUND(ride_count::numeric / lag(ride_count, 1) OVER (ORDER BY funnel_step), 4) AS conversion_rate,
    ROUND((1.0 - ride_count::numeric / lag(ride_count, 1) OVER (ORDER BY funnel_step)), 4) AS dropoff_percent
FROM steps
WHERE ride_count > 0
ORDER BY funnel_step ASC
;
/*
| funnel_step | funnel_name    | ride_count | lag    | diff   | conversion_rate | dropoff_percent |
| ----------- | -------------- | ---------- | ------ | ------ | --------------- | --------------- |
| 3           | ride_requested | 385477     |        |        |                 |                 |
| 4           | ride_accepted  | 248379     | 385477 | 137098 | 0.6443          | 0.3557          |
| 5           | ride_completed | 223652     | 248379 | 24727  | 0.9004          | 0.0996          |
| 6           | payment        | 212628     | 223652 | 11024  | 0.9507          | 0.0493          |
| 7           | review         | 156211     | 212628 | 56417  | 0.7347          | 0.2653          |
*/

-----------------------------------
-----------------------------------

-- 4) Rides Funnel using 'Percent of Top' metric

-- ride_status
WITH user_ride_status AS (
        SELECT
            ride_id,
            MAX(
                CASE
                    WHEN accept_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_accepted,
            MAX(
                CASE
                    WHEN dropoff_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_completed
        FROM ride_requests
        GROUP BY ride_id
    ),

-- payment_status
    payment_status AS (
        SELECT
            r.ride_id,
            COUNT(*) AS total_rides_with_payment
        FROM transactions AS t
        LEFT JOIN ride_requests AS r
        ON t.ride_id = r.ride_id
      	WHERE charge_status = 'Approved'
        GROUP BY r.ride_id
    ),

-- review_status
    review_status AS (
        SELECT
            ride_id,
            COUNT(*) AS total_reviews_per_ride
        FROM reviews
        GROUP BY ride_id
    ),

-- steps
    steps AS (
        SELECT
            1 AS funnel_step,
            'app_download' AS funnel_name,
            0 AS ride_count
        UNION
        SELECT
            2 AS funnel_step,
            'sign_up' AS funnel_name,
            0 AS ride_count
        UNION
        SELECT
            3 AS funnel_step,
            'ride_requested' AS funnel_name,
            COUNT(*) AS ride_count   --total_users_ride_requested
        FROM user_ride_status
        UNION
        SELECT
            4 AS funnel_step,
            'ride_accepted' AS funnel_name,
            SUM(ride_accepted) AS ride_count   --total_users_ride_accepted
        FROM user_ride_status
        UNION
        SELECT
            5 AS funnel_step,
            'ride_completed' AS funnel_name,
            SUM(ride_completed) AS ride_count     --total_users_ride_completed
        FROM user_ride_status
        UNION
        SELECT
            6 AS funnel_step,
            'payment' AS funnel_name,
            COUNT(*) AS ride_count
        FROM payment_status
      	UNION
        SELECT
            7 AS funnel_step,
      			'review' AS funnel_name,
            COUNT(*) AS ride_count
        FROM review_status
        )


SELECT
    funnel_step,
    funnel_name,
    ride_count,
    FIRST_VALUE(ride_count) OVER (ORDER BY funnel_step) AS first_value,
    (FIRST_VALUE(ride_count) OVER (ORDER BY funnel_step)) - ride_count AS diff,
    ROUND(ride_count::numeric / FIRST_VALUE(ride_count) OVER (ORDER BY funnel_step), 4) AS conversion_rate,
    ROUND((1.0 - ride_count::numeric / FIRST_VALUE(ride_count) OVER (ORDER BY funnel_step)), 4) AS dropoff_percent
FROM steps
WHERE ride_count > 0
ORDER BY funnel_step ASC
;
/*
| funnel_step | funnel_name    | ride_count | first_value | diff   | conversion_rate | dropoff_percent |
| ----------- | -------------- | ---------- | ----------- | ------ | --------------- | --------------- |
| 3           | ride_requested | 385477     | 385477      | 0      | 1.0000          | 0.0000          |
| 4           | ride_accepted  | 248379     | 385477      | 137098 | 0.6443          | 0.3557          |
| 5           | ride_completed | 223652     | 385477      | 161825 | 0.5802          | 0.4198          |
| 6           | payment        | 212628     | 385477      | 172849 | 0.5516          | 0.4484          |
| 7           | review         | 156211     | 385477      | 229266 | 0.4052          | 0.5948          |
*/

--------------------------------
--------------------------------


-- 5) segment contribution

-- platform
SELECT
    platform,
    COUNT(*) AS downloads,
    SUM(COUNT(*)) OVER () AS total_downloads,
    ROUND(COUNT(*)::numeric /
        SUM(COUNT(*)) OVER (), 4) AS pct_of_downloads
FROM app_downloads
GROUP BY platform;
/*
| platform | downloads | total_downloads | pct_of_downloads |
| -------- | --------- | --------------- | ---------------- |
| ios      | 14290     | 23608           | 0.6053           |
| web      | 2383      | 23608           | 0.1009           |
| android  | 6935      | 23608           | 0.2938           |
*/

-- age_range
SELECT
    age_range,
    COUNT(*) AS signups,
    SUM(COUNT(*)) OVER () AS total_signups,
    ROUND(COUNT(*)::numeric /
        SUM(COUNT(*)) OVER (), 4) AS pct_of_signups
FROM signups
GROUP BY age_range
ORDER BY age_range;
/*
| age_range | signups | total_signups | pct_of_signups |
| --------- | ------- | ------------- | -------------- |
| 18-24     | 1865    | 17623         | 0.1058         |
| 25-34     | 3447    | 17623         | 0.1956         |
| 35-44     | 5181    | 17623         | 0.2940         |
| 45-54     | 1826    | 17623         | 0.1036         |
| Unknown   | 5304    | 17623         | 0.3010         |
*/

---------------------------------
---------------------------------