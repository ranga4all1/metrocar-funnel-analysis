/*
MetroCar - Funnel Analysis - Data profiling/overview

PART 1: Understanding database/data profiling

-- postgres://Test:bQNxVzJL4g6u@ep-noisy-flower-846766-pooler.us-east-2.aws.neon.tech/Metrocar

*/

-- exploring database to find the data you need

-- What columns are in database?
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = 'public';

--------------------------------
--------------------------------

-- Basic data profiling
SELECT COUNT(*)
FROM app_downloads;

SELECT COUNT(*)
FROM signups;

SELECT COUNT(*)
FROM ride_requests;

SELECT COUNT(*)
FROM transactions;

SELECT COUNT(*)
FROM reviews;

----

SELECT *
FROM app_downloads
LIMIT 10;

SELECT *
FROM signups
LIMIT 10;

SELECT *
FROM ride_requests
LIMIT 10;

SELECT *
FROM transactions
LIMIT 10;

SELECT *
FROM reviews
LIMIT 10;

----

--  date range for ride_requests
SELECT
	MIN(DATE_TRUNC('day', dropoff_ts)),
	MAX(DATE_TRUNC('day', dropoff_ts))
from ride_requests
;
/*
| min                 | max                 |
| ------------------- | ------------------- |
| 2021-01-05 00:00:00 | 2022-04-24 00:00:00 |
*/
--------------------------------
--------------------------------

-- Q1: How many times was the app downloaded?
SELECT
	COUNT(*) AS total_users_app_downloaded,
    COUNT(DISTINCT app_download_key) AS total_users_distinct
FROM app_downloads;
/*
| total_users_app_downloaded | total_users_distinct |
| -------------------------- | -------------------- |
| 23608                      | 23608                |
*/

----

-- Q2: How many users signed up on the app?
SELECT
    COUNT(DISTINCT user_id) total_users_distinct,
    COUNT(user_id) total_users
FROM signups;
/*
| total_users_distinct | total_users |
| -------------------- | ----------- |
| 17623                | 17623       |
*/

----

-- Q3: How many rides were requested through the app?
SELECT
    COUNT(*) AS total_rides_requested,
    COUNT(DISTINCT ride_id) AS total_rides_distinct
FROM ride_requests;
/*
| total_rides_requested | total_rides_distinct |
| --------------------- | -------------------- |
| 385477                | 385477               |
*/

----

-- Q4: How many rides were requested and completed through the app?
WITH requested_rides AS (
  	SELECT COUNT(*) AS total_rides_requested
		FROM ride_requests
    ),

    accepted_rides AS (
    SELECT COUNT(*) AS total_rides_accepted
		FROM ride_requests
		WHERE
    accept_ts IS NOT NULL
    ),

    completed_rides AS (
  	SELECT COUNT(*) AS total_rides_completed
		FROM ride_requests
		WHERE
      	dropoff_ts IS NOT NULL
  			-- AND (pickup_ts, dropoff_ts, request_ts, accept_ts) IS NOT NULL
		)

SELECT
		total_rides_requested,
    total_rides_accepted,
    total_rides_completed
FROM requested_rides, accepted_rides, completed_rides
;
/*
| total_rides_requested | total_rides_accepted | total_rides_completed |
| --------------------- | -------------------- | --------------------- |
| 385477                | 248379               | 223652                |
*/

-- alternate way to get only total_rides_completed
SELECT COUNT(*) total_rides_completed
FROM ride_requests
WHERE
	cancel_ts IS NULL
  AND (pickup_ts, dropoff_ts, request_ts, accept_ts) IS NOT NULL;
/*
| total_rides_completed |
| --------------------- |
| 223652                |
*/

----

/*
Q5: How many rides were requested and
how many unique users requested a ride?
*/
SELECT
    COUNT(*) AS total_rides_requested,
    COUNT(DISTINCT user_id) AS total_users_distinct
FROM ride_requests;
/*
| total_rides_requested | total_users_distinct |
| --------------------- | -------------------- |
| 385477                | 12406                |
*/

-- and not cancelled (or ride completed)
SELECT
    COUNT(*) AS total_rides_requested,
    COUNT(DISTINCT user_id) AS total_users_distinct
FROM ride_requests
WHERE
	cancel_ts IS NULL
  AND (dropoff_ts, request_ts, accept_ts) IS NOT NULL;
/*
| total_users_distinct | total_users |
| -------------------- | ----------- |
| 6233                 | 223652      |
*/

----

-- Q6: What is the average time of a ride from pick up to drop off?
SELECT
	AVG(EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts))) AS avg_time_of_ride
FROM ride_requests;
/*
| avg_time_of_ride      |
| --------------------- |
| 3156.7387727362151915 |
This is in seconds. = 52.612312879 min
*/
-- in minutes
SELECT
	AVG(EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts))) / 60 AS avg_time_of_ride_in_minutes
FROM ride_requests;
/*
| avg_time_of_ride_in_minutes |
| --------------------------- |
| 52.6123128789369199         |
*/

----

-- Q7: How many rides were accepted by a driver?
SELECT
	COUNT(*) AS total_rides_accepted
 FROM ride_requests
 WHERE (driver_id, accept_ts) IS NOT NULL;
/*
| total_rides_accepted |
| -------------------- |
| 248379               |
*/

--  and NOT cancelled
SELECT
	COUNT(*) AS total_rides_accepted
 FROM ride_requests
 WHERE (driver_id, accept_ts) IS NOT NULL
 		AND cancel_ts IS NULL;
/*
| total_rides_accepted |
| -------------------- |
| 223652               |
*/

----

-- Q8: How many rides did we successfully collect payments
-- and how much was collected?
SELECT COUNT(*) AS total_rides_with_payment,
		SUM(purchase_amount_usd) AS total_payment_collected
FROM transactions
WHERE charge_status = 'Approved';
/*
| total_rides_with_payment | total_payment_collected |
| ------------------------ | ----------------------- |
| 212628                   | 4251667.610000016       |
*/

----

-- Q9: How many ride requests happened on each platform?
SELECT
	a.platform,
	COUNT(r.ride_id) AS total_ride_requests_per_platform
FROM ride_requests r
LEFT JOIN signups s
ON r.user_id = s.user_id
LEFT JOIN app_downloads AS a
ON s.session_id = a.app_download_key
GROUP BY a.platform
ORDER BY total_ride_requests_per_platform DESC;
/*
| platform | total_ride_requests_per_platform |
| -------- | -------------------------------- |
| ios      | 234693                           |
| android  | 112317                           |
| web      | 38467                            |
*/

----

-- Q10: What is the drop-off from users signing up to users
-- requesting a ride?
WITH user_signups AS (
    SELECT COUNT(s.user_id) AS total_signups
		FROM signups s
		),
    users_with_rides AS (
      SELECT COUNT(DISTINCT r.user_id) AS users_requesting_rides
      FROM ride_requests r
    )
SELECT
	total_signups,
  users_requesting_rides,
  total_signups - users_requesting_rides AS dropoff,
  ROUND((total_signups::numeric - users_requesting_rides::numeric) / total_signups::numeric, 4) AS dropoff_percent
FROM user_signups, users_with_rides;
/*
| total_signups | users_requesting_rides | dropoff | dropoff_percent |
| ------------- | ---------------------- | ------- | --------------- |
| 17623         | 12406                  | 5217    | 0.2960          |
*/
----

--------------------------------
--------------------------------

/*
In a user-level funnel analysis, we would want to know:

1. The number of users that downloaded the app
2. How many users signed up for an account
3. How many users requested a ride
4. How many users completed a ride

Notice that for #3 and and #4 we aren’t concerned with the number of rides
completed per user. We are only concerned whether or not a user
requested a ride (at least one) and completed a ride (at least one).
*/

-- 1. The number of users that downloaded the app
SELECT
	COUNT(*) AS total_users_app_downloaded,
    COUNT(DISTINCT app_download_key) AS total_users_distinct
FROM app_downloads;
/*
| total_users_app_downloaded | total_users_distinct |
| -------------------------- | -------------------- |
| 23608                      | 23608                |
*/

-- 2. How many users signed up for an account
SELECT
    COUNT(DISTINCT user_id) total_users_distinct,
    COUNT(user_id) total_users
FROM signups;
/*
| total_users_distinct | total_users |
| -------------------- | ----------- |
| 17623                | 17623       |
*/

/*
3. How many users requested a ride
4. How many users completed a ride
*/
-- Here is one way to answer 3 and 4:
SELECT
    user_id,
    MAX(
        CASE
            WHEN dropoff_ts IS NOT NULL
            THEN 1
            ELSE 0
        END
    ) AS ride_completed
FROM ride_requests
GROUP BY user_id
LIMIT 5;
/*
| user_id | ride_completed |
| ------- | -------------- |
| 100000  | 0              |
| 100001  | 0              |
| 100002  | 1              |
| 100004  | 1              |
| 100006  | 0              |
*/

/*
Since we are grouping by user_id, we guarantee that user_id will only appear once in this table. For the MAX() function, all that is being stated is that if the user ever completed the last step of the ride (dropoff), let’s call this ride complete. Then aggregate this query as follows:
*/
WITH user_ride_status AS (
    SELECT
        user_id,
        MAX(
            CASE
                WHEN dropoff_ts IS NOT NULL
                THEN 1
                ELSE 0
            END
        ) AS ride_completed
    FROM ride_requests
    GROUP BY user_id
)
SELECT
    COUNT(*) AS total_users_ride_requested,
    SUM(ride_completed) AS total_users_ride_completed
FROM user_ride_status;
/*
| total_users_ride_requested | total_users_ride_completed |
| -------------------------- | -------------------------- |
| 12406                      | 6233                       |
*/

--------------------------------
--------------------------------
