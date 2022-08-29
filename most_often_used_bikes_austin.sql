SELECT bikeid,
  count(bikeid) AS trips,
  sum(duration_minutes) AS time_used_minutes,
  avg(duration_minutes) AS avg_use_minutes
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` 
GROUP BY bikeid
ORDER BY trips DESC
LIMIT 100
