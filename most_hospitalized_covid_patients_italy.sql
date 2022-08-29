SELECT * 
FROM `bigquery-public-data.covid19_italy.national_trends` 
ORDER BY total_hospitalized_patients DESC
LIMIT 100
