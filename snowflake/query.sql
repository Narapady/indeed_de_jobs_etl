USE DATABASE INDEED_DB;
USE SCHEMA INDEED_SCHEMA;
SELECT * 
FROM indeed_de_jobs_us;

-- Data engineering job by states
SELECT state, 
       COUNT(*) de_jobs_by_state
FROM indeed_de_jobs_us
WHERE state <> 'unknown'
GROUP BY state
ORDER BY COUNT(*) DESC;

-- Data engineering job by city
SELECT city, 
       COUNT(*) de_jobs_by_city
FROM indeed_de_jobs_us
WHERE city <> 'unknown'
GROUP BY city
ORDER BY COUNT(*) DESC;

-- Remote Data engineering jobs
SELECT work_type, 
       COUNT(*) de_jobs_by_worktype
FROM indeed_de_jobs_us
GROUP BY work_type
ORDER BY COUNT(*) DESC;

SELECT latitude,
       longitude
FROM indeed_de_jobs_us
UNION
SELECT latitude,
       longitude
FROM indeed_de_jobs_us;

SELECT work_type,
       work_hour,
       count(*) AS work_tye_cnt
FROM indeed_de_jobs_us
GROUP BY work_type, work_hour
ORDER BY count(*) DESC;
























