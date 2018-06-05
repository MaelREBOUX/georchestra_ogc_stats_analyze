-- Insertion des données quotidiennes

--TRUNCATE ogcstatistics.ogc_services_stats_daily ;
INSERT INTO ogcstatistics.ogc_services_stats_daily
(
  SELECT
    1 AS siteid,
    '2018-03-18'::date AS date,
    org, user_name, service, request, layer,
    COUNT(*) AS count,
    EXTRACT(WEEK FROM '2018-03-18'::date)::integer AS week,
    EXTRACT(MONTH FROM '2018-03-18'::date)::integer AS month,
    EXTRACT(YEAR FROM '2018-03-18'::date)::integer AS year,
    CONCAT(EXTRACT(YEAR FROM '2018-03-18'::date), '-', EXTRACT(WEEK FROM '2018-03-18'::date)) AS weekyear,
    CONCAT(EXTRACT(YEAR FROM '2018-03-18'::date), '-', EXTRACT(MONTH FROM '2018-03-18'::date)) AS monthyear
  FROM ogcstatistics.ogc_services_log_y2018m3
  WHERE date > '2018-03-18'::date AND date < '2018-03-19'::date
  GROUP BY org, user_name, service, request, layer, roles
);

