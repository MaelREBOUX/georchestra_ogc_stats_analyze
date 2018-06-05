-- Insertion des données mensuelles

--TRUNCATE ogcstatistics.ogc_services_stats_monthly;
INSERT INTO ogcstatistics.ogc_services_stats_monthly
(
  SELECT
    1 AS siteid,
    org, user_name, service, request, layer,
    SUM(count) AS count,
    month, year, monthyear
  FROM ogcstatistics.ogc_services_stats_daily
  WHERE monthyear = '2018-3'
  GROUP BY org, user_name, service, request, layer, month, year, monthyear
  );