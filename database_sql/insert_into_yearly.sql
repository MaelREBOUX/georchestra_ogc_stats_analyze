-- Insertion des données annuelles

--TRUNCATE ogcstatistics.ogc_services_stats_yearly;
INSERT INTO ogcstatistics.ogc_services_stats_yearly
(
  SELECT
    1 AS siteid,
    org, user_name, service, request, layer,
    SUM(count) AS count,
    year
  FROM ogcstatistics.ogc_services_stats_monthly
  WHERE year = '2018'
  GROUP BY org, user_name, service, request, layer, year
  );