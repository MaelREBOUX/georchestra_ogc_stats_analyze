-- Insertion des données hebdomadaires

--TRUNCATE ogcstatistics.ogc_services_stats_weekly;
INSERT INTO ogcstatistics.ogc_services_stats_weekly
(
  SELECT
    1 AS siteid,
    org, user_name, service, request, layer,
    SUM(count) AS count,
    week, month, year, weekyear, monthyear
  FROM ogcstatistics.ogc_services_stats_daily
  WHERE weekyear = '2018-11'
  GROUP BY org, user_name, service, request, layer, week, month, year, weekyear, monthyear
  );

