-- Insertion des données quotidiennes

--TRUNCATE ogcstatistics.ogc_services_stats_daily ;
INSERT INTO ogcstatistics.ogc_services_stats_daily
(
  SELECT
    1 AS siteid,
    '2019-02-05'::date AS date,
    CASE WHEN org = 'Rennes_M_tropole' THEN 'Rennes Métropole'
      WHEN org = 'Ville_de_Rennes' THEN 'Ville de Rennes'
      WHEN org = 'Ville_de_rennes' THEN 'Ville de Rennes'
      ELSE org
    END AS org,
    user_name, service, request, layer,
    COUNT(*) AS count,
    EXTRACT(WEEK FROM '2019-02-05'::date)::integer AS week,
    EXTRACT(MONTH FROM '2019-02-05'::date)::integer AS month,
    EXTRACT(YEAR FROM '2019-02-05'::date)::integer AS year,
    CONCAT(EXTRACT(YEAR FROM '2019-02-05'::date), '-', EXTRACT(WEEK FROM '2019-02-05'::date)) AS weekyear,
    CONCAT(EXTRACT(YEAR FROM '2019-02-05'::date), '-', EXTRACT(MONTH FROM '2019-02-05'::date)) AS monthyear
  FROM ogcstatistics.ogc_services_log_y2019m2
  WHERE date > '2019-02-05'::date AND date < '2019-02-06'::date
  AND user_name NOT IN ('acces.sig', 'admsig', 'c2c-monitoring', 'geoserver_privileged_user', 'intranet', 'ldapsig')
  GROUP BY org, user_name, service, request, layer, roles
);

