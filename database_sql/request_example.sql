
-- Exemples de requêtes d'exploitation directe des logs georchestra

-- utilisateurs du jour
-- classés par ordre de consommation WMS OU WMTS
SELECT
  org, user_name,
  service,
  COUNT(service) AS hits,
  COUNT(DISTINCT(layer)) AS layers_nb,
  RIGHT(MIN(date)::text,8) AS first_hit,
  RIGHT(MAX(date)::text,8) AS last_hit
FROM ogcstatistics.ogc_services_log_y2018m7
WHERE
  date > CURRENT_DATE
  AND service IN ('WMS', 'WMTS')
  AND user_name NOT IN ('acces.sig', 'admsig', 'c2c-monitoring', 'geoserver_privileged_user', 'intranet', 'ldapsig')
GROUP BY org, user_name, service
ORDER BY hits DESC



-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

-- Exemples de requêtes d'exploitation des stats consolidées

SELECT
  org,
  service,
  SUM(count)
FROM ogcstatistics.ogc_services_stats_monthly
WHERE request = 'getmap'
GROUP BY org, service
ORDER BY sum desc

