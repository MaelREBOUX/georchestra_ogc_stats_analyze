-- Exemple d'une requête d'exploitation des logs georchestra

SELECT
  org,
  service,
  SUM(count)
FROM ogcstatistics.ogc_services_stats_monthly
WHERE request = 'getmap'
GROUP BY org, service
ORDER BY sum desc