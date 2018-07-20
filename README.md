# georchestra_ogc_stats_analyze

Cet outil permet de créer des statistiques sur les services OGC de geOrchestra.

Le script traite les données de la table ogc_services_log du mois courant afin de consolider des statistiques selon 3 plages de temps :

- stats au jour
- stats à la semaine
- stats au mois

Si aucune date n'est fournie alors le script traitera les logs de la veille. Usage typique : lancé par un cron pour traiter les stats de la veille. Le mode 'live' permet de connaître l'utilisation des services sur le jour courant. A exécuter toutes les n minutes.

Il est destinée à être lancé chaque nuit pour traiter les statistiques de la veille ou toutes les n minutes pour le mode "live".

Les temps d'exécution dépendent de la volumétrie des tables à lire.


## Prérequis

- une instance georchestra
- un accès en lecture à la base de données PostgreSQL "georchestra"
- une base de données PostgreSQL en écriture
- Python > 3.4
- modules python : psycopg2 (2.7.4)


## Installation sur Debian 9

TODO

## Installation sur Debian 8

Testé sur Debian 8.11


    # pyhton 3.4
	sudo apt-get install python3
	python3 --version
	
	# pip
	sudo apt-get install python3-pip
	sudo python3 -m pip --version
	pip 1.5.6 from /usr/lib/python3/dist-packages (python 3.4)

	# psycopg2
	sudo apt-get install python3-psycopg2


## Cron


    # traitement des stats OGC georchestra chaque matin à 7h00 = les stats de la veille
	0 9 * * * python3 /data/scripts/georchestra_ogc_stats_analyze/update_ogc_stats.py -site 1 > /data/scripts/georchestra_ogc_stats_analyze/upd$
	
	# traitement des stats OGC georchestra mode live  toutes les 15 min // entre 07h et 19h // du lundi au vendredi
	*/15 7-19 * * MON-FRI python3 /data/scripts/georchestra_ogc_stats_analyze/update_ogc_stats.py -site 1 -live > /data/scripts/georchestra_ogc$

