# georchestra_ogc_stats_analyze

## Description

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


## Installation

Cloner ce dépôt.

Création de la session virtuelle : ouvrir une console git bash dans le répertoire.

On crée la session virtuelle (1 seule fois).

```bash
python -m venv venv
source venv/Scripts/Activate

which python
python --version
```

Puis on installe les modules Python

```bash
python -m pip --trusted-host pypi.org install modules/*.whl
```

Vérification :

```bash
python -m pip --trusted-host pypi.org list
Package      Version
------------ -------
pip        22.2.2
psycopg2   2.9.2
setuptools 58.1.0
```


Ne pas oublier de sortir de la session virtuelle Python :

```bash
deactivate
```


## Utilisation

### mode live

Pour connaître l'utilisation de la plate-forme du jour courant : ``python ./update_ogc_stats.py -site 1 -live``

### mode standard

Pour traiter les données des tables ogc_services_log pour une date.

Pour la date courante -> les données de la veille : ``python ./update_ogc_stats.py -site 1``

Pour une date précise : ``python ./update_ogc_stats.py -site 1 -date 2018-05-28``



## Cron


    # traitement des stats OGC georchestra chaque matin à 7h00 = les stats de la veille
	0 9 * * * python3 /data/scripts/georchestra_ogc_stats_analyze/update_ogc_stats.py -site 1 > /data/scripts/georchestra_ogc_stats_analyze/upd$
	
	# traitement des stats OGC georchestra mode live  toutes les 15 min // entre 07h et 19h // du lundi au vendredi
	*/15 7-19 * * MON-FRI python3 /data/scripts/georchestra_ogc_stats_analyze/update_ogc_stats.py -site 1 -live > /data/scripts/georchestra_ogc$

