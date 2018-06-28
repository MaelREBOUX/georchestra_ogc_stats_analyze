# coding: utf8
#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      e.rouvin
#
# Created:     05/06/2018
# Copyright:   (c) e.rouvin 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import sys
import argparse
from argparse import RawTextHelpFormatter
from datetime import date, timedelta, datetime
import psycopg2
import re


# la base de données
strConnDB = "host='localhost' dbname='georchestra' user='www-data' password='www-data'"


# les variables globales
siteid = 0
DateArg = ""
DateToTreat = ""
DateToFollow = ""
WeekYear = ""



def DailyUpdate():

  # on va lire la table  ogc_services_log_y[YYY]m[M]  qui correspond à la date demandée
  # on va y sélectionner les enregistrements correspondants
  # avec un regroupement org / username / service / request / layer / role
  # tout en supprimant les enregistrements créés par par des comptes liés au monitoring des services
  # + count pour chaque cas

  # trouver la table à attaquer
  ogc_table = "ogc_services_log_y" + DateToTreat[0:4] + "m" + re.sub('^0+', '', DateToTreat[5:7])
  print ( "la table à traiter est : " + ogc_table)

  # on crée les 2 requêtes SQL à jouer
  SQLinsert = """INSERT INTO ogcstatistics_analyze.ogc_services_stats_daily
  (
    SELECT
      1 AS siteid,
      '""" + DateToTreat + """'::date AS date,
      org, user_name, service, request, layer,
      COUNT(*) AS count,
      EXTRACT(WEEK FROM '""" + DateToTreat + """'::date)::integer AS week,
      EXTRACT(MONTH FROM '""" + DateToTreat + """'::date)::integer AS month,
      EXTRACT(YEAR FROM '""" + DateToTreat + """'::date)::integer AS year,
      CONCAT(EXTRACT(YEAR FROM '""" + DateToTreat + """'::date), '-', EXTRACT(WEEK FROM '""" + DateToTreat + """'::date)) AS weekyear,
      CONCAT(EXTRACT(YEAR FROM '""" + DateToTreat + """'::date), '-', EXTRACT(MONTH FROM '""" + DateToTreat + """'::date)) AS monthyear
    FROM ogcstatistics.""" + ogc_table + """
    WHERE date > '""" + DateToTreat + """'::date AND date < '""" + DateToFollow + """'::date
    AND user_name NOT IN ('acces.sig', 'admsig', 'c2c-monitoring', 'geoserver_privileged_user', 'intranet', 'ldapsig')
    GROUP BY org, user_name, service, request, layer
  );"""
  #print(SQLinsert)

  SQLVerif = """SELECT COUNT(*) AS count
  FROM ogcstatistics_analyze.ogc_services_stats_daily
  WHERE date ='""" + DateToTreat + """'::date"""
  #print(SQLVerif)

  SQLVacuumD = """ VACUUM FULL ogcstatistics_analyze.ogc_services_stats_daily; """

  # connection à la base
  try:
    # connexion à la base, si plante, on sort
    conn = psycopg2.connect(strConnDB)
    cursor = conn.cursor()

  except:
    print( "connexion à la base impossible")

  try:
    # on lance la requêt INSERT
    cursor.execute(SQLinsert)
    conn.commit()

    # puis on lance la requête pour vérifier le nb d'enregistrements créés
    cursor.execute(SQLVerif)
    result = cursor.fetchone()
    NbRecordsInserted = result[0]
    print( "nombre d'enregistrements traités : " + str(NbRecordsInserted) )


    cursor.close()
    conn.close()

  except:
    print( "impossible d'exécuter la requête")



# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def WeeklyUpdate():

  # on va faire une requête sur la table daily dans laquelle on vient d'insérer des lignes
  # avant d"insérer on va supprimer les données de la semaine courante
  # car ce script peut être exécuté chaque jour de la semaine
  # on préfère un delete et insert plutôt que de chercher à incrémenter des compteurs

  # on commence par déterminer la semaine courante depuis la date à traiter                     re.sub('^0+', '', DateToTreat[5:7])
  global WeekYear

  # à simplifier mais fonctionner, peut être en créant des variables year, month, days en int
  WeekYear = date(int(DateToTreat[0:4]) , int(re.sub('^0+', '', DateToTreat[5:7])), int(re.sub('^0+', '', DateToTreat[8:10]))).isocalendar()[1]
  WeekYear = DateToTreat[0:4] + '-' + str(WeekYear)
  print( WeekYear)

  #on vide la table de la semaine courante avant d'insérer des enregistrements

  SQLdeleteW = """DELETE FROM ogcstatistics_analyze.ogc_services_stats_weekly
      WHERE weekyear = '""" + WeekYear +"""'; """

  print(SQLdeleteW)
  # on peut maintenant insérer toute les valeurs correspondant à cette semaine courante
  SQLinsertW = """INSERT INTO ogcstatistics_analyze.ogc_services_stats_weekly
(
  SELECT
    1 AS siteid,
    org, user_name, service, request, layer,
    SUM(count) AS count,
    week, year, weekyear
  FROM ogcstatistics_analyze.ogc_services_stats_daily
  WHERE weekyear = '""" + WeekYear + """'
  GROUP BY org, user_name, service, request, layer, week, year, weekyear
  );"""

  print(SQLinsertW)


   # connection à la base
  try:
    # connexion à la base, si plante, on sort
    conn = psycopg2.connect(strConnDB)
    cursor = conn.cursor()

  except:
    print( "connexion à la base impossible")

  try:
    # on lance la requêt DELETE
    cursor.execute(SQLdeleteW)
    conn.commit()

    # puis on lance la requête qui insère
    cursor.execute(SQLinsertW)
    conn.commit()

    cursor.close()
    conn.close()

  except:
    print( "impossible d'exécuter la requête")


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def MonthlyUpdate():

  # on va faire une requête sur la table daily dans laquelle on vient d'insérer des lignes
  # avant d"insérer on va supprimer les données du mois courante
  # car ce script peut être exécuté chaque jour de la semaine
  # on préfère un delete et insert plutôt que de chercher à incrémenter des compteurs

  # on commence par déterminer le mois courant depuis la date à traiter
  global MonthYear

  # à simplifier mais fonctionner, peut être en créant des variables year, month, days en int
  MonthYear = DateToTreat[0:4] + '-' + re.sub('^0+', '', DateToTreat[5:7])
  print( MonthYear )

  #on vide la table de la semaine courante avant d'insérer des enregistrements

  SQLdeleteM = """DELETE FROM ogcstatistics_analyze.ogc_services_stats_monthly
      WHERE monthyear = '""" + MonthYear +"""'; """

  print(SQLdeleteM)
  # on peut maintenant insérer toute les valeurs correspondant à cette semaine courante
  SQLinsertM = """INSERT INTO ogcstatistics_analyze.ogc_services_stats_monthly
(
  SELECT
    1 AS siteid,
    org, user_name, service, request, layer,
    SUM(count) AS count,
    month, year, monthyear
  FROM ogcstatistics_analyze.ogc_services_stats_daily
  WHERE monthyear = '""" + MonthYear + """'
  GROUP BY org, user_name, service, request, layer, month, year, monthyear
  );"""

  print(SQLinsertM)

   # connection à la base
  try:
    # connexion à la base, si plante, on sort
    conn = psycopg2.connect(strConnDB)
    cursor = conn.cursor()

  except:
    print( "connexion à la base impossible")

  try:
    # on lance la requêt DELETE
    cursor.execute(SQLdeleteM)
    conn.commit()

    # puis on lance la requête qui insère
    cursor.execute(SQLinsertM)
    conn.commit()

    cursor.close()
    conn.close()

  except:
    print( "impossible d'exécuter la requête")

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def Vacuum() :

  #on réalise un vacuum chaque début de mois afin d'optimiser l'espace de stockage
  SQLVacuumW = """ VACUUM FULL ogcstatistics_analyze.ogc_services_stats_weekly; """
  SQLVacuumM = """ VACUUM FULL ogcstatistics_analyze.ogc_services_stats_monthly; """

  if DateToTreat[8:10] == '01' :

    #connexion à la base
    conn = psycopg2.connect(strConnDB)
    cursor = conn.cursor()
    conn.set_session(autocommit=True)

    #lancement du VACUUM
    cursor.execute(SQLVacuumW)
    cursor.execute(SQLVacuumM)

    cursor.close()
    conn.close()

  else :
    print("Le VACUUM est lancé la 1er du mois")


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def main():

  # passage en variables globales
  global siteid
  global DateToTreat
  global DateToFollow
  global DateArg
  global strConnDB

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  parser = argparse.ArgumentParser(description="""

  Ce script blablabla [TODO]

  Si aucune date n'est fournie alors ce sera la date du jour qui sera prise en compte.
  Concrètement : les logs de la veille.

  Exemple : update_ogc_stats.py -site 1 -date 2018-05-28""", formatter_class=RawTextHelpFormatter)

  # identifiant du site à traiter
  # obligatoire
  parser.add_argument("-site", help="""Identifiant du site à traiter (obligatoire).""")

  # date
  # Par défaut= date du jour -1 si pas spécifié
  # optionnel
  parser.add_argument("-date", help="""Date à traiter au format 'YYYY-MM-DD' (optionnel)""")

  # debug
  #print( 'Number of arguments:', len(sys.argv), 'arguments.' )
  #print( 'Argument List:', str(sys.argv) )

  # test surt le nb d'arguments passés. On attend 3 au minimum
  if (len(sys.argv) < 3) :
    print("\n Erreur : pas assez d'arguments\n")
    parser.print_help()
    sys.exit()

  # on récupère les arguments passés
  args = parser.parse_args(sys.argv[1:])

  # on fait des tests
  # sur le site
  if sys.argv[1] != "-site" :
    print("\n Erreur : identifiant de site obligatoire\n")
    parser.print_help()
    sys.exit()
  else:
    # on mémorise le siteid
    siteid = str(sys.argv[2])
    # tester la valeur passée
    #print( "TODO : tester la valeur site" )

  # la date si fournie
  # test si un argument date a été fourni
  if (len(sys.argv) == 5) :
    # on a quelquechose
    DateArg = str(sys.argv[4])
    # on force pur le moment, sans contrôler
    DateToTreat = DateArg

    #print(Datearg)

    # on teste le format de date rentrée
    #if str(sys.argv[4]) == DateArg.strftime('%Y-%m-%d') :
    #    print("format de date ok")
    #    DateToTreat = str(sys.argv[4])
    #else :
    #    print("Mauvais format de date")
    #    sys.exit()

  else:
    # pas de date : donc on prend la date du jour courant par défaut
    yesterday = date.today() - timedelta(1)
    DateToTreat = yesterday.strftime('%Y-%m-%d')


  # on déduit la date du jour suivant
  ConvDateToTreat = datetime.strptime(DateToTreat, '%Y-%m-%d')
  nextday = ConvDateToTreat + timedelta(1)
  DateToFollow = nextday.strftime('%Y-%m-%d')


  # tout est OK : on peut traiter les logs

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  # debug
  #siteid = 1
  #DateToTreat = 2018-05-03


  # for debug
  print('\n')
  print( "date to query : " + DateToTreat)
  print( "date wich follow : " + DateToFollow )

  # et on lance le traitement des logs pour le jour demandé
  DailyUpdate()
  WeeklyUpdate()
  MonthlyUpdate()
  Vacuum()

  print( "")
  print( "  F I N")

  pass

if __name__ == '__main__':
    main()
