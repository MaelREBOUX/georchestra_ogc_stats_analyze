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
DateToTreat = ""
DateToFollow = ""
Datearg = ""



def DailyUpdate():

  # on va lire la table  ogc_services_log_y[YYY]m[M]  qui correspond à la date demandée
  # on va y sélectionner les enregistrements correspondants
  # avec un regroupement org / username / service / request / layer / role
  # tout en supprimant les enregistrements créés par par des comptes liés au monitoring des services
  # + count pour chaque cas

  # trouver la table à attaquer
  ogc_table = "ogc_services_log_y" + DateToTreat[0:4] + "m" + re.sub('^0+', '', DateToTreat[5:7])
  print (ogc_table)

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
    GROUP BY org, user_name, service, request, layer, roles
  );"""

  print(SQLinsert)

  SQLVerif = """SELECT COUNT(*) AS count
  FROM ogcstatistics_analyze.ogc_services_stats_daily
  WHERE date ='""" + DateToTreat + """'::date"""

  print(SQLVerif)

  # connection à la base
  try:
    # connexion à la base, si plante, on sort
    conn = psycopg2.connect(strConnDB)
    cursor = conn.cursor()

  except:
    print( "connexion impossible")

  try:

    # on lance la requêt INSERT
    cursor.execute(SQLinsert)
    conn.commit()

    # puis on lance la requête pour vérifier le nb d'enregistrements créé
    cursor.execute(SQLVerif)
    result = cursor.fetchone()
    NbRecordsInserted = result[0]
    print( str(NbRecordsInserted) )

    cursor.close()
    conn.close()

    # contrôle ?

  except:
    print( "impossible d'exécuter la requête")

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def main():

  # passage en variables globales
  global siteid
  global DateToTreat
  global DateToFollow
  global Datearg
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
    print( "TODO : tester la valeur site" )

  # la date si fournie
  # test si un argument date a été fourni
  if (len(sys.argv) == 5) :
    Datearg = str(sys.argv[4])
    print(Datearg)
    sys.exit()
  else:
    print("Non")

  sys.exit()

    # on teste le format de date rentrée
  if str(sys.argv[4]) == Datearg.strftime('%Y-%m-%d') :
      print("format de date ok")
      DateToTreat = str(sys.argv[4])

  else :
      print("Mauvais format de date")
      sys.exit()

  # on mémorise la date
  DateToTreat = str(sys.argv[4])
    # tester la valeur passée
  print( "TODO : tester la valeur date" )


  sys.exit()

    # on mémorise le siteid
    #dateArg = str(sys.argv[4])
    # tester la valeur passée
    #print( "TODO : tester la valeur date" )

  #else:
    #print("Date J-1")





  # tout est OK : on peut traiter les logs

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  # debug
  #siteid = 1

  # la date
  # si rien => date du jour -1 = hier
  yesterday = date.today() - timedelta(1)
  DateToTreat = yesterday.strftime('%Y-%m-%d')

  # for debug
  #DateToTreat = "2018-03-05"

  ConvDateToTreat = datetime.strptime(DateToTreat, '%Y-%m-%d')
  nextday = ConvDateToTreat + timedelta(1)
  DateToFollow = nextday.strftime('%Y-%m-%d')

  #tomorrow =  date.today()
  #DateToFollow = tomorrow.strftime('%Y-%m-%d')

  # sinon : prendre la date passée et vérifier la syntaxe
  # TODO



  # for debug
  print( "date to query : " + DateToTreat)
  print( "date wich follow : " + DateToFollow )

  #DailyUpdate()



  pass

if __name__ == '__main__':
    main()
