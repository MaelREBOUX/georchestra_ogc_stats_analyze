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
from datetime import date, timedelta
import psycopg2
import re


# la base de données
strConnDB = "host='localhost' dbname='georchestra' user='www-data' password='www-data'"


# les variables globales
siteid = 0
DateToTreat = ""
DateToFollow = ""


def DeleteMonitoringRecords():

  # pour la date donnée : va supprimer tous les enregistrements créés par des comptes liés au monitoring des services
  # TODO

  print()


def DailyUpdate():

  # on va lire la table  ogc_services_log_y[YYY]m[M]  qui correspond à la date demandée
  # on va y sélectionner les enregistrements correspondants
  # avec un regroupement org / username / service / request / layer / role
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

  parser = argparse.ArgumentParser(description="""Ce script blablabla [TODO]""", formatter_class=RawTextHelpFormatter)

  # identifiant du site à traiter
  # obligatoire
  #parser.add_argument("site", help="""Identifiant du site à traiter.""")

  # date
  # Par défaut= date du jour -1 si pas spécifié
  # optionnel
  #parser.add_argument("site", help="""Identifiant du site à traiter.""")


  #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  # l'identifiant du site à traiter
  global siteid
  global DateToTreat
  global DateToFollow
  global strConnDB

  # debug
  siteid = 1

  # la date
  # si rien => date du jour -1 = hier
  yesterday = date.today() - timedelta(1)
  DateToTreat = yesterday.strftime('%Y-%m-%d')
  tomorrow =  date.today()
  DateToFollow = tomorrow.strftime('%Y-%m-%d')

  # sinon : prendre la date passée et vérifier la syntaxe
  # TODO

  # for debug
  DateToTreat = "2018-03-05"
  DateToFollow = "2018-03-06"

  # for debug
  print( "date to query : " + DateToTreat)
  print( "date wich follow : " + DateToFollow )

  DailyUpdate()



  pass

if __name__ == '__main__':
    main()
