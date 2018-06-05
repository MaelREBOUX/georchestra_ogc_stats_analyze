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





# les variables globales
siteid = 0
DateToTreat = ""



def DailyUpdate():

  # on va lire la table  ogc_services_log_y[YYY]m[M]  qui correspond à la date demandée
  # on va y sélectionner les enregistrements correspondants
  # avec un regroupement org / username / service / request / layer / role
  # + count pour chaque cas

  SQLinsert = """INSERT INTO ogcstatistics.ogc_services_stats_daily
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
    FROM ogcstatistics.ogc_services_log_y2018m3
    WHERE date > '""" + DateToTreat + """'::date AND date < '""" + DateToTreat + """'::date
    GROUP BY org, user_name, service, request, layer, roles
  );"""

  print(SQLinsert)


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

  # debug
  siteid = 1

  # la date
  # si rien => date du jour -1 = hier
  yesterday = date.today() - timedelta(1)
  DateToTreat =  yesterday.strftime('%Y-%m-%d')

  # sinon : prendre la date passée et vérifier la syntaxe

  # for debug
  print( "date to query : " + DateToTreat)

  DailyUpdate()



  pass

if __name__ == '__main__':
    main()
