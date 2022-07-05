# coding: utf8
#-------------------------------------------------------------------------------
#
#  Ce script permet de créer des statistiques sur les services OGC de geOrchestra.
#
#  Le script traite les données de la table ogc_services_log du mois courant afin de consolider des statistiques selon 3 plages de temps :
#    stats au jour
#    stats à la semaine
#    stats au mois
#    + un mode 'live'
#
#
# Auteurs :
#   Maël REBOUX   m.reboux@rennesmetropole.fr
#   Étienne ROUVIN  e.rouvin@rennesmetropole.fr
#
# Created:     05/06/2018
#
# Licence:     GPL 3
#
# Requirements
#   python 3.4 >
#   psycopg2
#-------------------------------------------------------------------------------

import os
import sys
import configparser
import argparse
from argparse import RawTextHelpFormatter
from datetime import date, timedelta, datetime
import psycopg2
import re

# répertoire courant
script_dir = os.path.dirname(__file__)

# lecture du fichier de configuration qui contient les infos de connection
# à la base geOrchestra et à la base dans laquelle on écrit les stats consolidées
config = configparser.ConfigParser()
config.read( script_dir + '/config.ini')

# connexion DBLINK à la base georchestra
DB_georchestra_ConnString = "host="+ config.get('DB_georchestra', 'host') +" dbname="+ config.get('DB_georchestra', 'db') +" user="+ config.get('DB_georchestra', 'user') +" password="+ config.get('DB_georchestra', 'passwd')
# le schéma
DB_georchestra_schema = config.get('DB_georchestra', 'schema')


# connexion à la base d'écriture des stats
DB_stats_ConnString = "host='"+ config.get('DB_stats', 'host') +"' dbname='"+ config.get('DB_stats', 'db') +"' user='"+ config.get('DB_stats', 'user') +"' password='"+ config.get('DB_stats', 'passwd') +"'"
# le schéma
DB_stats_schema = config.get('DB_stats', 'schema')


# les variables globales
siteid = 0
DateArg = ""
DateToTreat = ""
DateToFollow = ""
WeekYear = ""


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def LiveUpdate() :

  print("")
  print( "# traitement des stats live !" )

  # indicateur pour l'état de la maj des stats
  status = False

  # trouver la table à attaquer
  DateToTreat = date.today().strftime('%Y-%m-%d')
  ogc_table = "ogc_services_log_y" + DateToTreat[0:4] + "m" + re.sub('^0+', '', DateToTreat[5:7])
  print ( "  table à traiter : " + DB_georchestra_schema + "." + ogc_table )

  # on crée les 2 requêtes SQL à jouer
  SQLinsert = """INSERT INTO """ + DB_stats_schema + """.ogc_services_stats_live
  (
    SELECT
      siteid,
      org,
      lower(user_name),
      service,
      hits,
      layers_nb,
      first_hit,
      last_hit
    FROM
      dblink('""" + DB_georchestra_ConnString + """'::text,
        'SELECT
          """ + siteid + """ AS siteid,
          CASE WHEN org = ''Rennes_M_tropole'' THEN ''Rennes Métropole''
            WHEN org = ''Ville_de_Rennes'' THEN ''Ville de Rennes''
            WHEN org = ''Ville_de_rennes'' THEN ''Ville de Rennes''
            ELSE org
          END AS org,
          lower(user_name), service,
          COUNT(service) AS hits,
          COUNT(DISTINCT(layer)) AS layers_nb,
          RIGHT(MIN(date)::text,8)::varchar AS first_hit,
          RIGHT(MAX(date)::text,8)::varchar AS last_hit
        FROM """ + DB_georchestra_schema + "." + ogc_table + """
        WHERE
          date > CURRENT_DATE
          AND service IN (''WMS'', ''WMTS'')
          AND lower(user_name) NOT IN (''acces.sig'', ''admsig'', ''c2c-monitoring'', ''geoserver_privileged_user'', ''intranet'', ''ldapsig'')
          GROUP BY
            CASE WHEN org = ''Rennes_M_tropole'' THEN ''Rennes Métropole''
              WHEN org = ''Ville_de_Rennes'' THEN ''Ville de Rennes''
              WHEN org = ''Ville_de_rennes'' THEN ''Ville de Rennes''
              ELSE org
            END , lower(user_name), service'::text)
      AS (
          siteid integer,
          org character varying(255),
          lower(user_name) character varying(255),
          service character varying(5),
          hits bigint,
          layers_nb bigint,
          first_hit character varying(8),
          last_hit character varying(8)
          )
  );"""
  #print("")
  #print(SQLinsert)
  #print("")

  SQLVerif = "SELECT COUNT(*) AS count FROM " + DB_stats_schema + ".ogc_services_stats_live ;"
  #print(SQLVerif)

  # connection à la base
  try:
    # connexion à la base, si plante, on sort
    conn = psycopg2.connect(DB_stats_ConnString)
    cursor = conn.cursor()
  except:
    print( "connexion à la base impossible")

  try:
    # on commence par faire un TRUNCATE
    cursor.execute( "TRUNCATE TABLE " + DB_stats_schema + ".ogc_services_stats_live ;" )
    conn.commit()

    # puis on lance la requêt INSERT
    cursor.execute(SQLinsert)
    conn.commit()

    try:
      # puis on lance la requête pour vérifier le nb d'enregistrements créés
      cursor.execute(SQLVerif)
      result = cursor.fetchone()
      NbRecordsInserted = result[0]
      print( "  nombre d'enregistrements insérés : " + str(NbRecordsInserted) )

    except:
      print( "  impossible d'exécuter la requête VERIF")

  except Exception as err:
    print( "  impossible d'exécuter la requête INSERT")
    print( "  PostgreSQL error code : " + err.pgcode )

  try:
    cursor.close()
    conn.close()
  except:
    print("")

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def DailyUpdate():

  # on va lire la table  ogc_services_log_y[YYY]m[M]  qui correspond à la date demandée
  # on va y sélectionner les enregistrements correspondants
  # avec un regroupement org / username / service / request / layer / role
  # tout en supprimant les enregistrements créés par par des comptes liés au monitoring des services
  # + count pour chaque cas

  print("")
  print( "# traitement des stats au jour" )

  # indicateur pour l'état de la maj des stats
  status = False

  # trouver la table à attaquer
  ogc_table = "ogc_services_log_y" + DateToTreat[0:4] + "m" + re.sub('^0+', '', DateToTreat[5:7])
  print ( "  table à traiter : " + DB_georchestra_schema + "." + ogc_table )

  # on crée les 2 requêtes SQL à jouer
  SQLinsert = """INSERT INTO """ + DB_stats_schema + """.ogc_services_stats_daily
  (
    SELECT
      siteid,
      date,
      org,
      lower(user_name),
      service,
      request,
      layer,
      count,
      week,
      month,
      year,
      weekyear,
      monthyear
    FROM
      dblink('""" + DB_georchestra_ConnString + """'::text,
        'SELECT
          """ + siteid + """ AS siteid,
          ''""" + DateToTreat + """''::date AS date,
          CASE WHEN org = ''Rennes_M_tropole'' THEN ''Rennes Métropole''
		        WHEN org = ''Ville_de_Rennes'' THEN ''Ville de Rennes''
		        WHEN org = ''Ville_de_rennes'' THEN ''Ville de Rennes''
		        ELSE org
          END AS org,
          lower(user_name), service, request, layer,
          COUNT(*) AS count,
          EXTRACT(WEEK FROM ''""" + DateToTreat + """''::date)::integer AS week,
          EXTRACT(MONTH FROM ''""" + DateToTreat + """''::date)::integer AS month,
          EXTRACT(YEAR FROM ''""" + DateToTreat + """''::date)::integer AS year,
          CONCAT(EXTRACT(YEAR FROM ''""" + DateToTreat + """''::date), ''-'', LPAD(EXTRACT(WEEK FROM ''""" + DateToTreat + """''::date)::text, 2, ''0'')) AS weekyear,
          CONCAT(EXTRACT(YEAR FROM ''""" + DateToTreat + """''::date), ''-'', LPAD(EXTRACT(MONTH FROM ''""" + DateToTreat + """''::date)::text, 2, ''0'')) AS monthyear
        FROM """ + DB_georchestra_schema + "." + ogc_table + """
        WHERE date > ''""" + DateToTreat + """''::date AND date < ''""" + DateToFollow + """''::date
        AND lower(user_name) NOT IN (''acces.sig'', ''admsig'', ''c2c-monitoring'', ''geoserver_privileged_user'', ''intranet'', ''ldapsig'')
        GROUP BY
            CASE WHEN org = ''Rennes_M_tropole'' THEN ''Rennes Métropole''
              WHEN org = ''Ville_de_Rennes'' THEN ''Ville de Rennes''
              WHEN org = ''Ville_de_rennes'' THEN ''Ville de Rennes''
              ELSE org
            END , lower(user_name), service, request, layer'::text)
      AS (
          siteid integer,
          date date,
          org character varying(255),
          lower(user_name) character varying(255),
          service character varying(5),
          request character varying(20),
          layer character varying(255),
          count bigint,
          week integer,
          month integer,
          year integer,
          weekyear character varying(10),
          monthyear character varying(10)
          )
  );"""
  #print("")
  #print(SQLinsert)
  #print("")

  SQLVerif = """SELECT COUNT(*) AS count
  FROM """ + DB_stats_schema + """.ogc_services_stats_daily
  WHERE date ='""" + DateToTreat + """'::date"""
  #print(SQLVerif)

  # connection à la base
  try:
    # connexion à la base, si plante, on sort
    conn = psycopg2.connect(DB_stats_ConnString)
    cursor = conn.cursor()
  except:
    print( "connexion à la base impossible")

  try:
    # on lance la requêt INSERT
    cursor.execute(SQLinsert)
    conn.commit()

    try:
      # puis on lance la requête pour vérifier le nb d'enregistrements créés
      cursor.execute(SQLVerif)
      result = cursor.fetchone()
      NbRecordsInserted = result[0]
      print( "  nombre d'enregistrements insérés : " + str(NbRecordsInserted) )

      # si on est là c'est que tout s'est bien passé
      status = True

    except Exception as err:
      print( "impossible d'exécuter la requête VERIF")
      print( "  PostgreSQL error code : " + err.pgcode )

  except Exception as err:
    print( "  impossible d'exécuter la requête INSERT")
    print( "  PostgreSQL error code : " + err.pgcode )

  try:
    cursor.close()
    conn.close()
  except:
    print("")

  # on peut donc passer à la maj des stats à la semaine
  # si feu vert
  if status == True :
    WeeklyUpdate()
  else:
    print("  arrêt du script")


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def WeeklyUpdate():

  # on va faire une requête sur la table daily dans laquelle on vient d'insérer des lignes
  # avant d"insérer on va supprimer les données de la semaine courante
  # car ce script peut être exécuté chaque jour de la semaine
  # on préfère un delete et insert plutôt que de chercher à incrémenter des compteurs

  print("")
  print( "# traitement des stats à la semaine" )

  # on commence par déterminer la semaine courante depuis la date à traiter
  global WeekYear


  # à simplifier mais fonctionner, peut être en créant des variables year, month, days en int
  WeekYear = date(int(DateToTreat[0:4]) , int(re.sub('^0+', '', DateToTreat[5:7])), int(re.sub('^0+', '', DateToTreat[8:10]))).isocalendar()[1]

  # si la semaine courante a un seul chiffre, on la modifie pour en avoir 2 (ex : 9 -> 09)
  if len(str(WeekYear))==1 :
    WeekYear = '%02d' % WeekYear

  WeekYear = DateToTreat[0:4] + '-' + str(WeekYear)
  print( "  WeekYear = " + WeekYear)


  # on vide la table de la semaine courante avant d'insérer les noveaux enregistrements de la semaine

  SQLdeleteW = """DELETE FROM """ + DB_stats_schema + """.ogc_services_stats_weekly
      WHERE weekyear = '""" + WeekYear +"""' AND siteid = """ + siteid + """; """
  #print(SQLdeleteW)

  # on peut maintenant insérer toute les valeurs correspondant à cette semaine courante
  SQLinsertW = """INSERT INTO """ + DB_stats_schema + """.ogc_services_stats_weekly
  (
    SELECT
      siteid, org, lower(user_name), service, request, layer,
      SUM(count) AS count,
      week, year, weekyear
    FROM """ + DB_stats_schema + """.ogc_services_stats_daily
    WHERE weekyear = '""" + WeekYear + """' AND siteid = """ + siteid + """
    GROUP BY siteid, org, lower(user_name), service, request, layer, week, year, weekyear
  );"""
  #print(SQLinsertW)

  SQLVerif = """SELECT COUNT(*) AS count
  FROM """ + DB_stats_schema + """.ogc_services_stats_weekly
  WHERE weekyear = '""" + WeekYear + """' AND siteid = """ + siteid + """;"""
  #print(SQLVerif)

   # connection à la base
  try:
    # connexion à la base, si plante, on sort
    conn = psycopg2.connect(DB_stats_ConnString)
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

    try:
      # puis on lance la requête pour vérifier le nb d'enregistrements créés
      cursor.execute(SQLVerif)
      result = cursor.fetchone()
      NbRecordsInserted = result[0]
      print( "  nombre d'enregistrements insérés : " + str(NbRecordsInserted) )
    except Exception as err:
      print( "impossible d'exécuter la requête VERIF")
      print( "  PostgreSQL error code : " + err.pgcode )

  except Exception as err:
    print( "  impossible d'exécuter la requête DELETE ou INSERT")
    print( "  PostgreSQL error code : " + err.pgcode )

  # si on est là c'est que tout s'est bien passé
  try:
    cursor.close()
    conn.close()
  except:
    print("")

  # on peut donc passer à la maj des stats au mois
  MonthlyUpdate()


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def MonthlyUpdate():

  # on va faire une requête sur la table daily dans laquelle on vient d'insérer des lignes
  # avant d"insérer on va supprimer les données du mois courante
  # car ce script peut être exécuté chaque jour de la semaine
  # on préfère un delete et insert plutôt que de chercher à incrémenter des compteurs

  print("")
  print( "# traitement des stats au mois" )

  # on commence par déterminer le mois courant depuis la date à traiter
  global MonthYear

  # à simplifier mais fonctionner, peut être en créant des variables year, month, days en int
  MonthYear = DateToTreat[0:4] + '-' + DateToTreat[5:7]

  print( "  MonthYear = " + MonthYear)

  #on vide la table de la semaine courante avant d'insérer des enregistrements

  SQLdeleteM = """DELETE FROM """ + DB_stats_schema + """.ogc_services_stats_monthly
  WHERE monthyear = '""" + MonthYear +"""' AND siteid = """ + siteid +"""; """
  #print(SQLdeleteM)

  # on peut maintenant insérer toute les valeurs correspondant à cette semaine courante
  SQLinsertM = """INSERT INTO """ + DB_stats_schema + """.ogc_services_stats_monthly
  (
    SELECT
      siteid, org, lower(user_name), service, request, layer,
      SUM(count) AS count,
      month, year, monthyear
    FROM """ + DB_stats_schema + """.ogc_services_stats_daily
    WHERE monthyear = '""" + MonthYear + """' AND siteid = """ + siteid +"""
    GROUP BY siteid, org, lower(user_name), service, request, layer, month, year, monthyear
  );"""
  #print(SQLinsertM)

  SQLVerif = """SELECT COUNT(*) AS count
  FROM """ + DB_stats_schema + """.ogc_services_stats_monthly
  WHERE monthyear = '""" + MonthYear + """' AND siteid = """ + siteid +""";"""
  #print(SQLVerif)

   # connection à la base
  try:
    # connexion à la base, si plante, on sort
    conn = psycopg2.connect(DB_stats_ConnString)
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

    try:
      # puis on lance la requête pour vérifier le nb d'enregistrements créés
      cursor.execute(SQLVerif)
      result = cursor.fetchone()
      NbRecordsInserted = result[0]
      print( "  nombre d'enregistrements insérés : " + str(NbRecordsInserted) )
    except Exception as err:
      print( "impossible d'exécuter la requête VERIF")
      print( "  PostgreSQL error code : " + err.pgcode )

  except Exception as err:
    print( "  impossible d'exécuter la requête DELETE ou INSERT")
    print( "  PostgreSQL error code : " + err.pgcode )

  # si on est là c'est que tout s'est bien passé
  try:
    cursor.close()
    conn.close()
  except:
    print("")

  # on peut donc passer au vaccum
  Vacuum()


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def Vacuum() :

  print("")
  print( "# vacuum des tables si on est le 1er du mois" )

  # on réalise un vacuum chaque début de mois afin d'optimiser l'espace de stockage
  SQLVacuumW = """ VACUUM FULL """ + DB_stats_schema + """.ogc_services_stats_weekly; """
  SQLVacuumM = """ VACUUM FULL """ + DB_stats_schema + """.ogc_services_stats_monthly; """

  if DateToTreat[8:10] == '01' :

    print("  vacuum en cours")

    #connexion à la base
    conn = psycopg2.connect(DB_stats_ConnString)
    cursor = conn.cursor()
    conn.set_session(autocommit=True)

    try:
      #lancement du VACUUM
      cursor.execute(SQLVacuumW)
      cursor.execute(SQLVacuumM)
    except Exception as err:
      print( "  impossible de faire les VACUUM")
      print( "  PostgreSQL error code : " + err.pgcode )

    cursor.close()
    conn.close()

  else:
     print("  pas de vacuum à faire")




# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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

  Ce script traite les données de la table ogc_services_log du mois courant afin de consolider des statistiques selon 3 plages de temps :
    - stats au jour
    - stats à la semaine
    - stats au mois

  Si aucune date n'est fournie alors le script traitera les logs de la veille. Usage typique : lancé par un cron pour traiter les stats de la veille.

  Le mode 'live' permet de connaître l'utilisation des services sur le jour courant. A exécuter toutes les n minutes.

  Exemple : update_ogc_stats.py -site 1 -date 2018-05-28""", formatter_class=RawTextHelpFormatter)

  # identifiant du site à traiter
  # obligatoire
  parser.add_argument("-site", help="""Identifiant du site à traiter (obligatoire).""")

  # mode live
  # optionnel
  parser.add_argument("-live", help="""Mode live : permet de connaître l'utilisation des services sur le jour courant (optionnel).""")

  # date
  # Par défaut= date du jour -1 si pas spécifié
  # optionnel
  parser.add_argument("-date", help="""Permet de forcer le traitement d'une date au format 'YYYY-MM-DD' (optionnel)""")

  # debug
  #print( 'Number of arguments:', len(sys.argv), 'arguments.' )
  #print( 'Argument List:', str(sys.argv) )


  # test des variables passées
  test_siteid = False
  test_live = False
  test_date = False

  # le site : obligatoire
  if ('-site' in sys.argv):
    # on mémorise le siteid
    siteid = str(sys.argv[2])
    print('identifiant de site = '+ siteid)
    # TODO tester si integer
    test_siteid = True
    pass
  else:
    print("\n Erreur : identifiant de site obligatoire\n")
    parser.print_help()
    sys.exit()
    return

  # le mode live
  if ('-live' in sys.argv):
    print( "mode live !" )
    test_live = True
    # le mode live est assez autonome donc on le lance directement
    LiveUpdate()

  # si pas mode live -> mode normal
  else:
    print( "mode normal")

    # on teste si une date de forçage a été fournie
    if ('-date' in sys.argv):
      # on force pur le moment, sans contrôler
      DateArg = str(sys.argv[4])
      DateToTreat = DateArg
      print( "date forcée : " + DateArg)

    # pas de date forcée = on prend la date de hier
    else:
      yesterday = date.today() - timedelta(1)
      DateToTreat = yesterday.strftime('%Y-%m-%d')
      print( "date de hier : " + DateToTreat)

    # on déduit la date du jour suivant, pour les intervalles dans les requêtes SQL
    ConvDateToTreat = datetime.strptime(DateToTreat, '%Y-%m-%d')
    nextday = ConvDateToTreat + timedelta(1)
    DateToFollow = nextday.strftime('%Y-%m-%d')
    print( "date du jour suivant : " + DateToFollow )

    # et on lance la maj des stats
    DailyUpdate()


  print( "")
  print( "  F I N")

  return

if __name__ == '__main__':
    main()
