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

import time
import psycopg2


# les variables globales
today=time.strftime("%Y-%m-%d") # donne la date du jour


def test():

    SQLinsert = """INSERT INTO ogcstatistics.ogc_services_stats_daily
    (
      SELECT
        1 AS siteid,
        '{}'::date-1 AS date,
        org, user_name, service, request, layer,
        COUNT(*) AS count,
        EXTRACT(WEEK FROM '""" + today + """'::date-1)::integer AS week,
        EXTRACT(MONTH FROM '{}'::date-1)::integer AS month,
        EXTRACT(YEAR FROM '{}'::date-1)::integer AS year,
        CONCAT(EXTRACT(YEAR FROM '{}'::date-1), '-', EXTRACT(WEEK FROM '{}'::date-1)) AS weekyear,
        CONCAT(EXTRACT(YEAR FROM '{}'::date-1), '-', EXTRACT(MONTH FROM '{}'::date-1)) AS monthyear
      FROM ogcstatistics.ogc_services_log_y2018m3
      WHERE date > '{}'::date-1 AND date < '{}'::date
      GROUP BY org, user_name, service, request, layer, roles
    );"""

    print(SQLinsert)



def main():

    test()

    pass

if __name__ == '__main__':
    main()
