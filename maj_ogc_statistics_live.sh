#/bin/bash

# cron System Scheduler
# "C:\Program Files\Git\bin\sh.exe"
# --login -i -- ./maj_ogc_statistics.sh {dataset}

# exit dès que qqch se passe mal
set -e

cd /e/taches/georchestra_ogc_stats_analyze

# cette ligne permet de rediriger toutes les sorties qui suivent
# dans un fichier de log
# Redirect stdout to file log.out then redirect stderr to stdout.
exec 1>./logs/live.log 2>&1


# session virtuelle python
source venv/Scripts/Activate

python update_ogc_stats.py -site 1 -live

# fermeture
deactivate
