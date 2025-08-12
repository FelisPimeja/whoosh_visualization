set -e


# Запуск анализа на дорогах и треках:
psql "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@db/$POSTGRES_DB" -f import_csv.sql

# Загружаем дороги из OpenStreetMap через Overpass:
echo "Downloading OSM roads"
python overpass.py

# Запуск анализа на дорогах и треках:
psql "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@db/$POSTGRES_DB" -f process_data.sql
