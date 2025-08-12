import os
import requests
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from geojson import LineString

# Конфигурация
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
DATABASE_URL = os.getenv('DATABASE_URL')
HEADERS = {'User-Agent': 'RoadsDownloader/1.0'}
DATA_DIR = '../data'

# Создаем папку для данных, если ее нет
os.makedirs(DATA_DIR, exist_ok=True)

# Список городов (оставлен только Новосибирск для примера)
CITIES = {
    "Пушкино": [37.82, 55.99, 37.88, 56.03],
    "Королёв": [37.80, 55.90, 37.90, 55.95],
    "Ивантеевка": [37.90, 55.96, 37.97, 55.99],
    "Фрязино": [38.02, 55.94, 38.08, 55.97],
    "Балашиха": [37.90, 55.78, 37.99, 55.84],
    "Реутов": [37.84, 55.72, 37.90, 55.78],
    "Люберцы": [37.82, 55.65, 37.98, 55.72],
    "Купчино": [37.91, 55.72, 38.05, 55.78],
    "Красногорск": [37.26, 55.80, 37.42, 55.87],
    "Одинцово": [37.23, 55.65, 37.41, 55.72],
    "Долгопрудный": [37.47, 55.90, 37.56, 55.97],
    "Мытищи": [37.68, 55.88, 37.80, 55.94],
    "Жуковский": [38.06, 55.58, 38.15, 55.62],
    "Раменское": [38.19, 55.55, 38.27, 55.59],
    "Электросталь": [38.41, 55.75, 38.50, 55.81],
    "Москва": [37.38, 55.49, 37.84, 55.91],
    "Калуга": [36.16, 54.48, 36.33, 54.54],
    "Тула": [37.55, 54.14, 37.71, 54.24],
    "Рязань": [39.64, 54.60, 39.84, 54.65],
    "Тверь": [35.81, 56.81, 35.96, 56.89],
    "Минск": [27.42, 53.84, 27.72, 53.97],
    "Кронштадт": [29.69, 59.98, 29.81, 60.02],
    "Сестрорецк": [29.92, 60.04, 30.02, 60.13],
    "Петергоф": [29.83, 59.86, 29.96, 59.90],
    "Колпино": [30.55, 59.72, 30.64, 59.76],
    "Пушкин": [30.36, 59.67, 30.50, 59.77],
    "Санкт-Петербург": [30.07, 59.80, 30.54, 60.10],
    "Калининград": [20.43, 54.66, 20.59, 54.76],
    "Гурьевск": [20.59, 54.76, 20.62, 54.78],
    "Ярославль": [39.75, 57.55, 39.98, 57.71],
    "Липецк": [39.47, 52.57, 39.64, 52.63],
    "Воронеж": [39.11, 51.63, 39.23, 51.73],
    "Нижний Новгород": [43.79, 56.21, 44.09, 56.37],
    "Чебоксары": [47.15, 56.08, 47.40, 56.16],
    "Новочебоксарск": [47.44, 56.10, 47.52, 56.13],
    "Иннополис": [48.74, 55.74, 48.75, 55.76],
    "Казань": [49.02, 55.72, 49.26, 55.87],
    "Самара": [50.07, 53.17, 50.29, 53.29],
    "Ижевск": [53.13, 56.83, 53.32, 56.89],
    "Пермь": [56.11, 57.95, 56.33, 58.04],
    "Екатеринбург": [60.48, 56.74, 60.71, 56.92],
    "Сургут": [73.32, 61.23, 73.49, 61.29],
    "Нижневартовск": [76.54, 60.92, 76.64, 60.96],
    "Тюмень": [65.45, 57.09, 65.68, 57.21],
    "Челябинск": [61.25, 55.11, 61.49, 55.22],
    "Уфа": [55.93, 54.68, 56.14, 54.83],
    "Магнитогорск": [58.95, 53.35, 59.01, 53.44],
    "Оренбург": [55.05, 51.75, 55.19, 51.85],
    "Волгоград": [44.46, 48.68, 44.57, 48.78],
    "Ростов-на-Дону": [39.57, 47.19, 39.84, 47.31],
    "Махачкала": [47.42, 42.94, 47.56, 43.01],
    "Каспийск": [47.60, 42.88, 47.65, 42.91],
    "Владикавказ": [44.62, 43.01, 44.71, 43.07],
    "Краснодар": [38.89, 44.99, 39.13, 45.15],
    "Анапа": [37.29, 44.86, 37.36, 44.96],
    "Новороссийск": [37.72, 44.67, 37.81, 44.75],
    "Сочи": [39.71, 43.56, 39.76, 43.63],
    "Сириус": [39.88, 43.38, 40.01, 43.50],
    "Красная Поляна": [40.19, 43.67, 40.31, 43.69],
    "Алматы": [76.84, 43.19, 76.98, 43.28],
    "Астана": [71.36, 51.10, 71.51, 51.20],
    "Омск": [73.24, 54.93, 73.44, 55.05],
    "Барнаул": [83.64, 53.32, 83.80, 53.39],
    "Новокузнецк": [87.09, 53.74, 87.22, 53.91],
    "Новосибирск": [82.80, 54.96, 83.03, 55.11],
    "Академгородок": [83.04, 54.83, 83.12, 54.87],
    "Кемерово": [86.06, 55.31, 86.20, 55.41],
    "Томск": [84.93, 56.45, 85.07, 56.53],
    "Красноярск": [92.71, 55.97, 93.04, 56.07],
    "Иркутск": [104.13, 52.21, 104.38, 52.37],
    "Владивосток": [131.84, 43.00, 131.98, 43.18],
    "Хабаровск": [135.03, 48.38, 135.18, 48.57],
    "Лиссабон": [-9.21, 38.69, -9.09, 38.77],
    "Флорианополис": [-48.59, -27.62, -48.50, -27.57],
    "Сантьяго": [-70.60, -33.46, -70.53, -33.38]
}

def setup_database():
    """Подключение к БД"""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS roads (
            id SERIAL PRIMARY KEY,
            city TEXT NOT NULL,
            highway TEXT NOT NULL,
            name TEXT,
            geom GEOMETRY(LINESTRING, 4326)
        );
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS roads_geom_idx ON roads USING GIST(geom);
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS roads_city_idx ON roads(city);
        """))
        conn.commit()
    
    return Session

def build_overpass_query(bbox):
    """Overpass QL запрос"""
    return f"""
    [out:json][timeout:90];
    (
      way["highway"]
        ["highway"!="motorway"]
        ["highway"!="motorway_link"]
        ["highway"!="steps"]
        ["indoor"!="yes"]
        ["level"!~"."]
        ["subway"!~"."]
        ["layer"!~"-2|-3|-4|-5"]
        ({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
    );
    out body geom qt;
    """

def save_to_disk(city_name, data):
    """Сохраняем данные в JSON файл"""
    filename = f"{DATA_DIR}/{city_name}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Данные сохранены в {filename}")

def load_from_disk(city_name):
    """Загружаем данные из JSON файла"""
    filename = f"{DATA_DIR}/{city_name}.json"
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def download_city_data(city_name, bbox):
    """Загрузка данных с Overpass API"""
    print(f"Загружаем данные для {city_name}...")
    query = build_overpass_query(bbox)
    try:
        response = requests.get(
            OVERPASS_URL,
            params={'data': query},
            headers=HEADERS,
            timeout=120
        )
        response.raise_for_status()
        data = response.json()
        save_to_disk(city_name, data)
        return data
    except Exception as e:
        print(f"Ошибка при загрузке данных: {str(e)}")
        return None

def import_to_database(session, data, city_name):
    """Импорт данных в БД"""
    count = 0
    for element in data.get('elements', []):
        if element['type'] == 'way' and 'geometry' in element:
            tags = element.get('tags', {})
            highway_type = tags.get('highway', 'unknown')
            
            coords = [(node['lon'], node['lat']) for node in element['geometry']]
            linestring = json.dumps(LineString(coords))
            
            try:
                session.execute(text("""
                INSERT INTO roads (city, highway, name, geom)
                VALUES (:city, :highway_type, :name, ST_GeomFromGeoJSON(:geom))
                ON CONFLICT DO NOTHING
                """), {
                    'city': city_name,
                    'highway_type': highway_type,
                    'name': tags.get('name'),
                    'geom': linestring
                })
                count += 1
            except Exception as e:
                print(f"Ошибка при вставке: {str(e)}")
                session.rollback()
                continue
    
    session.commit()
    return count

def main():
    # Этап 1: Загрузка данных на диск
    for city_name, bbox in CITIES.items():
        # Проверяем наличие файла перед загрузкой
        if not os.path.exists(f"{DATA_DIR}/{city_name}.json"):
            data = download_city_data(city_name, bbox)
            if not data:
                print(f"Не удалось загрузить данные для {city_name}")
                continue

    # Этап 2: Импорт данных из файлов в БД
    Session = setup_database()
    session = Session()
    total_count = 0
    
    for city_name in CITIES:
        data = load_from_disk(city_name)
        if not data:
            print(f"Файл с данными для {city_name} не найден")
            continue
            
        count = import_to_database(session, data, city_name)
        total_count += count
        print(f"Импортировано дорог для {city_name}: {count}")
    
    session.close()
    print(f"\nИтого импортировано {total_count} объектов")

if __name__ == "__main__":
    main()