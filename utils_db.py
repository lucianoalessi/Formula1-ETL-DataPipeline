# Importamos librerías
#parte1:
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
#parte 2:
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from configparser import ConfigParser

# Función para obtener datos de la API
def get_data(base_url, endpoint, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos en formato JSON.

    Parámetros:
    base_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API al que se realizará la solicitud.
    params (dict): Parámetros de consulta para enviar con la solicitud (opcional).
    headers (dict): Encabezados para enviar con la solicitud (opcional).

    Retorna:
    dict/list: Los datos obtenidos de la API, o None si hay un error.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error en la petición: {e}")
        return None

# Función para construir un DataFrame a partir de datos JSON
def build_table(json_data, record_path=None):
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON.

    Parámetros:
    json_data (dict/list): Los datos en formato JSON obtenidos de una API.

    Retorna:
    DataFrame: Un DataFrame de pandas que contiene los datos, o None si hay un error.
    """
    try:
        df = pd.json_normalize(json_data, record_path)
        return df
    except ValueError:
        print("Los datos no están en el formato esperado")
        return None

# Función para guardar un DataFrame en formato Parquet
def save_to_parquet(df, output_path, partition_cols=None):
    """
    Guarda un DataFrame en formato Parquet en el directorio especificado usando fastparquet.

    Parámetros:
    df (DataFrame): El DataFrame a guardar.
    path (str): La ruta del archivo donde se guardará el DataFrame.
    partition_cols (list, opcional): Columnas por las que se particionarán los datos.
    """
    directory = os.path.dirname(output_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    df.to_parquet(output_path, partition_cols=partition_cols, engine='fastparquet')

# Función para obtener la fecha de la última extracción incremental
def get_last_extraction_date(file_path):
    """
    Obtiene la fecha de la última extracción incremental desde un archivo.

    Parámetros:
    file_path (str): La ruta del archivo que contiene la fecha de la última extracción.

    Retorna:
    datetime: La fecha de la última extracción, o None si hay un error.
    """
    try:
        with open(file_path, 'r') as file:
            last_extraction_date = file.readline().strip()
            return datetime.strptime(last_extraction_date, '%Y-%m-%d')
    except (FileNotFoundError, ValueError):
        return None

# Función para actualizar la fecha de la última extracción incremental
def update_last_extraction_date(file_path, extraction_date):
    """
    Actualiza la fecha de la última extracción incremental en un archivo.

    Parámetros:
    file_path (str): La ruta del archivo donde se guardará la fecha de extracción.
    extraction_date (datetime): La fecha de la extracción actual.
    """
    with open(file_path, 'w') as file:
        file.write(extraction_date.strftime('%Y-%m-%d'))

# Función para extraer datos de todos los pilotos, actuales e historicos (datos estáticos)
def get_drivers(api_url, endpoint, params):
    """
    Extrae datos de pilotos desde la API ergast de la F1 y construye un DataFrame con estos datos.

    Parámetros:
    api_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API para obtener los datos de pilotos.
    params (dict): Parámetros de consulta para enviar con la solicitud.

    Retorna:
    DataFrame: Un DataFrame de pandas que contiene los datos de los pilotos, o None si hay un error.
    """
    data = get_data(api_url, endpoint, params=params)
    if data:
        drivers = data.get('MRData', {}).get('DriverTable', {}).get('Drivers', [])
        return build_table(drivers)
    return None

# Función para obtener el número total de rondas por temporada de F1 (dato estatico)
def total_round(api_url, season):
    """
    Obtiene el número total de rondas en una temporada específica de F1.

    Parámetros:
    api_url (str): La URL base de la API.
    season (int/str): El año de la temporada.

    Retorna:
    int: El número total de rondas en la temporada.
    """
    endpoint_season = f"{season}.json"
    data_season = get_data(api_url, endpoint_season)
    if data_season:
        total_rounds = int(data_season.get('MRData', {}).get('total', 0))
    else:
        total_rounds = 0
    return total_rounds

# Función para extraer tiempos de vuelta de los pilotos (datos temporales, se van agregando nuevos registros por carrera)
def get_lap_times(api_url, season, params, last_extraction_date=None):
    """
    Extrae los tiempos de vuelta de los pilotos por vuelta en cada carrera en una temporada específica.

    Parámetros:
    api_url (str): La URL base de la API.
    season (int/str): El año de la temporada.
    params (dict): Parámetros de consulta para enviar con la solicitud.
    last_extraction_date (datetime, opcional): Fecha de la última extracción incremental.

    Retorna:
    list: Lista de diccionarios con los tiempos de vuelta.
    """
    lap_times = []
    total_rounds = total_round(api_url, season)
    for round_number in range(1, total_rounds + 1):
        endpoint = f"{season}/{round_number}/laps.json"
        data = get_data(api_url, endpoint, params=params)
        if data:
            races = data.get('MRData', {}).get('RaceTable', {}).get('Races', [])
            if races:
                race_info = races[0]
                date = race_info['date']
                country = race_info['Circuit']['Location']['country']
                circuit_name = race_info['Circuit']['circuitName']
                laps = race_info['Laps']
                for lap in laps:
                    lap_number = lap['number']
                    for timing in lap['Timings']:
                        lap_time = {
                            'season': season,
                            'date': date,
                            'round': round_number,
                            'country': country,
                            'circuit_name': circuit_name,
                            'lap_number': lap_number,
                            'driverId': timing['driverId'],
                            'position': timing['position'],
                            'time': timing['time']
                        }

                        # Convertimos la fecha de lap_time a un objeto datetime antes de la comparación
                        lap_time_date = datetime.strptime(lap_time['date'], '%Y-%m-%d')
                        if not last_extraction_date or lap_time_date > last_extraction_date:
                            lap_times.append(lap_time)
    return lap_times

# Función para extraer los resultados de cada carrera de la temporada (datos temporales, se agregan nuevos registros)
def get_race_results(api_url, season, params=None, last_extraction_date=None):
    """
    Extrae los resultados de las carreras de una temporada específica.

    Parámetros:
    api_url (str): La URL base de la API.
    season (int/str): El año de la temporada.
    params (dict): Parámetros de consulta para enviar con la solicitud (opcional).
    last_extraction_date (datetime, opcional): Fecha de la última extracción incremental.

    Retorna:
    list: Lista de diccionarios con los resultados de las carreras.
    """
    race_results = []
    total_rounds = total_round(api_url, season)
    for round_number in range(1, total_rounds + 1):
        endpoint = f"{season}/{round_number}/results.json"
        data = get_data(api_url, endpoint, params=params)
        if data:
            races = data.get('MRData', {}).get('RaceTable', {}).get('Races', [])
            for race in races:
                race_date = datetime.strptime(race['date'], "%Y-%m-%d")
                if not last_extraction_date or race_date > last_extraction_date:
                    for result in race.get('Results', []):
                        result_data = {
                            'season': season,
                            'round': round_number,
                            'race_name': race['raceName'],
                            'circuit_name': race['Circuit']['circuitName'],
                            'date': race['date'],
                            'driver': result['Driver']['familyName'],
                            'position': result['position'],
                            'time': result.get('Time', {}).get('time', None),
                            'points': result['points']
                        }
                        race_results.append(result_data)
    return race_results

# Función para extraer clasificación en el campeonato de pilotos por temporada (datos temporales, van cambiando a medida que avanza la temporada)
def get_driver_standings(api_url, season, params=None, last_extraction_date=None):
    """
    Extrae la clasificación en el campeonato de pilotos en una temporada específica.

    Parámetros:
    api_url (str): La URL base de la API.
    season (int/str): El año de la temporada.
    params (dict): Parámetros de consulta para enviar con la solicitud (opcional).
    last_extraction_date (datetime, opcional): Fecha de la última extracción incremental.

    Retorna:
    list: Lista de diccionarios con la clasificación de los pilotos.
    """
    standings_data = []
    endpoint = f"{season}/driverStandings.json"
    data = get_data(api_url, endpoint, params=params)
    if data:
        standings = data.get('MRData', {}).get('StandingsTable', {}).get('StandingsLists', [])
        for standing in standings:
            for driver in standing.get('DriverStandings', []):
                standing_data = {
                    'season': season,
                    'round': standing['round'],
                    'driver': driver['Driver']['familyName'],
                    'position': driver['position'],
                    'points': driver['points']
                }
                standings_data.append(standing_data)
    return standings_data


#Funcion para establecer la conexion con la base de datos(EN LA PARTE 2). Es necesario crear un archivo aparte con los datos de conexion.
def connect_to_db(config_file, section, driverdb):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parámetros:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.
    driverdb (str): El driver de la base de datos a la que se conectará.

    Retorna:
    Un objeto de conexión a la base de datos.
    """
    try:
        # Lectura del archivo de configuración
        parser = ConfigParser()
        parser.read(config_file)

        # Creación de un diccionario
        # donde cargaremos los parámetros de la base de datos
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            # Creación de la conexión a la base de datos
            engine = create_engine(
                f"{driverdb}://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}"
            )
            return engine

        else:
            print(
                f"Sección {section} no encontrada en el archivo de configuración.")
            return None
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None