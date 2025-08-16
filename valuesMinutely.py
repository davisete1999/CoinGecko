#!/usr/bin/env python3
"""
Script para descargar datos de criptomonedas en rangos de días desde CoinGecko usando Selenium
Obtiene cryptos desde PostgreSQL y guarda en InfluxDB + respaldos JSON
Con tracking de última fecha procesada para continuar desde donde se quedó
"""

import json
import os
import time
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

import psycopg2
from psycopg2.extras import RealDictCursor

# Configurar logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('coingecko_range_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_env_file(env_file: str = '.env'):
    """Cargar variables de entorno desde archivo .env"""
    env_vars_loaded = 0
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    
                    os.environ[key] = value
                    env_vars_loaded += 1
                    logger.debug(f"Cargada variable: {key}")
        
        logger.info(f"✅ {env_vars_loaded} variables de entorno cargadas desde {env_file}")
            
    except FileNotFoundError:
        logger.warning(f"⚠️ Archivo {env_file} no encontrado, usando variables de entorno del sistema")
    except Exception as e:
        logger.error(f"❌ Error cargando {env_file}: {e}")

@dataclass
class InfluxDBConfig:
    """Configuración de InfluxDB desde variables de entorno"""
    host: str = os.getenv('INFLUXDB_HOST', 'localhost')
    port: int = int(os.getenv('INFLUXDB_EXTERNAL_PORT', '8086'))
    database: str = os.getenv('INFLUXDB_DB', 'quotes')
    token: str = os.getenv('INFLUXDB_TOKEN', 'pU2j5zaAjPTbyC5QARlA60eQ9OsVtIaRODcQLBtRk7K6jmEbA9al98CbNMxMP4kl5DICLL5SH_vHe9rQqbvmvA==')
    org: str = os.getenv('INFLUXDB_ORG', 'CoinAdvisor')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        if not self.token:
            logger.error("❌ INFLUXDB_TOKEN está vacío")
            raise ValueError("INFLUXDB_TOKEN is required for InfluxDB v2")
        
        if not self.org:
            logger.error("❌ INFLUXDB_ORG está vacío")
            raise ValueError("INFLUXDB_ORG is required for InfluxDB v2")
        
        logger.info(f"🔧 InfluxDB Config: {self.host}:{self.port} | org='{self.org}', bucket='{self.database}'")

@dataclass
class PostgreSQLConfig:
    """Configuración de PostgreSQL desde variables de entorno"""
    host: str = os.getenv('POSTGRES_HOST', 'localhost')
    port: int = int(os.getenv('POSTGRES_EXTERNAL_PORT', '5432'))
    database: str = os.getenv('POSTGRES_DB', 'cryptodb')
    user: str = os.getenv('POSTGRES_USER', 'crypto_user')
    password: str = os.getenv('POSTGRES_PASSWORD', 'davisete453')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        logger.info(f"🔧 PostgreSQL Config: {self.host}:{self.port}/{self.database}")

class PostgreSQLManager:
    """Manejador de PostgreSQL para obtener lista de criptomonedas y actualizar progreso"""
    
    def __init__(self, config: PostgreSQLConfig):
        self.config = config
        self.connection = None
        
    def connect(self):
        """Conectar a PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                cursor_factory=RealDictCursor
            )
            logger.info("✅ Conectado a PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando a PostgreSQL: {e}")
            return False
    
    def get_active_cryptocurrencies(self, limit: Optional[int] = None) -> List[Dict]:
        """Obtiene lista de criptomonedas activas desde la base de datos"""
        if not self.connection:
            logger.error("❌ No hay conexión a PostgreSQL")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                # Consulta para obtener criptomonedas activas, priorizando las que necesitan actualización
                sql = """
                SELECT 
                    name,
                    symbol,
                    slug,
                    cmc_id,
                    is_active,
                    tags,
                    badges,
                    last_values_update,
                    oldest_data_fetched,
                    scraping_status
                FROM cryptos 
                WHERE is_active = true 
                AND slug IS NOT NULL 
                AND slug != ''
                ORDER BY 
                    CASE 
                        WHEN scraping_status = 'pending' THEN 1
                        WHEN scraping_status = 'in_progress' THEN 2
                        WHEN scraping_status = 'error' THEN 3
                        WHEN scraping_status = 'completed' THEN 4
                        ELSE 5
                    END,
                    cmc_rank ASC NULLS LAST, 
                    symbol ASC
                """
                
                if limit:
                    sql += f" LIMIT {limit}"
                
                cursor.execute(sql)
                rows = cursor.fetchall()
                
                # Convertir a formato compatible
                cryptocurrencies = []
                for row in rows:
                    crypto = {
                        'nombre': row['name'],
                        'simbolo': row['symbol'],
                        'enlace': f"https://www.coingecko.com/es/monedas/{row['slug']}",
                        'slug': row['slug'],
                        'cmc_id': row['cmc_id'],
                        'tags': row['tags'] or [],
                        'badges': row['badges'] or [],
                        'last_values_update': row['last_values_update'],
                        'oldest_data_fetched': row['oldest_data_fetched'],
                        'scraping_status': row['scraping_status']
                    }
                    cryptocurrencies.append(crypto)
                
                logger.info(f"✅ Obtenidas {len(cryptocurrencies)} criptomonedas activas desde PostgreSQL")
                return cryptocurrencies
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo criptomonedas desde PostgreSQL: {e}")
            return []
    
    def update_range_scraping_status(self, symbol: str, status: str, last_date: str = None, 
                                   total_points: int = 0, notes: str = None):
        """Actualiza el estado de scraping por rangos"""
        if not self.connection:
            return
        
        try:
            with self.connection.cursor() as cursor:
                update_fields = []
                params = []
                
                # Campos básicos siempre actualizados
                update_fields.extend([
                    "scraping_status = %s",
                    "last_fetch_attempt = CURRENT_TIMESTAMP"
                ])
                params.extend([status])
                
                # Campos condicionales
                if last_date:
                    update_fields.append("last_values_update = %s")
                    params.append(last_date)
                
                if total_points > 0:
                    update_fields.append("total_data_points = total_data_points + %s")
                    params.append(total_points)
                
                if status == 'completed':
                    update_fields.append("fetch_error_count = 0")
                elif status == 'error':
                    update_fields.append("fetch_error_count = fetch_error_count + 1")
                
                if notes:
                    update_fields.append("scraping_notes = %s")
                    params.append(notes)
                
                # Símbolo para WHERE
                params.append(symbol)
                
                sql = f"""
                    UPDATE cryptos 
                    SET {', '.join(update_fields)}
                    WHERE symbol = %s
                """
                
                cursor.execute(sql, params)
                self.connection.commit()
                logger.debug(f"✅ Estado actualizado en BD para {symbol}: {status}")
                
        except Exception as e:
            logger.error(f"❌ Error actualizando estado en BD para {symbol}: {e}")
            if self.connection:
                self.connection.rollback()
    
    def close(self):
        """Cerrar conexión a PostgreSQL"""
        if self.connection:
            self.connection.close()
            logger.info("🔐 Conexión PostgreSQL cerrada")

class InfluxDBManager:
    """Manejador de InfluxDB para datos históricos por rangos"""
    
    def __init__(self, config: InfluxDBConfig):
        self.config = config
        self.client = None
        self.write_api = None
        
    def connect(self):
        """Conectar a InfluxDB v2"""
        try:
            url = f"http://{self.config.host}:{self.config.port}"
            
            logger.info(f"🔄 Conectando a InfluxDB v2: {url}")
            
            self.client = influxdb_client.InfluxDBClient(
                url=url,
                token=self.config.token,
                org=self.config.org
            )
            
            # Verificar conexión
            try:
                health = self.client.health()
                logger.info(f"✅ InfluxDB Status: {health.status}")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo verificar estado de InfluxDB: {e}")
            
            # Configurar write API
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            logger.info("✅ Conectado a InfluxDB v2")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error conectando a InfluxDB: {e}")
            return False
    
    def save_range_data(self, symbol: str, name: str, url_name: str, 
                       start_date: str, end_date: str, combined_data: List[Dict]) -> bool:
        """Guarda datos de rango en InfluxDB"""
        if not combined_data or not self.write_api:
            logger.warning("⚠️ No hay datos o write_api no disponible")
            return False
        
        try:
            points = []
            
            logger.info(f"🔄 Preparando {len(combined_data)} puntos para rango {start_date} → {end_date}")
            
            for data_point in combined_data:
                try:
                    # Convertir timestamp de milisegundos a datetime
                    timestamp_ms = data_point.get('timestamp', 0)
                    if timestamp_ms == 0:
                        continue
                    
                    # CoinGecko timestamps están en milisegundos
                    timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    
                    price = data_point.get('price')
                    market_cap = data_point.get('market_cap')
                    
                    # Crear punto para InfluxDB
                    point = Point("crypto_historical_ranges")
                    point.tag("symbol", symbol)
                    point.tag("name", name)
                    point.tag("url_name", url_name)
                    point.tag("source", "coingecko_ranges")
                    point.tag("range_start", start_date)
                    point.tag("range_end", end_date)
                    
                    # Añadir campos si están disponibles
                    if price is not None:
                        point.field("price", float(price))
                    
                    if market_cap is not None:
                        point.field("market_cap", float(market_cap))
                    
                    point.time(timestamp_dt)
                    points.append(point)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error procesando punto de datos para {symbol}: {e}")
                    continue
            
            if points:
                logger.info(f"🔄 Escribiendo {len(points)} puntos del rango a InfluxDB...")
                
                # Escribir en lotes para mejor rendimiento
                batch_size = 1000
                for i in range(0, len(points), batch_size):
                    batch = points[i:i + batch_size]
                    
                    self.write_api.write(
                        bucket=self.config.database,
                        org=self.config.org,
                        record=batch
                    )
                    
                    logger.debug(f"✅ Lote {i//batch_size + 1} escrito ({len(batch)} puntos)")
                
                logger.info(f"✅ Guardados {len(points)} puntos del rango {start_date} → {end_date} en InfluxDB")
                return True
            else:
                logger.warning(f"⚠️ No hay puntos válidos para el rango {start_date} → {end_date}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error guardando rango {start_date} → {end_date}: {e}")
            return False
    
    def close(self):
        """Cerrar conexión a InfluxDB"""
        if self.client:
            self.client.close()
            logger.info("🔐 Conexión InfluxDB cerrada")

class SeleniumRangeCryptoDataScraper:
    def __init__(self, 
                 values_dir: str = "values",
                 daily_dir: str = "daily_data", 
                 delay: float = 2.0,
                 headless: bool = True,
                 range_days: int = 30,
                 crypto_limit: Optional[int] = None,
                 influxdb_config: InfluxDBConfig = None,
                 postgres_config: PostgreSQLConfig = None):
        self.values_dir = values_dir
        self.daily_dir = daily_dir
        self.delay = delay
        self.range_days = range_days
        self.crypto_limit = crypto_limit
        self.driver = None
        
        # Manejadores de base de datos
        self.influxdb_config = influxdb_config or InfluxDBConfig()
        self.postgres_config = postgres_config or PostgreSQLConfig()
        self.influx_manager = InfluxDBManager(self.influxdb_config)
        self.postgres_manager = PostgreSQLManager(self.postgres_config)
        
        self.setup_driver(headless)
        
        # Crear directorio principal
        os.makedirs(self.daily_dir, exist_ok=True)
    
    def setup_driver(self, headless: bool = True):
        """
        Configura el driver de Chrome con opciones anti-detección
        """
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
        
        # Opciones anti-detección
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
        
        # Deshabilitar imágenes para velocidad
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.media_stream": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Anti-detección adicional
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        try:
            # Usar webdriver-manager para automáticamente descargar ChromeDriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Ejecutar script para ocultar webdriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
            })
            logger.info("✅ Driver de Chrome configurado correctamente")
        except Exception as e:
            logger.error(f"❌ Error al configurar Chrome driver: {e}")
            logger.error("\n🚨 CHROME NO ESTÁ INSTALADO 🚨")
            logger.error("Instala Chrome en tu sistema:")
            logger.error("Ubuntu/Debian:")
            logger.error("  wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -")
            logger.error("  sudo sh -c 'echo \"deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main\" >> /etc/apt/sources.list.d/google-chrome.list'")
            logger.error("  sudo apt update && sudo apt install google-chrome-stable")
            logger.error("CentOS/RHEL/Fedora:")
            logger.error("  sudo dnf install google-chrome-stable")
            raise
    
    def connect_databases(self):
        """Conectar a PostgreSQL e InfluxDB"""
        postgres_connected = False
        influx_connected = False
        
        try:
            postgres_connected = self.postgres_manager.connect()
        except Exception as e:
            logger.error(f"❌ No se pudo conectar a PostgreSQL: {e}")
        
        try:
            influx_connected = self.influx_manager.connect()
        except Exception as e:
            logger.error(f"❌ No se pudo conectar a InfluxDB: {e}")
        
        return postgres_connected, influx_connected
    
    def load_cryptocurrencies(self) -> List[Dict]:
        """Carga la lista de criptomonedas desde PostgreSQL con fallback a JSON"""
        try:
            cryptocurrencies = self.postgres_manager.get_active_cryptocurrencies(limit=self.crypto_limit)
            
            if not cryptocurrencies:
                logger.warning("⚠️ No se encontraron criptomonedas en PostgreSQL")
                logger.info("💡 Intentando cargar desde archivo JSON como respaldo...")
                
                # Fallback al archivo JSON si existe
                try:
                    with open("criptomonedas.json", 'r', encoding='utf-8') as f:
                        json_cryptos = json.load(f)
                        # Adaptar formato
                        cryptocurrencies = []
                        for crypto in json_cryptos[:self.crypto_limit] if self.crypto_limit else json_cryptos:
                            adapted = {
                                'nombre': crypto.get('nombre', ''),
                                'simbolo': crypto.get('simbolo', ''),
                                'enlace': crypto.get('enlace', ''),
                                'slug': crypto.get('enlace', '').split('/')[-1] if crypto.get('enlace') else '',
                                'scraping_status': 'pending'
                            }
                            cryptocurrencies.append(adapted)
                        
                        logger.info(f"✅ Cargadas {len(cryptocurrencies)} criptomonedas desde archivo JSON de respaldo")
                except FileNotFoundError:
                    logger.error("❌ No se encontró archivo JSON de respaldo")
                    return []
                except json.JSONDecodeError:
                    logger.error("❌ El archivo JSON de respaldo no tiene un formato válido")
                    return []
            
            return cryptocurrencies
            
        except Exception as e:
            logger.error(f"❌ Error cargando criptomonedas: {e}")
            return []
    
    def group_dates_into_ranges(self, dates: List[str], range_days: int = None) -> List[Tuple[str, str]]:
        """Agrupa fechas en rangos de N días"""
        if not dates:
            return []
        
        if range_days is None:
            range_days = self.range_days
        
        sorted_dates = sorted(dates)
        ranges = []
        
        i = 0
        while i < len(sorted_dates):
            start_date = sorted_dates[i]
            
            # Buscar el final del rango (hasta range_days fechas)
            end_index = min(i + range_days - 1, len(sorted_dates) - 1)
            end_date = sorted_dates[end_index]
            
            ranges.append((start_date, end_date))
            i = end_index + 1
        
        return ranges
    
    def get_last_processed_date(self, symbol: str) -> Optional[str]:
        """Obtiene la última fecha procesada de una criptomoneda"""
        crypto_data = self.load_crypto_daily_data(symbol)
        return crypto_data.get('last_processed_date')
    
    def filter_pending_dates(self, dates: List[str], last_processed_date: Optional[str]) -> List[str]:
        """Filtra fechas que aún no han sido procesadas"""
        if not last_processed_date:
            return dates
        
        # Continuar desde la siguiente fecha después de la última procesada
        return [date for date in dates if date > last_processed_date]
    
    def get_next_start_date(self, available_dates: List[str], last_processed_date: Optional[str]) -> Optional[str]:
        """Obtiene la siguiente fecha desde donde empezar a procesar"""
        if not last_processed_date:
            return min(available_dates) if available_dates else None
        
        # Buscar la primera fecha disponible después de la última procesada
        sorted_dates = sorted(available_dates)
        for date in sorted_dates:
            if date > last_processed_date:
                return date
        
        return None  # No hay fechas pendientes
    
    def extract_url_name(self, enlace: str) -> str:
        """Extrae el nombre de la URL del enlace"""
        return enlace.rstrip('/').split('/')[-1]
    
    def get_available_dates_from_values(self, symbol: str) -> Set[str]:
        """Obtiene las fechas disponibles del archivo de valores generales"""
        try:
            values_file = os.path.join(self.values_dir, f"{symbol}.json")
            if not os.path.exists(values_file):
                logger.warning(f"⚠️ No se encontró archivo de valores para {symbol}")
                return set()
            
            with open(values_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            dates = set()
            for item in data.get('data', []):
                timestamp = item.get('timestamp')
                if timestamp:
                    # Convertir timestamp a fecha
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    date_str = dt.strftime('%Y-%m-%d')
                    dates.add(date_str)
            
            logger.info(f"📅 Encontradas {len(dates)} fechas únicas para {symbol}")
            return dates
            
        except Exception as e:
            logger.error(f"❌ Error leyendo fechas de {symbol}: {e}")
            return set()
    
    def timestamp_for_date(self, date_str: str, is_end: bool = False) -> int:
        """Convierte fecha string a timestamp Unix"""
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            if is_end:
                # Final del día (23:59:59)
                dt = dt.replace(hour=23, minute=59, second=59)
            else:
                # Inicio del día (00:00:00)
                dt = dt.replace(hour=0, minute=0, second=0)
            
            return int(dt.timestamp())
        except Exception as e:
            logger.error(f"❌ Error convirtiendo fecha {date_str}: {e}")
            return 0
    
    def get_crypto_file_path(self, symbol: str) -> str:
        """Obtiene la ruta del archivo consolidado de una criptomoneda"""
        return os.path.join(self.daily_dir, f"{symbol}_daily_data.json")
    
    def load_crypto_daily_data(self, symbol: str) -> Dict:
        """Carga los datos existentes de una criptomoneda"""
        filepath = self.get_crypto_file_path(symbol)
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {
                'symbol': symbol,
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat(),
                'last_processed_date': None,
                'data': []
            }
        except Exception as e:
            logger.warning(f"⚠️ Error cargando datos de {symbol}: {e}")
            return {
                'symbol': symbol,
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat(),
                'last_processed_date': None,
                'data': []
            }
    
    def save_crypto_daily_data(self, symbol: str, crypto_data: Dict) -> bool:
        """Guarda los datos consolidados de una criptomoneda"""
        filepath = self.get_crypto_file_path(symbol)
        try:
            crypto_data['last_updated'] = datetime.now().isoformat()
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(crypto_data, f, indent=2, ensure_ascii=False)
            
            return True
        except Exception as e:
            logger.error(f"❌ Error guardando datos consolidados de {symbol}: {e}")
            return False
    
    def combine_range_data(self, price_data: List, market_cap_data: List) -> List[Dict]:
        """Combina datos de precios y capitalización por timestamp para un rango"""
        combined_data = []
        
        # Convertir market_cap_data a diccionario para búsqueda rápida
        market_cap_dict = {int(item[0]): item[1] for item in market_cap_data} if market_cap_data else {}
        
        # Procesar datos de precios y combinar con market cap
        for price_item in price_data:
            if len(price_item) >= 2:
                timestamp = int(price_item[0])
                price = price_item[1]
                market_cap = market_cap_dict.get(timestamp, None)
                
                combined_data.append({
                    'timestamp': timestamp,
                    'price': price,
                    'market_cap': market_cap
                })
        
        return combined_data
    
    def add_range_data_to_crypto(self, symbol: str, end_date: str, combined_data: List[Dict]) -> bool:
        """Añade datos de rango al archivo consolidado de una criptomoneda"""
        try:
            # Cargar datos existentes
            crypto_data = self.load_crypto_daily_data(symbol)
            
            # Añadir nuevos datos a la lista existente
            crypto_data['data'].extend(combined_data)
            
            # Actualizar la última fecha procesada con el final del rango
            crypto_data['last_processed_date'] = end_date
            crypto_data['last_updated'] = datetime.now().isoformat()
            
            # Ordenar datos por timestamp para mantener orden cronológico
            crypto_data['data'].sort(key=lambda x: x['timestamp'])
            
            # Guardar archivo consolidado
            if self.save_crypto_daily_data(symbol, crypto_data):
                logger.debug(f"📁 Respaldo JSON actualizado para {symbol} (hasta {end_date})")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Error añadiendo datos de rango de {symbol} (hasta {end_date}): {e}")
            return False
    
    def download_range_data_selenium(self, url_name: str, start_date: str, end_date: str, data_type: str) -> Optional[List]:
        """Descarga datos para un rango de fechas usando Selenium"""
        timestamp_from = self.timestamp_for_date(start_date)
        timestamp_to = self.timestamp_for_date(end_date, True)
        
        url = f"https://www.coingecko.com/{data_type}/{url_name}/usd/custom.json?from={timestamp_from}&to={timestamp_to}"
        
        try:
            logger.info(f"🔄 Descargando {data_type} para {url_name} - {start_date} a {end_date}")
            logger.debug(f"URL: {url}")
            
            # Navegar a la URL
            self.driver.get(url)
            
            # Esperar a que se cargue el contenido JSON
            wait = WebDriverWait(self.driver, 15)
            
            # Buscar el elemento <pre> que contiene el JSON
            try:
                json_element = wait.until(
                    EC.presence_of_element_located((By.TAG_NAME, "pre"))
                )
                json_text = json_element.text
            except TimeoutException:
                # Si no hay elemento <pre>, intentar obtener el texto completo de la página
                try:
                    body_element = self.driver.find_element(By.TAG_NAME, "body")
                    json_text = body_element.text
                except Exception:
                    logger.error(f"❌ No se pudo encontrar contenido JSON para {url_name} - {start_date} a {end_date}")
                    return None
            
            # Verificar si hay contenido
            if not json_text.strip():
                logger.warning(f"⚠️ Respuesta vacía para {url_name} - {start_date} a {end_date} ({data_type})")
                return None
            
            # Parsear el JSON
            try:
                data = json.loads(json_text)
                stats = data.get('stats', [])
                
                logger.info(f"✅ {len(stats)} registros para {url_name} - {start_date} a {end_date} ({data_type})")
                return stats
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ Error al parsear JSON para {url_name} - {start_date} a {end_date} ({data_type}): {e}")
                logger.debug(f"Contenido recibido: {json_text[:500]}...")
                return None
                
        except TimeoutException:
            logger.error(f"❌ Timeout al cargar datos para {url_name} - {start_date} a {end_date} ({data_type})")
            return None
        except WebDriverException as e:
            logger.error(f"❌ Error de WebDriver para {url_name} - {start_date} a {end_date} ({data_type}): {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Error inesperado para {url_name} - {start_date} a {end_date} ({data_type}): {e}")
            return None
    
    def random_delay(self, min_delay: float = None, max_delay: float = None):
        """Delay aleatorio entre peticiones"""
        if min_delay is None:
            min_delay = self.delay
        if max_delay is None:
            max_delay = self.delay * 1.5
        
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def process_cryptocurrency_daily(self, crypto: Dict) -> Dict:
        """Procesa una criptomoneda por rangos de días con integración BD"""
        symbol = crypto.get('simbolo', '').upper()
        enlace = crypto.get('enlace', '')
        nombre = crypto.get('nombre', '')
        
        if not symbol or not enlace:
            logger.warning(f"⚠️ Datos incompletos para {nombre}")
            return {'symbol': symbol, 'success': 0, 'failed': 0, 'skipped': 0}
        
        logger.info(f"\n📊 Procesando {nombre} ({symbol})")
        
        # Marcar como en progreso en PostgreSQL
        self.postgres_manager.update_range_scraping_status(
            symbol, 'in_progress', 
            notes=f'Iniciando descarga de rangos históricos'
        )
        
        # Extraer nombre de URL
        url_name = self.extract_url_name(enlace)
        logger.info(f"🔍 URL name: {url_name}")
        
        # Obtener fechas disponibles del archivo general
        available_dates = self.get_available_dates_from_values(symbol)
        if not available_dates:
            logger.warning(f"⚠️ No hay fechas disponibles para {symbol}")
            self.postgres_manager.update_range_scraping_status(
                symbol, 'error', 
                notes='No se encontraron fechas disponibles en archivo de valores'
            )
            return {'symbol': symbol, 'success': 0, 'failed': 0, 'skipped': 0}
        
        # Verificar última fecha procesada y determinar desde dónde continuar
        last_processed = self.get_last_processed_date(symbol)
        sorted_dates = sorted(list(available_dates))
        
        if last_processed:
            logger.info(f"📅 Última fecha procesada: {last_processed}")
            
            # Obtener la siguiente fecha desde donde continuar
            next_start_date = self.get_next_start_date(sorted_dates, last_processed)
            
            if not next_start_date:
                logger.info(f"✅ {symbol} está completamente actualizado (hasta {last_processed})")
                self.postgres_manager.update_range_scraping_status(
                    symbol, 'completed', last_processed,
                    notes='Crypto completamente actualizada - no hay fechas pendientes'
                )
                return {'symbol': symbol, 'success': 0, 'failed': 0, 'skipped': len(available_dates)}
            
            # Filtrar fechas pendientes
            pending_dates = self.filter_pending_dates(sorted_dates, last_processed)
            logger.info(f"🔄 Continuando desde {next_start_date}")
            logger.info(f"📅 Fechas pendientes: {len(pending_dates)} (de {next_start_date} a {max(pending_dates)})")
        else:
            pending_dates = sorted_dates
            logger.info(f"🆕 Primera ejecución - procesando todas las fechas")
            logger.info(f"📅 Total de fechas: {len(pending_dates)} (de {min(pending_dates)} a {max(pending_dates)})")
        
        if not pending_dates:
            logger.info(f"✅ No hay fechas nuevas para procesar en {symbol}")
            self.postgres_manager.update_range_scraping_status(
                symbol, 'completed', last_processed,
                notes='No hay fechas pendientes para procesar'
            )
            return {'symbol': symbol, 'success': 0, 'failed': 0, 'skipped': len(available_dates)}
        
        # Agrupar fechas en rangos
        date_ranges = self.group_dates_into_ranges(pending_dates, self.range_days)
        logger.info(f"📦 Agrupado en {len(date_ranges)} rangos de hasta {self.range_days} días cada uno")
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        total_points_saved = 0
        
        for i, (start_date, end_date) in enumerate(date_ranges, 1):
            try:
                logger.info(f"📦 [{i}/{len(date_ranges)}] Rango {symbol}: {start_date} → {end_date}")
                
                # Descargar datos de precios para el rango
                price_data = self.download_range_data_selenium(url_name, start_date, end_date, 'price_charts')
                if not price_data:
                    logger.error(f"❌ No se pudieron obtener datos de precios para {symbol} - {start_date} a {end_date}")
                    failed_count += 1
                    continue
                
                # Delay entre peticiones
                self.random_delay()
                
                # Descargar datos de capitalización de mercado para el rango
                market_cap_data = self.download_range_data_selenium(url_name, start_date, end_date, 'market_cap')
                if not market_cap_data:
                    logger.warning(f"⚠️ No se pudieron obtener datos de capitalización para {symbol} - {start_date} a {end_date}")
                    market_cap_data = []
                
                # Combinar datos
                combined_data = self.combine_range_data(price_data, market_cap_data)
                
                if combined_data:
                    # Guardar en InfluxDB
                    influx_success = False
                    if self.influx_manager.write_api:
                        influx_success = self.influx_manager.save_range_data(
                            symbol, nombre, url_name, start_date, end_date, combined_data
                        )
                    
                    # Guardar respaldo JSON
                    backup_success = self.add_range_data_to_crypto(symbol, end_date, combined_data)
                    
                    if influx_success or backup_success:
                        success_count += 1
                        total_points_saved += len(combined_data)
                        logger.info(f"✅ Rango completado: {symbol} hasta {end_date} ({len(combined_data)} puntos)")
                        
                        # Actualizar progreso en PostgreSQL
                        self.postgres_manager.update_range_scraping_status(
                            symbol, 'in_progress', end_date, len(combined_data),
                            notes=f'Procesando rango {i}/{len(date_ranges)}: {start_date} → {end_date}'
                        )
                    else:
                        failed_count += 1
                        logger.error(f"❌ Error guardando datos de {symbol} - {start_date} a {end_date}")
                else:
                    failed_count += 1
                    logger.error(f"❌ No se generaron datos combinados para {symbol} - {start_date} a {end_date}")
                
                # Delay entre rangos
                self.random_delay()
                
            except Exception as e:
                logger.error(f"❌ Error procesando {symbol} - {start_date} a {end_date}: {e}")
                failed_count += 1
        
        # Actualizar estado final en PostgreSQL
        if failed_count == 0 and success_count > 0:
            final_date = max(pending_dates) if pending_dates else last_processed
            self.postgres_manager.update_range_scraping_status(
                symbol, 'completed', final_date, total_points_saved,
                notes=f'Descarga completada exitosamente. {success_count} rangos procesados, {total_points_saved} puntos totales'
            )
        elif success_count > 0:
            final_date = max([end for _, end in date_ranges[:success_count]]) if success_count > 0 else last_processed
            self.postgres_manager.update_range_scraping_status(
                symbol, 'error', final_date, total_points_saved,
                notes=f'Descarga parcial. {success_count} rangos exitosos, {failed_count} fallidos'
            )
        else:
            self.postgres_manager.update_range_scraping_status(
                symbol, 'error', last_processed, 0,
                notes=f'Descarga falló completamente. {failed_count} rangos fallidos'
            )
        
        return {
            'symbol': symbol,
            'success': success_count,
            'failed': failed_count,
            'skipped': skipped_count,
            'total_points': total_points_saved
        }
    
    def run(self) -> None:
        """Ejecuta el proceso completo con integración PostgreSQL e InfluxDB"""
        
        logger.info(f"🚀 Iniciando descarga por rangos desde PostgreSQL")
        
        # Conectar a bases de datos
        postgres_connected, influx_connected = self.connect_databases()
        
        if not postgres_connected:
            logger.error("❌ No se pudo conectar a PostgreSQL - Proceso abortado")
            return
        
        if influx_connected:
            logger.info("✅ InfluxDB conectado - Los datos se guardarán en InfluxDB")
        else:
            logger.warning("⚠️ InfluxDB no disponible - Solo se guardarán respaldos JSON y estados en PostgreSQL")
        
        # Cargar criptomonedas desde PostgreSQL
        cryptocurrencies = self.load_cryptocurrencies()
        
        if not cryptocurrencies:
            logger.error("❌ No se encontraron criptomonedas para procesar")
            return
        
        logger.info(f"📊 Procesando {len(cryptocurrencies)} criptomonedas desde PostgreSQL")
        if self.crypto_limit:
            logger.info(f"🔢 Límite aplicado: {self.crypto_limit} criptomonedas")
        
        logger.info(f"📦 Tamaño de rango: {self.range_days} días por rango")
        logger.info(f"📁 Los archivos de respaldo se guardarán en: {os.path.abspath(self.daily_dir)}")
        
        total_stats = {
            'processed': 0,
            'total_success': 0,
            'total_failed': 0,
            'total_skipped': 0,
            'total_points': 0
        }
        
        try:
            for i, crypto in enumerate(cryptocurrencies, 1):
                logger.info(f"\n[{i}/{len(cryptocurrencies)}] ======================")
                
                try:
                    result = self.process_cryptocurrency_daily(crypto)
                    
                    total_stats['processed'] += 1
                    total_stats['total_success'] += result['success']
                    total_stats['total_failed'] += result['failed']
                    total_stats['total_skipped'] += result['skipped']
                    total_stats['total_points'] += result.get('total_points', 0)
                    
                    logger.info(f"Resultado {result['symbol']}: ✅{result['success']} ❌{result['failed']} ⏭️{result['skipped']} 📊{result.get('total_points', 0)}")
                    
                except Exception as e:
                    logger.error(f"❌ Error procesando {crypto.get('nombre', 'Desconocido')}: {e}")
                    total_stats['total_failed'] += 1
                
                # Pausa entre criptomonedas
                if i < len(cryptocurrencies):
                    self.random_delay(self.delay * 2, self.delay * 3)
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Proceso interrumpido por el usuario")
        
        logger.info(f"\n\n📈 === RESUMEN FINAL ===")
        logger.info(f"🔢 Criptomonedas procesadas: {total_stats['processed']}")
        logger.info(f"✅ Rangos exitosos: {total_stats['total_success']}")
        logger.info(f"❌ Rangos fallidos: {total_stats['total_failed']}")
        logger.info(f"⏭️ Rangos saltados: {total_stats['total_skipped']}")
        logger.info(f"📊 Total de puntos guardados: {total_stats['total_points']}")
        logger.info(f"📊 Estados actualizados en PostgreSQL")
        
        if influx_connected:
            logger.info(f"💾 Datos históricos guardados en InfluxDB (bucket: {self.influxdb_config.database})")
        
        logger.info(f"📁 Respaldos JSON en: {os.path.abspath(self.daily_dir)}")
    
    def close(self):
        """Cierra el driver y conexiones"""
        if self.driver:
            self.driver.quit()
            logger.info("🔐 Driver cerrado")
        
        if self.influx_manager:
            self.influx_manager.close()
        
        if self.postgres_manager:
            self.postgres_manager.close()


def main():
    """Función principal"""
    scraper = None
    
    try:
        print("🚀 === Descargador de Rangos con PostgreSQL + InfluxDB ===")
        print("Formato: [{timestamp, price, market_cap}, ...] con tracking de progreso")
        print("Instalando dependencias:")
        print("pip install selenium webdriver-manager influxdb-client psycopg2-binary")
        print("ChromeDriver se descarga automáticamente\n")
        
        # Cargar variables de entorno
        load_env_file()
        
        # Configuración
        headless = input("¿Ejecutar en modo headless? (s/N): ").lower().startswith('s')
        delay = float(input("Delay entre peticiones en segundos (recomendado: 2-4): ") or "2.5")
        range_days = int(input("Días por rango (recomendado: 30): ") or "30")
        
        # Límite de criptomonedas (opcional)
        limit_input = input("¿Límite de criptomonedas a procesar? (Enter para todas): ").strip()
        crypto_limit = int(limit_input) if limit_input.isdigit() else None
        
        # Crear configuraciones
        try:
            postgres_config = PostgreSQLConfig()
        except Exception as e:
            logger.error(f"❌ Error en configuración de PostgreSQL: {e}")
            return
        
        try:
            influxdb_config = InfluxDBConfig()
        except ValueError as e:
            logger.error(f"❌ Error en configuración de InfluxDB: {e}")
            logger.info("Continuando solo con PostgreSQL y respaldos JSON...")
            influxdb_config = None
        
        # Crear scraper
        scraper = SeleniumRangeCryptoDataScraper(
            values_dir="values",
            daily_dir="daily_data",
            delay=delay,
            headless=headless,
            range_days=range_days,
            crypto_limit=crypto_limit,
            influxdb_config=influxdb_config,
            postgres_config=postgres_config
        )
        
        # Ejecutar scraping
        scraper.run()
        
    except KeyboardInterrupt:
        print("\n🛑 Proceso interrumpido por el usuario")
    except Exception as e:
        logger.error(f"❌ Error en main: {e}")
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    main()