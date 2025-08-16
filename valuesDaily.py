#!/usr/bin/env python3
"""
Script para obtener datos históricos de precios y capitalización de mercado de criptomonedas 
desde CoinGecko usando Selenium. Obtiene la lista de criptomonedas desde PostgreSQL y 
guarda los datos históricos en InfluxDB.
"""

import json
import os
import time
import random
import logging
from urllib.parse import urlparse
from typing import Dict, List, Optional
from datetime import datetime, timezone
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
        logging.FileHandler('coingecko_historical.log'),
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
    """Manejador de PostgreSQL para obtener lista de criptomonedas"""
    
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
                # Consulta para obtener criptomonedas activas
                sql = """
                SELECT 
                    name,
                    symbol,
                    slug,
                    cmc_id,
                    is_active,
                    tags,
                    badges
                FROM cryptos 
                WHERE is_active = true 
                AND slug IS NOT NULL 
                AND slug != ''
                ORDER BY cmc_rank ASC NULLS LAST, symbol ASC
                """
                
                if limit:
                    sql += f" LIMIT {limit}"
                
                cursor.execute(sql)
                rows = cursor.fetchall()
                
                # Convertir a formato compatible con el script original
                cryptocurrencies = []
                for row in rows:
                    crypto = {
                        'nombre': row['name'],
                        'simbolo': row['symbol'],
                        'enlace': f"https://www.coingecko.com/es/monedas/{row['slug']}",
                        'slug': row['slug'],
                        'cmc_id': row['cmc_id'],
                        'tags': row['tags'] or [],
                        'badges': row['badges'] or []
                    }
                    cryptocurrencies.append(crypto)
                
                logger.info(f"✅ Obtenidas {len(cryptocurrencies)} criptomonedas activas desde PostgreSQL")
                return cryptocurrencies
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo criptomonedas desde PostgreSQL: {e}")
            return []
    
    def update_historical_data_status(self, symbol: str, success: bool, total_points: int = 0):
        """Actualiza el estado de descarga de datos históricos"""
        if not self.connection:
            return
        
        try:
            with self.connection.cursor() as cursor:
                if success:
                    cursor.execute("""
                        UPDATE cryptos 
                        SET 
                            scraping_status = 'completed',
                            total_data_points = %s,
                            last_fetch_attempt = CURRENT_TIMESTAMP,
                            fetch_error_count = 0,
                            scraping_notes = 'Datos históricos obtenidos exitosamente desde CoinGecko'
                        WHERE symbol = %s
                    """, (total_points, symbol))
                else:
                    cursor.execute("""
                        UPDATE cryptos 
                        SET 
                            scraping_status = 'error',
                            last_fetch_attempt = CURRENT_TIMESTAMP,
                            fetch_error_count = fetch_error_count + 1,
                            scraping_notes = 'Error obteniendo datos históricos desde CoinGecko'
                        WHERE symbol = %s
                    """, (symbol,))
                
                self.connection.commit()
                logger.debug(f"✅ Estado actualizado en BD para {symbol}")
                
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
    """Manejador de InfluxDB para datos históricos"""
    
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
                
                # Verificar organización
                orgs_api = self.client.organizations_api()
                orgs = orgs_api.find_organizations()
                org_names = [org.name for org in orgs]
                
                if self.config.org not in org_names:
                    logger.warning(f"⚠️ Organización '{self.config.org}' no encontrada. Disponibles: {org_names}")
                    if org_names:
                        self.config.org = org_names[0]
                        logger.info(f"🔄 Usando organización: '{self.config.org}'")
                        self.client.close()
                        self.client = influxdb_client.InfluxDBClient(url=url, token=self.config.token, org=self.config.org)
                else:
                    logger.info(f"✅ Organización '{self.config.org}' encontrada")
                
            except Exception as e:
                logger.warning(f"⚠️ No se pudo verificar organizaciones: {e}")
            
            # Configurar write API
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            logger.info("✅ Conectado a InfluxDB v2")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error conectando a InfluxDB: {e}")
            return False
    
    def save_historical_data(self, symbol: str, name: str, url_name: str, combined_data: List[Dict]) -> bool:
        """Guarda datos históricos en InfluxDB"""
        if not combined_data or not self.write_api:
            logger.warning("⚠️ No hay datos o write_api no disponible")
            return False
        
        try:
            points = []
            
            logger.info(f"🔄 Preparando {len(combined_data)} puntos históricos para {symbol}...")
            
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
                    point = Point("crypto_historical")
                    point.tag("symbol", symbol)
                    point.tag("name", name)
                    point.tag("url_name", url_name)
                    point.tag("source", "coingecko_historical")
                    
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
                logger.info(f"🔄 Escribiendo {len(points)} puntos históricos a InfluxDB para {symbol}...")
                
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
                
                logger.info(f"✅ Guardados {len(points)} puntos históricos para {symbol} en InfluxDB")
                return True
            else:
                logger.warning(f"⚠️ No hay puntos válidos para insertar para {symbol}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error guardando datos históricos para {symbol}: {e}")
            return False
    
    def close(self):
        """Cerrar conexión a InfluxDB"""
        if self.client:
            self.client.close()
            logger.info("🔐 Conexión InfluxDB cerrada")

class SeleniumCryptoDataScraper:
    def __init__(self, output_dir: str = "values", headless: bool = True, delay: float = 2.0, 
                 influxdb_config: InfluxDBConfig = None, postgres_config: PostgreSQLConfig = None,
                 crypto_limit: Optional[int] = None):
        self.output_dir = output_dir
        self.delay = delay
        self.driver = None
        self.crypto_limit = crypto_limit
        
        # Manejadores de base de datos
        self.influxdb_config = influxdb_config or InfluxDBConfig()
        self.postgres_config = postgres_config or PostgreSQLConfig()
        self.influx_manager = InfluxDBManager(self.influxdb_config)
        self.postgres_manager = PostgreSQLManager(self.postgres_config)
        
        self.setup_driver(headless)
        
        # Crear directorio de salida si no existe (para respaldos JSON)
        os.makedirs(self.output_dir, exist_ok=True)
    
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
            logger.error("Intenta instalar/actualizar: pip install webdriver-manager")
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
    
    def random_delay(self, min_delay: float = 1.0, max_delay: float = 3.0):
        """
        Implementa un delay aleatorio entre requests
        """
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def load_cryptocurrencies(self) -> List[Dict]:
        """Carga la lista de criptomonedas desde PostgreSQL"""
        try:
            cryptocurrencies = self.postgres_manager.get_active_cryptocurrencies(limit=self.crypto_limit)
            
            if not cryptocurrencies:
                logger.warning("⚠️ No se encontraron criptomonedas en PostgreSQL")
                logger.info("💡 Intentando cargar desde archivo JSON como respaldo...")
                
                # Fallback al archivo JSON si existe
                try:
                    with open("criptomonedas.json", 'r', encoding='utf-8') as f:
                        cryptocurrencies = json.load(f)
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
    
    def extract_url_name(self, enlace: str) -> str:
        """Extrae el nombre de la URL del enlace (último segmento después del último '/')"""
        return enlace.rstrip('/').split('/')[-1]
    
    def get_json_data(self, url: str, data_type: str, crypto_name: str) -> Optional[List]:
        """
        Obtiene datos JSON usando Selenium
        data_type: 'price_charts' o 'market_cap'
        """
        try:
            logger.info(f"🔄 Obteniendo {data_type} para {crypto_name} desde: {url}")
            
            # Navegar a la URL
            self.driver.get(url)
            
            # Esperar a que se cargue el contenido JSON
            wait = WebDriverWait(self.driver, 15)
            
            # Buscar el elemento <pre> que contiene el JSON (típico en respuestas JSON del navegador)
            try:
                json_element = wait.until(
                    EC.presence_of_element_located((By.TAG_NAME, "pre"))
                )
                json_text = json_element.text
            except TimeoutException:
                # Si no hay elemento <pre>, intentar obtener el texto completo de la página
                json_text = self.driver.find_element(By.TAG_NAME, "body").text
            
            # Parsear el JSON
            if json_text.strip():
                try:
                    data = json.loads(json_text)
                    stats = data.get('stats', [])
                    logger.info(f"✅ Obtenidos {len(stats)} puntos de datos para {crypto_name} ({data_type})")
                    return stats
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Error al parsear JSON para {crypto_name} ({data_type}): {e}")
                    return None
            else:
                logger.warning(f"⚠️ Respuesta vacía para {crypto_name} ({data_type})")
                return None
                
        except TimeoutException:
            logger.error(f"❌ Timeout al cargar datos para {crypto_name} ({data_type})")
            return None
        except WebDriverException as e:
            logger.error(f"❌ Error de WebDriver para {crypto_name} ({data_type}): {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Error inesperado para {crypto_name} ({data_type}): {e}")
            return None
    
    def get_crypto_data(self, url_name: str, data_type: str) -> Optional[List]:
        """
        Obtiene datos de precios o capitalización de mercado
        data_type: 'price_charts' o 'market_cap'
        """
        url = f"https://www.coingecko.com/{data_type}/{url_name}/usd/max.json"
        return self.get_json_data(url, data_type, url_name)
    
    def combine_data(self, price_data: List, market_cap_data: List) -> List[Dict]:
        """Combina datos de precios y capitalización de mercado según timestamp"""
        combined_data = []
        
        # Convertir market_cap_data a diccionario para búsqueda rápida
        market_cap_dict = {int(item[0]): item[1] for item in market_cap_data} if market_cap_data else {}
        
        for price_item in price_data:
            if len(price_item) >= 2:
                timestamp = int(price_item[0])
                price = price_item[1]
                market_cap = market_cap_dict.get(timestamp)
                
                combined_data.append({
                    'timestamp': timestamp,
                    'price': price,
                    'market_cap': market_cap
                })
        
        return combined_data
    
    def save_crypto_data_backup(self, symbol: str, combined_data: List[Dict]) -> bool:
        """Guarda los datos combinados en un archivo JSON como respaldo"""
        filename = f"{symbol}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({
                    'symbol': symbol,
                    'data': combined_data,
                    'total_records': len(combined_data)
                }, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"📁 Respaldo JSON guardado para {symbol} ({len(combined_data)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error al guardar respaldo JSON para {symbol}: {e}")
            return False
    
    def process_cryptocurrency(self, crypto: Dict) -> bool:
        """Procesa una criptomoneda individual"""
        symbol = crypto.get('simbolo', '').upper()
        enlace = crypto.get('enlace', '')
        nombre = crypto.get('nombre', '')
        
        if not symbol or not enlace:
            logger.warning(f"⚠️ Datos incompletos para {nombre}")
            return False
        
        logger.info(f"\n📊 Procesando {nombre} ({symbol})")
        
        # Extraer nombre de URL
        url_name = self.extract_url_name(enlace)
        logger.info(f"🔍 Nombre URL extraído: {url_name}")
        
        try:
            # Obtener datos de precios
            price_data = self.get_crypto_data(url_name, 'price_charts')
            if not price_data:
                logger.error(f"❌ No se pudieron obtener datos de precios para {symbol}")
                self.postgres_manager.update_historical_data_status(symbol, False)
                return False
            
            # Delay entre peticiones
            self.random_delay(self.delay, self.delay * 1.5)
            
            # Obtener datos de capitalización de mercado
            market_cap_data = self.get_crypto_data(url_name, 'market_cap')
            if not market_cap_data:
                logger.warning(f"⚠️ No se pudieron obtener datos de capitalización para {symbol}")
                market_cap_data = []
            
            # Combinar datos
            combined_data = self.combine_data(price_data, market_cap_data)
            
            if not combined_data:
                logger.error(f"❌ No hay datos combinados para {symbol}")
                self.postgres_manager.update_historical_data_status(symbol, False)
                return False
            
            # Guardar en InfluxDB
            influx_success = False
            if self.influx_manager.write_api:
                influx_success = self.influx_manager.save_historical_data(symbol, nombre, url_name, combined_data)
            
            # Guardar respaldo JSON
            backup_success = self.save_crypto_data_backup(symbol, combined_data)
            
            # Actualizar estado en PostgreSQL
            if influx_success:
                self.postgres_manager.update_historical_data_status(symbol, True, len(combined_data))
                logger.info(f"✅ {symbol}: Guardado en InfluxDB ({len(combined_data)} puntos)")
            else:
                self.postgres_manager.update_historical_data_status(symbol, False)
                logger.warning(f"⚠️ {symbol}: No se pudo guardar en InfluxDB")
            
            return influx_success or backup_success
            
        except Exception as e:
            logger.error(f"❌ Error procesando {symbol}: {e}")
            self.postgres_manager.update_historical_data_status(symbol, False)
            return False
    
    def run(self) -> None:
        """Ejecuta el proceso completo para todas las criptomonedas"""
        
        logger.info(f"🚀 Iniciando descarga de datos históricos desde PostgreSQL")
        
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
        
        logger.info(f"📁 Los archivos de respaldo se guardarán en: {os.path.abspath(self.output_dir)}")
        
        successful = 0
        failed = 0
        
        try:
            for i, crypto in enumerate(cryptocurrencies, 1):
                print(f"\n[{i}/{len(cryptocurrencies)}] ", end="")
                
                try:
                    if self.process_cryptocurrency(crypto):
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"❌ Error procesando {crypto.get('nombre', 'Desconocido')}: {e}")
                    failed += 1
                
                # Pausa entre criptomonedas para evitar rate limiting
                if i < len(cryptocurrencies):
                    self.random_delay(self.delay * 1.5, self.delay * 2.5)
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Proceso interrumpido por el usuario")
        except Exception as e:
            logger.error(f"❌ Error inesperado durante el procesamiento: {e}")
        
        logger.info(f"\n\n📈 Resumen final:")
        logger.info(f"✅ Exitosos: {successful}")
        logger.info(f"❌ Fallidos: {failed}")
        logger.info(f"📊 Estados actualizados en PostgreSQL")
        
        if influx_connected:
            logger.info(f"💾 Datos históricos guardados en InfluxDB (bucket: {self.influxdb_config.database})")
        
        logger.info(f"📁 Respaldos JSON en: {os.path.abspath(self.output_dir)}")
    
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
        print("🚀 === CoinGecko Historical Data Scraper con PostgreSQL + InfluxDB ===")
        print("Instalando dependencias:")
        print("pip install selenium webdriver-manager influxdb-client psycopg2-binary")
        print("ChromeDriver se descarga automáticamente\n")
        
        # Cargar variables de entorno
        load_env_file()
        
        # Configuración
        headless = input("¿Ejecutar en modo headless? (s/N): ").lower().startswith('s')
        delay = float(input("Delay entre peticiones en segundos (recomendado: 2-4): ") or "2.5")
        
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
        scraper = SeleniumCryptoDataScraper(
            output_dir="values",
            headless=headless,
            delay=delay,
            influxdb_config=influxdb_config,
            postgres_config=postgres_config,
            crypto_limit=crypto_limit
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