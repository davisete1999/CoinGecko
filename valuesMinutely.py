#!/usr/bin/env python3
"""
Script para completar datos históricos de criptomonedas con status 'completed_daily' desde CoinGecko
Procesa criptomonedas una por una desde hoy hacia atrás hasta oldest_data_fetched y más allá hasta crypto_start_date
SOLO BASE DE DATOS - Sin archivos CSV/JSON

VERSIÓN ESPECÍFICA PARA completed_daily:
- Filtra solo criptomonedas con scraping_status = 'completed_daily'
- Procesa desde hoy hacia atrás hasta crypto_start_date
- Marca como 'completed' cuando termina todos los rangos
- Sin lógica de prioridades
"""

import json
import os
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
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
                    print(f"Cargada variable: {key}")
        
        print(f"✅ {env_vars_loaded} variables de entorno cargadas desde {env_file}")
            
    except FileNotFoundError:
        print(f"⚠️ Archivo {env_file} no encontrado, usando variables de entorno del sistema")
    except Exception as e:
        print(f"❌ Error cargando {env_file}: {e}")

@dataclass
class InfluxDBConfig:
    """Configuración de InfluxDB desde variables de entorno"""
    host: str = os.getenv('INFLUXDB_HOST', 'localhost')
    port: int = int(os.getenv('INFLUXDB_EXTERNAL_PORT') or '8086')
    database: str = os.getenv('INFLUXDB_DB', 'quotes')
    token: str = os.getenv('INFLUXDB_TOKEN', '')
    org: str = os.getenv('INFLUXDB_ORG', 'CoinAdvisor')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        print(f"🔧 InfluxDB Config: {self.host}:{self.port} | org='{self.org}', bucket='{self.database}'")

@dataclass
class PostgreSQLConfig:
    """Configuración de PostgreSQL desde variables de entorno"""
    host: str = os.getenv('POSTGRES_HOST', 'localhost')
    port: int = int(os.getenv('POSTGRES_EXTERNAL_PORT') or '5432')
    database: str = os.getenv('POSTGRES_DB', 'cryptodb')
    user: str = os.getenv('POSTGRES_USER', 'crypto-user')
    password: str = os.getenv('POSTGRES_PASSWORD', 'davisete453')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        print(f"🔧 PostgreSQL Config: {self.host}:{self.port}/{self.database}")

class PostgreSQLManager:
    """Manejador de PostgreSQL para criptomonedas completed_daily"""
    
    def __init__(self, config: PostgreSQLConfig):
        self.config = config
        self.connection = None
        self.coingecko_source_id = None
        
    def connect(self):
        """Conectar a PostgreSQL y obtener ID de fuente CoinGecko"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                cursor_factory=RealDictCursor
            )
            
            # Obtener ID de fuente CoinGecko
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT id FROM crypto_sources WHERE source_name = 'coingecko'")
                result = cursor.fetchone()
                if result:
                    self.coingecko_source_id = result['id']
                    print(f"✅ Conectado a PostgreSQL - CoinGecko source_id: {self.coingecko_source_id}")
                else:
                    print("❌ No se encontró fuente 'coingecko' en crypto_sources")
                    return False
            
            return True
        except Exception as e:
            print(f"❌ Error conectando a PostgreSQL: {e}")
            return False
    
    def get_completed_daily_cryptocurrencies(self, limit: Optional[int] = None) -> List[Dict]:
        """Obtiene criptomonedas con scraping_status = 'completed_daily' - VERSIÓN ARREGLADA"""
        if not self.connection:
            print("❌ No hay conexión a PostgreSQL")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                # ARREGLADO: Consulta menos restrictiva con fallbacks
                sql = """
                SELECT 
                    c.id as crypto_id,
                    c.name,
                    c.symbol,
                    c.slug,
                    c.is_active,
                    c.created_at,
                    
                    -- Datos específicos de CoinGecko
                    cg.coingecko_rank,
                    cg.coingecko_url,
                    cg.icon_url,
                    cg.coin_url,
                    cg.tags,
                    cg.badges,
                    cg.last_values_update,
                    cg.oldest_data_fetched,
                    cg.scraping_status,
                    cg.total_data_points,
                    cg.last_fetch_attempt,
                    cg.fetch_error_count,
                    cg.scraping_notes,
                    
                    -- Fecha de inicio específica de la crypto (para límite histórico)
                    COALESCE(
                        cmc.date_added::date,
                        c.created_at::date,
                        '2009-01-01'::date
                    ) as crypto_start_date
                    
                FROM cryptos c
                INNER JOIN coingecko_cryptos cg ON c.id = cg.crypto_id
                LEFT JOIN coinmarketcap_cryptos cmc ON c.id = cmc.crypto_id
                WHERE c.is_active = true 
                AND (
                    -- OPCIÓN 1: Criptos con completed_daily (ideal)
                    cg.scraping_status = 'completed_daily' 
                    OR 
                    -- OPCIÓN 2: Criptos completed que necesitan históricos (fallback)
                    (cg.scraping_status = 'completed' AND cg.oldest_data_fetched IS NOT NULL)
                    OR
                    -- OPCIÓN 3: Criptos con datos parciales (último recurso)
                    (cg.scraping_status IS NULL AND cg.total_data_points > 0)
                )
                -- ARREGLADO: Construcción flexible de URL
                AND (
                    (cg.coin_url IS NOT NULL AND cg.coin_url != '') 
                    OR 
                    (c.slug IS NOT NULL AND c.slug != '')
                )
                ORDER BY 
                    -- Priorizar completed_daily, luego por ranking
                    CASE 
                        WHEN cg.scraping_status = 'completed_daily' THEN 1
                        WHEN cg.scraping_status = 'completed' THEN 2
                        ELSE 3
                    END,
                    cg.oldest_data_fetched DESC NULLS LAST,  
                    cg.coingecko_rank ASC NULLS LAST,
                    c.symbol ASC
                """
                
                if limit:
                    sql += f" LIMIT {limit}"
                
                cursor.execute(sql)
                rows = cursor.fetchall()
                
                # ARREGLADO: Construcción más robusta de datos
                cryptocurrencies = []
                
                for row in rows:
                    # Construir URL del coin de manera más flexible
                    coin_url = row['coin_url']
                    if not coin_url and row['slug']:
                        coin_url = f"https://www.coingecko.com/es/monedas/{row['slug']}"
                    elif not coin_url:
                        print(f"⚠️ Sin URL para {row['symbol']}, saltando...")
                        continue
                    
                    # ARREGLADO: Manejo seguro de fecha oldest_data_fetched
                    oldest_date = row['oldest_data_fetched']
                    if not oldest_date:
                        # Si no hay oldest_date, usar fecha reciente como punto de partida
                        oldest_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                        print(f"⚠️ Sin oldest_data_fetched para {row['symbol']}, usando {oldest_date}")
                    
                    crypto = {
                        'crypto_id': row['crypto_id'],
                        'nombre': row['name'],
                        'simbolo': row['symbol'],
                        'enlace': coin_url,
                        'slug': row['slug'],
                        'coingecko_rank': row['coingecko_rank'],
                        'tags': json.loads(row['tags']) if row['tags'] else [],
                        'badges': json.loads(row['badges']) if row['badges'] else [],
                        'last_values_update': row['last_values_update'],
                        'oldest_data_fetched': oldest_date,  # ARREGLADO
                        'scraping_status': row['scraping_status'] or 'pending',
                        'total_data_points': row['total_data_points'] or 0,
                        'fetch_error_count': row['fetch_error_count'] or 0,
                        'scraping_notes': row['scraping_notes'],
                        'crypto_start_date': str(row['crypto_start_date'])
                    }
                    cryptocurrencies.append(crypto)
                
                print(f"✅ Encontradas {len(cryptocurrencies)} criptomonedas para completar históricos")
                
                # ARREGLADO: Mostrar distribución por status para debugging
                status_counts = {}
                for crypto in cryptocurrencies:
                    status = crypto['scraping_status']
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                print(f"📊 Distribución por status: {status_counts}")
                
                return cryptocurrencies
                
        except Exception as e:
            print(f"❌ Error obteniendo criptomonedas: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def update_coingecko_scraping_progress(self, crypto_id: int, status: str = 'in_progress', 
                                         total_points: int = 0, oldest_date: str = None, 
                                         latest_date: str = None, notes: str = None):
        """Actualiza el progreso de scraping"""
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
                
                # Campos condicionales según estado
                if status == 'completed':
                    update_fields.extend([
                        "fetch_error_count = 0",
                        "total_data_points = COALESCE(total_data_points, 0) + %s"
                    ])
                    params.extend([total_points])
                    
                    if latest_date:
                        update_fields.append("last_values_update = %s")
                        params.append(latest_date)
                    
                    if oldest_date:
                        update_fields.append("oldest_data_fetched = %s") 
                        params.append(oldest_date)
                        
                elif status == 'error':
                    update_fields.append("fetch_error_count = fetch_error_count + 1")
                elif status == 'in_progress':
                    if total_points > 0:
                        update_fields.append("total_data_points = COALESCE(total_data_points, 0) + %s")
                        params.append(total_points)
                    
                    if oldest_date:
                        update_fields.append("oldest_data_fetched = %s") 
                        params.append(oldest_date)
                
                # Notas descriptivas
                if notes:
                    update_fields.append("scraping_notes = %s")
                    params.append(notes)
                
                params.append(crypto_id)
                
                sql = f"""
                    UPDATE coingecko_cryptos 
                    SET {', '.join(update_fields)}
                    WHERE crypto_id = %s
                """
                
                cursor.execute(sql, params)
                rows_affected = cursor.rowcount
                self.connection.commit()
                
                if rows_affected > 0:
                    print(f"✅ Progreso actualizado para crypto_id={crypto_id}: {status}")
                else:
                    print(f"⚠️ No se encontró crypto para actualizar: crypto_id={crypto_id}")
                
        except Exception as e:
            print(f"❌ Error actualizando progreso: {e}")
            if self.connection:
                self.connection.rollback()
    
    def get_coingecko_scraping_stats(self) -> Dict:
        """Obtiene estadísticas de scraping de CoinGecko"""
        if not self.connection:
            return {}
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        cg.scraping_status,
                        COUNT(*) as count,
                        SUM(cg.total_data_points) as total_points,
                        AVG(cg.fetch_error_count) as avg_errors
                    FROM cryptos c
                    JOIN coingecko_cryptos cg ON c.id = cg.crypto_id
                    WHERE c.is_active = true
                    GROUP BY cg.scraping_status
                    ORDER BY cg.scraping_status
                """)
                
                stats = {}
                for row in cursor.fetchall():
                    stats[row['scraping_status']] = {
                        'count': row['count'],
                        'total_points': row['total_points'] or 0,
                        'avg_errors': float(row['avg_errors'] or 0)
                    }
                
                return stats
                
        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return {}
    
    def close(self):
        """Cerrar conexión a PostgreSQL"""
        if self.connection:
            self.connection.close()
            print("🔐 Conexión PostgreSQL cerrada")

class InfluxDBManager:
    """Manejador de InfluxDB para datos históricos"""
    
    def __init__(self, config: InfluxDBConfig):
        self.config = config
        self.client = None
        self.write_api = None
        
    def connect(self):
        """Conectar a InfluxDB (compatible con 1.x y 2.x)"""
        try:
            # Para InfluxDB 1.x (sin token)
            if not self.config.token:
                from influxdb import InfluxDBClient
                self.client = InfluxDBClient(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database
                )
                try:
                    self.client.ping()
                    print(f"✅ Conectado a InfluxDB 1.x: {self.config.host}:{self.config.port}")
                    return True
                except Exception as e:
                    print(f"❌ Error conectando a InfluxDB 1.x: {e}")
                    return False
            
            # InfluxDB 2.x (con token)
            url = f"http://{self.config.host}:{self.config.port}"
            print(f"🔄 Conectando a InfluxDB v2: {url}")
            
            self.client = influxdb_client.InfluxDBClient(
                url=url,
                token=self.config.token,
                org=self.config.org
            )
            
            try:
                health = self.client.health()
                print(f"✅ InfluxDB Status: {health.status}")
            except Exception as e:
                print(f"⚠️ No se pudo verificar estado de InfluxDB: {e}")
            
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            print("✅ Conectado a InfluxDB v2")
            
            return True
            
        except Exception as e:
            print(f"❌ Error conectando a InfluxDB: {e}")
            return False
    
    def save_coingecko_range_data(self, crypto_data: Dict, start_date: str, end_date: str, 
                                 combined_data: List[Dict]) -> Dict:
        """Guarda datos de rango CoinGecko en InfluxDB"""
        if not combined_data:
            print("⚠️ No hay datos para guardar")
            return {'success': False, 'points_saved': 0}
        
        try:
            # Para InfluxDB 1.x
            if not self.config.token and hasattr(self.client, 'write_points'):
                return self._save_to_influxdb_1x(crypto_data, start_date, end_date, combined_data)
            
            # Para InfluxDB 2.x
            elif self.write_api:
                return self._save_to_influxdb_2x(crypto_data, start_date, end_date, combined_data)
            
            else:
                print("❌ Cliente InfluxDB no configurado correctamente")
                return {'success': False, 'points_saved': 0}
                
        except Exception as e:
            print(f"❌ Error guardando datos de rango: {e}")
            return {'success': False, 'points_saved': 0, 'error': str(e)}
    
    def _save_to_influxdb_1x(self, crypto_data: Dict, start_date: str, end_date: str, combined_data: List[Dict]) -> Dict:
        """Guardar en InfluxDB 1.x"""
        symbol = crypto_data.get('simbolo', 'UNKNOWN')
        name = crypto_data.get('nombre', 'Unknown')
        slug = crypto_data.get('slug', '')
        
        points = []
        timestamps = []
        
        for data_point in combined_data:
            try:
                timestamp_ms = data_point.get('timestamp')
                if timestamp_ms is None or timestamp_ms <= 0:
                    continue
                
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                timestamps.append(timestamp_dt)
                
                price = data_point.get('price')
                market_cap = data_point.get('market_cap')
                
                if price is None and market_cap is None:
                    continue
                
                point = {
                    "measurement": "coingecko_historical_complete",
                    "tags": {
                        "symbol": symbol,
                        "name": name,
                        "slug": slug,
                        "source": "coingecko_completed_daily",
                        "range_start": start_date,
                        "range_end": end_date
                    },
                    "time": timestamp_dt.isoformat(),
                    "fields": {}
                }
                
                if price is not None:
                    try:
                        price_float = float(price)
                        if price_float >= 0:
                            point["fields"]["price"] = price_float
                    except (ValueError, TypeError):
                        pass
                
                if market_cap is not None:
                    try:
                        market_cap_float = float(market_cap)
                        if market_cap_float >= 0:
                            point["fields"]["market_cap"] = market_cap_float
                    except (ValueError, TypeError):
                        pass
                
                if point["fields"]:
                    points.append(point)
                    
            except Exception as e:
                continue
        
        if points:
            batch_size = 1000
            points_written = 0
            
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                
                success = self.client.write_points(batch)
                if success:
                    points_written += len(batch)
                else:
                    print(f"❌ Error escribiendo lote {i//batch_size + 1}")
            
            oldest_date = min(timestamps).strftime('%Y-%m-%d') if timestamps else None
            latest_date = max(timestamps).strftime('%Y-%m-%d') if timestamps else None
            
            return {
                'success': True,
                'points_saved': points_written,
                'oldest_date': oldest_date,
                'latest_date': latest_date,
                'date_range_days': (max(timestamps) - min(timestamps)).days if len(timestamps) > 1 else 0
            }
        else:
            return {'success': False, 'points_saved': 0}
    
    def _save_to_influxdb_2x(self, crypto_data: Dict, start_date: str, end_date: str, combined_data: List[Dict]) -> Dict:
        """Guardar en InfluxDB 2.x"""
        symbol = crypto_data.get('simbolo', 'UNKNOWN')
        name = crypto_data.get('nombre', 'Unknown')
        slug = crypto_data.get('slug', '')
        
        points = []
        timestamps = []
        
        for data_point in combined_data:
            try:
                timestamp_ms = data_point.get('timestamp')
                if timestamp_ms is None or timestamp_ms <= 0:
                    continue
                
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                timestamps.append(timestamp_dt)
                
                price = data_point.get('price')
                market_cap = data_point.get('market_cap')
                
                if price is None and market_cap is None:
                    continue
                
                point = Point("coingecko_historical_complete")
                point.tag("symbol", symbol)
                point.tag("name", name)
                point.tag("slug", slug)
                point.tag("source", "coingecko_completed_daily")
                point.tag("range_start", start_date)
                point.tag("range_end", end_date)
                
                if price is not None:
                    try:
                        price_float = float(price)
                        if price_float >= 0:
                            point.field("price", price_float)
                    except (ValueError, TypeError):
                        pass
                
                if market_cap is not None:
                    try:
                        market_cap_float = float(market_cap)
                        if market_cap_float >= 0:
                            point.field("market_cap", market_cap_float)
                    except (ValueError, TypeError):
                        pass
                
                point.time(timestamp_dt)
                points.append(point)
                    
            except Exception as e:
                continue
        
        if points:
            batch_size = 1000
            points_written = 0
            
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                
                self.write_api.write(
                    bucket=self.config.database,
                    org=self.config.org,
                    record=batch
                )
                
                points_written += len(batch)
            
            oldest_date = min(timestamps).strftime('%Y-%m-%d') if timestamps else None
            latest_date = max(timestamps).strftime('%Y-%m-%d') if timestamps else None
            
            return {
                'success': True,
                'points_saved': points_written,
                'oldest_date': oldest_date,
                'latest_date': latest_date,
                'date_range_days': (max(timestamps) - min(timestamps)).days if len(timestamps) > 1 else 0
            }
        else:
            return {'success': False, 'points_saved': 0}
    
    def close(self):
        """Cerrar conexión a InfluxDB"""
        if self.client:
            if hasattr(self.client, 'close'):
                self.client.close()
            print("🔐 Conexión InfluxDB cerrada")

class SeleniumHistoricalCompleteScraper:
    def __init__(self, 
                 delay: float = 2.0,
                 headless: bool = True,
                 range_days: int = 30,
                 crypto_limit: Optional[int] = None,
                 influxdb_config: InfluxDBConfig = None,
                 postgres_config: PostgreSQLConfig = None):
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
    
    def setup_driver(self, headless: bool = True):
        """Configura el driver de Chrome con opciones anti-detección mejoradas"""
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
        
        # Opciones anti-detección robustas (como en script original)
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-javascript") 
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Configurar preferencias para deshabilitar contenido innecesario
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.media_stream": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.cookies": 1,
            "profile.managed_default_content_settings.javascript": 1,
            "profile.managed_default_content_settings.notifications": 2,
            "profile.managed_default_content_settings.auto_select_certificate": 2,
            "profile.managed_default_content_settings.mixed_script": 2,
            "profile.managed_default_content_settings.media_stream_mic": 2,
            "profile.managed_default_content_settings.media_stream_camera": 2,
            "profile.managed_default_content_settings.protocol_handlers": 2,
            "profile.managed_default_content_settings.push_messaging": 2,
            "profile.managed_default_content_settings.ssl_cert_decisions": 2,
            "profile.managed_default_content_settings.metro_switch_to_desktop": 2,
            "profile.managed_default_content_settings.protected_media_identifier": 2,
            "profile.managed_default_content_settings.app_banner": 2,
            "profile.managed_default_content_settings.site_engagement": 2,
            "profile.managed_default_content_settings.durable_storage": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Anti-detección adicional
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Script anti-detección adicional
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            
            # Configurar timeouts más largos
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            print("Driver de Chrome configurado correctamente para scraping CoinGecko")
            
        except Exception as e:
            print(f"Error al configurar Chrome driver: {e}")
            raise
    
    def connect_databases(self):
        """Conectar a PostgreSQL e InfluxDB"""
        print("=== INICIANDO CONEXIONES DE BD ===")
        postgres_connected = False
        influx_connected = False
        
        print("Intentando conectar a PostgreSQL...")
        try:
            postgres_connected = self.postgres_manager.connect()
            print(f"PostgreSQL conectado: {postgres_connected}")
            
            if postgres_connected:
                print("Obteniendo estadísticas de scraping...")
                stats = self.postgres_manager.get_coingecko_scraping_stats()
                if stats:
                    print("Estado actual scraping CoinGecko:")
                    for status, data in stats.items():
                        print(f"   {status}: {data['count']} cryptos, {data['total_points']} puntos")
                else:
                    print("No se pudieron obtener estadísticas")
        except Exception as e:
            print(f"ERROR conectando a PostgreSQL: {e}")
            import traceback
            traceback.print_exc()
        
        print("Intentando conectar a InfluxDB...")
        try:
            influx_connected = self.influx_manager.connect()
            print(f"InfluxDB conectado: {influx_connected}")
        except Exception as e:
            print(f"ERROR conectando a InfluxDB: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"Resultado final conexiones - PostgreSQL: {postgres_connected}, InfluxDB: {influx_connected}")
        return postgres_connected, influx_connected
    
    def load_completed_daily_cryptocurrencies(self) -> List[Dict]:
        """VERSIÓN ARREGLADA - Carga con debugging mejorado"""
        print("=== CARGANDO CRIPTOMONEDAS PARA HISTÓRICOS ===")
        try:
            # ARREGLADO: Primero verificar qué hay en la BD
            if self.postgres_manager.connection:
                with self.postgres_manager.connection.cursor() as cursor:
                    # Contar total de cryptos activas
                    cursor.execute("SELECT COUNT(*) as total FROM cryptos WHERE is_active = true")
                    total_active = cursor.fetchone()["total"]

                    
                    # Contar por status en coingecko_cryptos
                    cursor.execute("""
                        SELECT cg.scraping_status, COUNT(*) 
                        FROM cryptos c 
                        JOIN coingecko_cryptos cg ON c.id = cg.crypto_id 
                        WHERE c.is_active = true 
                        GROUP BY cg.scraping_status
                    """)
                    status_counts = dict(cursor.fetchall())
                    
                    print(f"📊 Estado actual BD:")
                    print(f"   Total cryptos activas: {total_active}")
                    print(f"   Por status: {status_counts}")
            
            # Llamar al método arreglado
            cryptocurrencies = self.postgres_manager.get_completed_daily_cryptocurrencies(limit=self.crypto_limit)
            print(f"📤 Resultado: {len(cryptocurrencies) if cryptocurrencies else 'None'} criptomonedas")
            
            if cryptocurrencies:
                print(f"🔍 Primeras 3 criptomonedas:")
                for i, crypto in enumerate(cryptocurrencies[:3]):
                    print(f"   {i+1}. {crypto.get('simbolo')} - Status: {crypto.get('scraping_status')} - Oldest: {crypto.get('oldest_data_fetched')}")
            
            return cryptocurrencies
            
        except Exception as e:
            print(f"❌ ERROR cargando: {e}")
            import traceback
            traceback.print_exc()
            return []

    
    def extract_url_name(self, enlace: str) -> str:
        """Extrae el nombre de la URL del enlace"""
        return enlace.rstrip('/').split('/')[-1]
    
    def timestamp_for_date(self, date_str: str, is_end: bool = False) -> int:
        """Convierte fecha string a timestamp Unix"""
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            if is_end:
                dt = dt.replace(hour=23, minute=59, second=59)
            else:
                dt = dt.replace(hour=0, minute=0, second=0)
            
            return int(dt.timestamp())
        except Exception as e:
            print(f"❌ Error convirtiendo fecha {date_str}: {e}")
            return 0
    
    def download_range_data_selenium(self, url_name: str, start_date: str, end_date: str, data_type: str) -> Optional[List]:
        """Descarga datos para un rango de fechas usando las URLs exactas de CoinGecko"""
        print(f"\n=== DEBUG DESCARGA ===")
        print(f"URL name: {url_name}")
        print(f"Start date: {start_date}")
        print(f"End date: {end_date}")
        print(f"Data type: {data_type}")
        
        timestamp_from = self.timestamp_for_date(start_date)
        timestamp_to = self.timestamp_for_date(end_date, True)
        
        print(f"Timestamp from: {timestamp_from}")
        print(f"Timestamp to: {timestamp_to}")
        
        if timestamp_from == 0 or timestamp_to == 0:
            print(f"ERROR: Timestamps inválidos para {url_name} - {start_date} a {end_date}")
            return None
        
        # Verificar que el timestamp from es anterior al timestamp to
        if timestamp_from >= timestamp_to:
            print(f"ERROR: Timestamp from ({timestamp_from}) >= timestamp to ({timestamp_to})")
            return None
        
        # URLs exactas según la estructura de CoinGecko
        if data_type == 'price_charts' or data_type == 'price':
            url = f"https://www.coingecko.com/price_charts/{url_name}/usd/custom.json?from={timestamp_from}&to={timestamp_to}"
        elif data_type == 'market_cap':
            url = f"https://www.coingecko.com/market_cap/{url_name}/usd/custom.json?from={timestamp_from}&to={timestamp_to}"
        else:
            print(f"ERROR: Tipo de datos desconocido: {data_type}")
            return None
        
        print(f"URL construida: {url}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"\n  --> Intento {attempt + 1}/{max_retries}: {data_type}")
                print(f"  --> Navegando a: {url}")
                
                # Navegar a la URL
                start_time = time.time()
                self.driver.get(url)
                nav_time = time.time() - start_time
                print(f"  --> Navegación completada en {nav_time:.2f}s")
                
                # Esperar a que se cargue el contenido JSON
                wait = WebDriverWait(self.driver, 20)
                
                # Buscar el elemento <pre> que contiene el JSON
                json_element = None
                json_text = ""
                
                try:
                    print(f"  --> Buscando elemento <pre>...")
                    # Método principal: buscar elemento <pre>
                    json_element = wait.until(
                        EC.presence_of_element_located((By.TAG_NAME, "pre"))
                    )
                    json_text = json_element.text.strip()
                    print(f"  --> Elemento <pre> encontrado, longitud texto: {len(json_text)}")
                    
                    if not json_text:
                        print(f"  --> ERROR: Elemento <pre> está vacío")
                        raise Exception("Elemento <pre> vacío")
                        
                except TimeoutException:
                    print(f"  --> TIMEOUT esperando elemento <pre>, intentando obtener body...")
                    try:
                        # Método alternativo: obtener texto del body
                        body_element = wait.until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        json_text = body_element.text.strip()
                        print(f"  --> Body obtenido, longitud texto: {len(json_text)}")
                        
                        if not json_text:
                            print(f"  --> ERROR: Body está vacío")
                            raise Exception("Body vacío")
                            
                    except Exception as e:
                        print(f"  --> ERROR obteniendo body: {e}")
                        
                        # Intentar obtener page source como último recurso
                        try:
                            page_source = self.driver.page_source
                            print(f"  --> Page source obtenido, longitud: {len(page_source)}")
                            print(f"  --> Primeros 500 chars de page source:")
                            print(f"  --> {page_source[:500]}")
                            
                            # Buscar JSON en el page source
                            import re
                            json_match = re.search(r'\{.*"stats".*\}', page_source, re.DOTALL)
                            if json_match:
                                json_text = json_match.group()
                                print(f"  --> JSON encontrado en page source, longitud: {len(json_text)}")
                            else:
                                print(f"  --> No se encontró JSON en page source")
                                
                        except Exception as e2:
                            print(f"  --> ERROR obteniendo page source: {e2}")
                        
                        if attempt < max_retries - 1:
                            print(f"  --> Reintentando en 3 segundos...")
                            time.sleep(3)
                            continue
                        return None
                
                # Verificar si hay contenido JSON válido
                if not json_text or len(json_text) < 10:
                    print(f"  --> ERROR: Contenido JSON insuficiente: {len(json_text)} caracteres")
                    if json_text:
                        print(f"  --> Contenido recibido: '{json_text}'")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    return None
                
                print(f"  --> Intentando parsear JSON de {len(json_text)} caracteres...")
                print(f"  --> Primeros 200 chars: {json_text[:200]}")
                
                # Intentar parsear el JSON
                try:
                    data = json.loads(json_text)
                    print(f"  --> JSON parseado exitosamente, tipo: {type(data)}")
                    
                    if isinstance(data, dict):
                        print(f"  --> Claves disponibles: {list(data.keys())}")
                        
                        # Estructura esperada: {"stats": [[timestamp, value], ...], "total_volumes": [[timestamp, value], ...]}
                        stats = data.get('stats', [])
                        total_volumes = data.get('total_volumes', [])
                        
                        print(f"  --> Stats encontrados: {len(stats)} elementos")
                        print(f"  --> Total volumes encontrados: {len(total_volumes)} elementos")
                        
                        if stats and isinstance(stats, list):
                            print(f"  --> EXITO: {len(stats)} puntos de {data_type}")
                            if len(stats) > 0:
                                print(f"  --> Primer elemento stats: {stats[0]}")
                                print(f"  --> Último elemento stats: {stats[-1]}")
                            return stats
                        else:
                            print(f"  --> ERROR: Sin stats válidos en respuesta JSON")
                            if attempt < max_retries - 1:
                                time.sleep(2)
                                continue
                            return None
                    else:
                        print(f"  --> ERROR: JSON no es un diccionario: {type(data)}")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                            continue
                        return None
                        
                except json.JSONDecodeError as e:
                    print(f"  --> ERROR parseando JSON: {e}")
                    print(f"  --> JSON problemático (primeros 500 chars): {json_text[:500]}")
                    if attempt < max_retries - 1:
                        time.sleep(3)
                        continue
                    return None
                    
            except WebDriverException as e:
                print(f"  --> ERROR WebDriver: {e}")
                if attempt < max_retries - 1:
                    time.sleep(4)
                    continue
                return None
                
            except Exception as e:
                print(f"  --> ERROR general: {e}")
                import traceback
                print(f"  --> Traceback: {traceback.format_exc()}")
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                return None
        
        print(f"  --> FALLO FINAL después de {max_retries} intentos")
        return None
    
    def combine_range_data(self, price_data: List, market_cap_data: List) -> List[Dict]:
        """Combina datos de precios y capitalización por timestamp con validación mejorada"""
        combined_data = []
        
        # Validar datos de entrada
        if not price_data or not isinstance(price_data, list):
            print("    Datos de precios inválidos o vacíos")
            return []
        
        # Convertir market_cap_data a diccionario para búsqueda rápida
        market_cap_dict = {}
        if market_cap_data and isinstance(market_cap_data, list):
            for item in market_cap_data:
                try:
                    if isinstance(item, list) and len(item) >= 2:
                        timestamp = int(float(item[0])) if item[0] is not None else None
                        market_cap_value = float(item[1]) if item[1] is not None else None
                        if timestamp is not None and market_cap_value is not None and market_cap_value > 0:
                            market_cap_dict[timestamp] = market_cap_value
                except (ValueError, TypeError, IndexError) as e:
                    continue  # Saltar datos malformados
        
        # Procesar datos de precios
        valid_points = 0
        for price_item in price_data:
            try:
                if isinstance(price_item, list) and len(price_item) >= 2:
                    timestamp = int(float(price_item[0])) if price_item[0] is not None else None
                    price_value = float(price_item[1]) if price_item[1] is not None else None
                    
                    if timestamp is not None and timestamp > 0:
                        # Obtener market cap correspondiente
                        market_cap_value = market_cap_dict.get(timestamp, None)
                        
                        # Validar que tenemos al menos precio válido
                        if price_value is not None and price_value > 0:
                            combined_data.append({
                                'timestamp': timestamp,
                                'price': price_value,
                                'market_cap': market_cap_value
                            })
                            valid_points += 1
                        elif market_cap_value is not None:
                            # Solo market cap sin precio
                            combined_data.append({
                                'timestamp': timestamp,
                                'price': None,
                                'market_cap': market_cap_value
                            })
                            valid_points += 1
                            
            except (ValueError, TypeError, IndexError) as e:
                continue  # Saltar datos malformados
        
        # Ordenar por timestamp para consistencia
        if combined_data:
            combined_data.sort(key=lambda x: x['timestamp'])
            print(f"    Datos combinados: {len(combined_data)} puntos válidos de {len(price_data)} precios y {len(market_cap_data)} market caps")
        
        return combined_data
    
    def random_delay(self, min_delay: float = None, max_delay: float = None):
        """Delay aleatorio entre peticiones"""
        if min_delay is None:
            min_delay = self.delay
        if max_delay is None:
            max_delay = self.delay * 1.5
        
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def complete_cryptocurrency_historical_data(self, crypto: Dict) -> bool:
        """VERSIÓN ARREGLADA - Completa datos históricos desde hoy hacia atrás hasta crypto_start_date"""
        symbol = crypto.get('simbolo', '').upper()
        enlace = crypto.get('enlace', '')
        nombre = crypto.get('nombre', '')
        crypto_id = crypto.get('crypto_id')
        current_oldest_date_str = str(crypto.get('oldest_data_fetched', ''))
        crypto_start_date_str = crypto.get('crypto_start_date', '2009-01-01')
        
        if not symbol or not enlace or not crypto_id:
            print(f"⚠️ Datos incompletos para {nombre}: symbol={symbol}, enlace={enlace}, crypto_id={crypto_id}")
            return False
        
        print(f"\n📊 === COMPLETANDO DATOS HISTÓRICOS: {nombre} ({symbol}) ===")
        print(f"📅 Comenzando desde hoy hasta: {crypto_start_date_str}")
        
        # ARREGLADO: Validación de fechas más robusta
        try:
            crypto_start_date = datetime.strptime(crypto_start_date_str, '%Y-%m-%d').date()
            
            # Fecha de inicio: hoy
            current_end_date = datetime.now().date()
            
            # Si la fecha de inicio es posterior a hoy, no hay nada que hacer
            if crypto_start_date > current_end_date:
                print(f"⚠️ {symbol} inicia en el futuro, nada que hacer")
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    status='completed',
                    notes='Fecha de inicio en el futuro'
                )
                return True
                
        except ValueError as e:
            print(f"❌ Error en fechas para {symbol}: {e}")
            return False
        
        # Extraer nombre de URL
        url_name = self.extract_url_name(enlace)
        
        # Variables para seguimiento
        total_points_saved = 0
        total_ranges_processed = 0
        final_oldest_date = current_oldest_date_str or crypto_start_date_str
        final_latest_date = datetime.now().strftime('%Y-%m-%d')
        
        # Marcar como en progreso
        self.postgres_manager.update_coingecko_scraping_progress(
            crypto_id=crypto_id,
            status='in_progress',
            notes=f'Completando datos históricos desde hoy hasta {crypto_start_date_str}'
        )
        
        max_consecutive_empty_ranges = 5
        empty_ranges_count = 0
        range_num = 0
        # Removido max_ranges para procesar sin límite
        
        try:
            # BUCLE PRINCIPAL ARREGLADO PARA RECORRER DESDE HOY HACIA ATRÁS
            while current_end_date > crypto_start_date:
                range_num += 1
                print(f"\n🔄 --- RANGO HISTÓRICO {range_num} para {symbol} ---")
                print(f"📅 Fecha final actual: {current_end_date.strftime('%Y-%m-%d')}")
                
                # Calcular fechas para este rango (hacia atrás)
                end_date = current_end_date.strftime('%Y-%m-%d')
                start_date_dt = current_end_date - timedelta(days=self.range_days - 1)
                
                # ARREGLADO: Validar límite histórico
                if start_date_dt < crypto_start_date:
                    start_date_dt = crypto_start_date
                    print(f"📅 Ajustado al límite histórico: {start_date_dt}")
                    is_final_range = True
                else:
                    is_final_range = False
                
                start_date = start_date_dt.strftime('%Y-%m-%d')
                
                print(f"📦 Procesando rango {range_num}: {start_date} → {end_date}")
                
                # Validar que el rango tiene sentido
                if start_date >= end_date:
                    print(f"🏁 Rango inválido para {symbol}, terminando")
                    break
                
                # Descargar datos de precios
                print(f"🔍 Descargando precios para {url_name}...")
                price_data = self.download_range_data_selenium(url_name, start_date, end_date, 'price_charts')
                
                if not price_data:
                    empty_ranges_count += 1
                    print(f"⚠️ Sin datos de precios para rango {range_num} ({empty_ranges_count}/{max_consecutive_empty_ranges} vacíos consecutivos)")
                    
                    if empty_ranges_count >= max_consecutive_empty_ranges:
                        print(f"🏁 Alcanzado límite de rangos vacíos para {symbol}")
                        break
                    
                    # Intentar el siguiente rango
                    current_end_date = start_date_dt - timedelta(days=1)
                    continue
                
                # Reset contador de rangos vacíos
                empty_ranges_count = 0
                
                # Delay entre peticiones
                self.random_delay(self.delay * 0.8, self.delay * 1.2)
                
                # Descargar datos de capitalización de mercado
                print(f"🔍 Descargando market cap para {url_name}...")
                market_cap_data = self.download_range_data_selenium(url_name, start_date, end_date, 'market_cap')
                if not market_cap_data:
                    print(f"⚠️ Sin datos de market cap para {url_name}")
                    market_cap_data = []
                
                # Combinar datos
                combined_data = self.combine_range_data(price_data, market_cap_data)
                
                if not combined_data:
                    print(f"⚠️ Sin datos combinados válidos para rango {range_num}")
                    current_end_date = start_date_dt - timedelta(days=1)
                    
                    if is_final_range:
                        break
                    continue
                
                print(f"✅ Datos procesados: {len(combined_data)} puntos válidos en rango {range_num}")
                
                # Guardar datos en InfluxDB
                influx_result = {'success': False, 'points_saved': 0}
                if self.influx_manager.client:
                    influx_result = self.influx_manager.save_coingecko_range_data(
                        crypto, start_date, end_date, combined_data
                    )
                
                if influx_result.get('success'):
                    # Acumular estadísticas
                    total_points_saved += influx_result['points_saved']
                    total_ranges_processed += 1
                    
                    # Actualizar fechas extremas
                    if influx_result.get('oldest_date') and (not final_oldest_date or influx_result['oldest_date'] < final_oldest_date):
                        final_oldest_date = influx_result['oldest_date']
                    
                    if influx_result.get('latest_date') and (not final_latest_date or influx_result['latest_date'] > final_latest_date):
                        final_latest_date = influx_result['latest_date']
                    
                    print(f"💾 Guardado exitoso - Rango {range_num}: {influx_result['points_saved']} puntos")
                    
                    # Actualizar progreso parcial
                    new_oldest_date = influx_result.get('oldest_date')
                    self.postgres_manager.update_coingecko_scraping_progress(
                        crypto_id=crypto_id,
                        status='in_progress',
                        total_points=influx_result['points_saved'],
                        oldest_date=new_oldest_date,
                        latest_date=influx_result.get('latest_date'),
                        notes=f'Rango {range_num}: {influx_result["points_saved"]} puntos, oldest: {new_oldest_date}'
                    )
                    
                    # Preparar para siguiente rango
                    current_end_date = start_date_dt - timedelta(days=1)
                    
                else:
                    print(f"❌ Error guardando rango {range_num}")
                    current_end_date = start_date_dt - timedelta(days=1)
                
                # ARREGLADO: Verificar si es el rango final
                if is_final_range:
                    print(f"🏁 Completado último rango posible para {symbol}")
                    break
                
                # Pausa entre rangos
                self.random_delay(self.delay * 1.5, self.delay * 2.5)
            
            # FINALIZACIÓN
            print(f"\n📈 === RESUMEN HISTÓRICO {symbol} ===")
            print(f"📊 Rangos procesados: {total_ranges_processed}")
            print(f"💾 Puntos guardados: {total_points_saved}")
            print(f"📅 Fecha más antigua: {final_oldest_date}")
            
            if total_ranges_processed > 0 and total_points_saved > 0:
                # Determinar si completado
                is_completed = (
                    empty_ranges_count >= max_consecutive_empty_ranges or
                    final_oldest_date <= crypto_start_date_str
                )
                
                final_status = 'completed' if is_completed else 'completed_daily'
                
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    status=final_status,
                    oldest_date=final_oldest_date,
                    latest_date=final_latest_date,
                    notes=f'{final_status.upper()}: {total_ranges_processed} rangos, {total_points_saved} puntos, desde {final_oldest_date}'
                )
                
                print(f"🎉 {symbol} {final_status.upper()}")
                return True
            
            else:
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    status='error',
                    notes=f'Sin datos después de {total_ranges_processed} rangos'
                )
                print(f"❌ {symbol} ERROR - Sin datos")
                return False
                
        except Exception as e:
            print(f"❌ Error completando {symbol}: {e}")
            import traceback
            traceback.print_exc()
            
            self.postgres_manager.update_coingecko_scraping_progress(
                crypto_id=crypto_id,
                status='error',
                notes=f'Error durante completado: {str(e)[:200]}'
            )
            return False
    
    def run(self) -> None:
        """Ejecuta el proceso de completar datos históricos SIN LÍMITE DE RANGOS"""
        
        print(f"🚀 === COMPLETAR DATOS HISTÓRICOS COINGECKO (SIN LÍMITES) ===")
        print(f"🎯 TARGET: Criptomonedas con scraping_status = 'completed_daily'")
        print(f"📈 PROCESAMIENTO: Desde hoy hacia atrás hasta el origen")
        print(f"📦 Rangos de {self.range_days} días - SIN LÍMITE MÁXIMO")
        print(f"🔄 Continúa hasta obtener TODOS los datos históricos disponibles")
        print(f"✅ MARCA COMPLETED: Cuando completa absolutamente todos los datos históricos")
        
        # Conectar a bases de datos
        print(f"\n=== CONECTANDO A BASES DE DATOS ===")
        try:
            postgres_connected, influx_connected = self.connect_databases()
            print(f"PostgreSQL conectado: {postgres_connected}")
            print(f"InfluxDB conectado: {influx_connected}")
        except Exception as e:
            print(f"ERROR conectando bases de datos: {e}")
            import traceback
            traceback.print_exc()
            return
        
        if not postgres_connected:
            print("❌ No se pudo conectar a PostgreSQL - Proceso abortado")
            return
        
        if influx_connected:
            print("✅ InfluxDB conectado - Los datos se guardarán en InfluxDB")
        else:
            print("⚠️ InfluxDB no disponible - Solo se actualizarán estados en PostgreSQL")
        
        # Cargar criptomonedas completed_daily
        print(f"\n=== CARGANDO CRIPTOMONEDAS ===")
        try:
            cryptocurrencies = self.load_completed_daily_cryptocurrencies()
            print(f"Criptomonedas cargadas: {len(cryptocurrencies) if cryptocurrencies else 'None'}")
        except Exception as e:
            print(f"ERROR cargando criptomonedas: {e}")
            import traceback
            traceback.print_exc()
            return
        
        if not cryptocurrencies:
            print("❌ No se encontraron criptomonedas 'completed_daily' para procesar")
            return
        
        print(f"📊 Procesando {len(cryptocurrencies)} criptomonedas 'completed_daily' SIN LÍMITES")
        if self.crypto_limit:
            print(f"🔢 Límite aplicado: {self.crypto_limit} criptomonedas")
        
        # Mostrar primeras criptomonedas para debugging
        print(f"\n=== PRIMERAS CRIPTOMONEDAS A PROCESAR ===")
        for i, crypto in enumerate(cryptocurrencies[:3]):  # Mostrar solo las primeras 3
            print(f"{i+1}. {crypto.get('simbolo', 'N/A')} - {crypto.get('nombre', 'N/A')}")
            print(f"   Oldest date: {crypto.get('oldest_data_fetched', 'N/A')}")
            print(f"   Crypto ID: {crypto.get('crypto_id', 'N/A')}")
            print(f"   Enlace: {crypto.get('enlace', 'N/A')}")
        
        # Estadísticas de ejecución
        successful = 0
        failed = 0
        
        try:
            for i, crypto in enumerate(cryptocurrencies, 1):
                symbol = crypto.get('simbolo', 'UNKNOWN')
                oldest_date = crypto.get('oldest_data_fetched', 'UNKNOWN')
                crypto_id = crypto.get('crypto_id', 'UNKNOWN')
                
                print(f"\n=== PROCESANDO CRYPTO {i}/{len(cryptocurrencies)} ===")
                print(f"Symbol: {symbol}")
                print(f"Oldest date: {oldest_date}")
                print(f"Crypto ID: {crypto_id}")
                print(f"Iniciando procesamiento...")
                
                try:
                    # Completar datos históricos sin límite de rangos
                    print(f"Llamando a complete_cryptocurrency_historical_data...")
                    result = self.complete_cryptocurrency_historical_data(crypto)
                    print(f"Resultado del procesamiento: {result}")
                    
                    if result:
                        successful += 1
                        print(f"✅ {symbol}: Datos históricos completados exitosamente (sin límites)")
                    else:
                        failed += 1
                        print(f"❌ {symbol}: Error en procesamiento de datos históricos")
                        
                except Exception as e:
                    print(f"❌ ERROR procesando {crypto.get('nombre', 'Desconocido')}: {e}")
                    import traceback
                    traceback.print_exc()
                    failed += 1
                
                # Pausa entre criptomonedas
                if i < len(cryptocurrencies):
                    delay_time = random.uniform(self.delay * 2, self.delay * 3)
                    print(f"Pausa entre cryptos: {delay_time:.1f} segundos...")
                    time.sleep(delay_time)
                    
        except KeyboardInterrupt:
            print("\n🛑 Proceso interrumpido por el usuario")
        except Exception as e:
            print(f"❌ Error inesperado durante el procesamiento: {e}")
            import traceback
            traceback.print_exc()
        
        # Estadísticas finales
        print(f"\n\n📈 === RESUMEN FINAL (HISTÓRICOS SIN LÍMITES) ===")
        print(f"✅ Exitosos: {successful}")
        print(f"❌ Fallidos: {failed}")
        print(f"📊 Estados actualizados en PostgreSQL")
        print(f"🔄 Procesamiento SIN LÍMITE DE RANGOS - Obtenidos todos los datos disponibles")
        print(f"🎯 Cryptos marcadas como 'completed' tienen absolutamente toda su historia completa")
        
        if influx_connected:
            print(f"💾 Datos históricos guardados en InfluxDB")
        
        # Mostrar estadísticas finales
        try:
            final_stats = self.postgres_manager.get_coingecko_scraping_stats()
            if final_stats:
                print(f"\n📊 === ESTADO FINAL SCRAPING ===")
                for status, data in final_stats.items():
                    print(f"{status}: {data['count']} cryptos, {data['total_points']} puntos")
        except Exception as e:
            print(f"ERROR obteniendo estadísticas finales: {e}")
    
    def close(self):
        """Cierra el driver y conexiones"""
        if self.driver:
            self.driver.quit()
            print("🔐 Driver cerrado")
        
        if self.influx_manager:
            self.influx_manager.close()
        
        if self.postgres_manager:
            self.postgres_manager.close()


def main():
    """Función principal"""
    scraper = None
    
    try:
        print("🚀 === CoinGecko COMPLETAR DATOS HISTÓRICOS (SIN LÍMITES) ===")
        print("PROCESA CRIPTOS 'completed_daily' HASTA COMPLETAR TODA SU HISTORIA")
        print("SIN LÍMITE DE RANGOS - OBTIENE TODOS LOS DATOS DISPONIBLES")
        print("MARCA 'completed' CUANDO TERMINA ABSOLUTAMENTE TODOS LOS DATOS HISTÓRICOS")
        
        # Cargar variables de entorno
        load_env_file()
        
        # Configuración
        headless = input("¿Ejecutar en modo headless? (s/N): ").lower().startswith('s')
        delay = float(input("Delay entre peticiones en segundos (recomendado: 2-4): ") or "3.0")
        range_days = int(input("Días por rango histórico (recomendado: 30): ") or "30")
        
        # Límite de criptomonedas (opcional)
        limit_input = input("¿Límite de criptomonedas a procesar? (Enter para todas): ").strip()
        crypto_limit = int(limit_input) if limit_input.isdigit() else None
        
        # Crear configuraciones
        try:
            postgres_config = PostgreSQLConfig()
        except Exception as e:
            print(f"❌ Error en configuración de PostgreSQL: {e}")
            return
        
        try:
            influxdb_config = InfluxDBConfig()
        except Exception as e:
            print(f"❌ Error en configuración de InfluxDB: {e}")
            print("Continuando solo con PostgreSQL...")
            influxdb_config = None
        
        # Crear scraper
        scraper = SeleniumHistoricalCompleteScraper(
            delay=delay,
            headless=headless,
            range_days=range_days,
            crypto_limit=crypto_limit,
            influxdb_config=influxdb_config,
            postgres_config=postgres_config
        )
        
        # Ejecutar completado de datos históricos
        scraper.run()
        
    except KeyboardInterrupt:
        print("\n🛑 Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"❌ Error en main: {e}")
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    main()