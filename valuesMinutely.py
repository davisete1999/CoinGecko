#!/usr/bin/env python3
"""
Script para descargar datos de criptomonedas en rangos de días desde CoinGecko usando Selenium
Usa el esquema normalizado de PostgreSQL con tablas separadas por fuente y guarda en InfluxDB
Con tracking de última fecha procesada para continuar desde donde se quedó

VERSIÓN FINAL NORMALIZADA:
- Esquema normalizado con separación por fuentes
- Manejo robusto de valores None en conversiones numéricas
- Identificación única por crypto_id para evitar duplicados
- Validaciones mejoradas de datos de entrada
- Logs detallados y tracking completo de progreso
- Compatible con estructura actual de crypto_scraping_log
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
    port: int = int(os.getenv('INFLUXDB_EXTERNAL_PORT') or '8086')  # ARREGLADO: Manejar None
    database: str = os.getenv('INFLUXDB_DB', 'crypto_historical')
    token: str = os.getenv('INFLUXDB_TOKEN', '0_aEXpz0v0Nhbw-fHpaarS4IEcrktOJjSGFpG9SLMh3tijC8QDyI9ahXfmNnBtQ1sSnIIlGMYqJCo3A6rtF9NQ==')
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
    port: int = int(os.getenv('POSTGRES_EXTERNAL_PORT') or '5432')  # ARREGLADO: Manejar None
    database: str = os.getenv('POSTGRES_DB', 'cryptodb')
    user: str = os.getenv('POSTGRES_USER', 'crypto_user')
    password: str = os.getenv('POSTGRES_PASSWORD', 'davisete453')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        logger.info(f"🔧 PostgreSQL Config: {self.host}:{self.port}/{self.database}")

class PostgreSQLManager:
    """Manejador de PostgreSQL con esquema normalizado para rangos CoinGecko"""
    
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
                    logger.info(f"✅ Conectado a PostgreSQL - CoinGecko source_id: {self.coingecko_source_id}")
                else:
                    logger.error("❌ No se encontró fuente 'coingecko' en crypto_sources")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando a PostgreSQL: {e}")
            return False
    
    def get_coingecko_cryptocurrencies_for_ranges(self, limit: Optional[int] = None) -> List[Dict]:
        """Obtiene criptomonedas CoinGecko priorizadas para scraping por rangos"""
        if not self.connection:
            logger.error("❌ No hay conexión a PostgreSQL")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                # Consulta usando esquema normalizado con priorización para rangos
                sql = """
                SELECT 
                    c.id as crypto_id,
                    c.name,
                    c.symbol,
                    c.slug,
                    c.is_active,
                    
                    -- Datos específicos de CoinGecko
                    cg.id as coingecko_id,
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
                    cg.next_fetch_priority,
                    cg.scraping_notes,
                    
                    -- Determinar categoría de prioridad específica para rangos
                    CASE 
                        WHEN cg.scraping_status = 'pending' THEN 'URGENT'
                        WHEN cg.scraping_status = 'error' AND cg.fetch_error_count < 3 THEN 'RETRY'
                        WHEN cg.scraping_status = 'in_progress' AND 
                             cg.last_fetch_attempt < NOW() - INTERVAL '2 hours' THEN 'STUCK'
                        WHEN cg.scraping_status = 'completed' AND 
                             (cg.last_values_update IS NULL OR 
                              cg.last_values_update < CURRENT_DATE - INTERVAL '30 days') THEN 'UPDATE_NEEDED'
                        WHEN cg.scraping_status = 'completed' THEN 'CURRENT'
                        ELSE 'UNKNOWN'
                    END as priority_category
                    
                FROM cryptos c
                INNER JOIN coingecko_cryptos cg ON c.id = cg.crypto_id
                WHERE c.is_active = true 
                AND c.slug IS NOT NULL 
                AND c.slug != ''
                ORDER BY 
                    -- Prioridad por categoría específica para rangos
                    CASE 
                        WHEN cg.scraping_status = 'pending' THEN 1
                        WHEN cg.scraping_status = 'in_progress' AND 
                             cg.last_fetch_attempt < NOW() - INTERVAL '2 hours' THEN 2
                        WHEN cg.scraping_status = 'error' AND cg.fetch_error_count < 3 THEN 3
                        WHEN cg.scraping_status = 'completed' AND 
                             (cg.last_values_update IS NULL OR 
                              cg.last_values_update < CURRENT_DATE - INTERVAL '30 days') THEN 4
                        WHEN cg.scraping_status = 'completed' THEN 5
                        ELSE 6
                    END,
                    -- Prioridad secundaria por sistema calculado
                    cg.next_fetch_priority ASC,
                    -- Prioridad terciaria por ranking CoinGecko
                    cg.coingecko_rank ASC NULLS LAST,
                    -- Última prioridad alfabética
                    c.symbol ASC
                """
                
                if limit:
                    sql += f" LIMIT {limit}"
                
                cursor.execute(sql)
                rows = cursor.fetchall()
                
                # Convertir a formato compatible para ranges
                cryptocurrencies = []
                priority_stats = {'URGENT': 0, 'STUCK': 0, 'RETRY': 0, 'UPDATE_NEEDED': 0, 'CURRENT': 0, 'UNKNOWN': 0}
                
                for row in rows:
                    crypto = {
                        'crypto_id': row['crypto_id'],
                        'coingecko_id': row['coingecko_id'],
                        'nombre': row['name'],
                        'simbolo': row['symbol'],
                        'enlace': row['coin_url'] or f"https://www.coingecko.com/es/monedas/{row['slug']}",
                        'slug': row['slug'],
                        'coingecko_rank': row['coingecko_rank'],
                        'tags': row['tags'] or [],
                        'badges': row['badges'] or [],
                        'last_values_update': row['last_values_update'],
                        'oldest_data_fetched': row['oldest_data_fetched'],
                        'scraping_status': row['scraping_status'],
                        'total_data_points': row['total_data_points'] or 0,
                        'fetch_error_count': row['fetch_error_count'] or 0,
                        'priority_category': row['priority_category'],
                        'scraping_notes': row['scraping_notes']
                    }
                    cryptocurrencies.append(crypto)
                    
                    # Contar estadísticas de prioridad
                    category = row['priority_category']
                    if category in priority_stats:
                        priority_stats[category] += 1
                
                logger.info(f"✅ Obtenidas {len(cryptocurrencies)} criptomonedas CoinGecko para rangos")
                logger.info(f"📊 Distribución por prioridad: {priority_stats}")
                
                return cryptocurrencies
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo criptomonedas CoinGecko para rangos: {e}")
            return []
    
    def update_coingecko_range_scraping_status(self, crypto_id: int = None, symbol: str = None, 
                                             name: str = None, status: str = 'in_progress', 
                                             last_date: str = None, total_points: int = 0, 
                                             notes: str = None):
        """
        Actualiza el estado de scraping por rangos en esquema normalizado
        ARREGLADO: Usa crypto_id directamente cuando está disponible para evitar duplicados
        """
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
                    update_fields.append("total_data_points = COALESCE(total_data_points, 0) + %s")
                    params.append(total_points)
                
                if status == 'completed':
                    update_fields.append("fetch_error_count = 0")
                elif status == 'error':
                    update_fields.append("fetch_error_count = COALESCE(fetch_error_count, 0) + 1")
                
                if notes:
                    update_fields.append("scraping_notes = %s")
                    params.append(notes)
                
                # ARREGLADO: Usar crypto_id directamente cuando esté disponible
                if crypto_id:
                    # Usar crypto_id directamente (método preferido)
                    params.append(crypto_id)
                    where_clause = "WHERE crypto_id = %s"
                    identifier = f"crypto_id={crypto_id}"
                    
                elif symbol and name:
                    # Usar combinación de symbol y name para identificar únicamente
                    params.extend([symbol, name])
                    where_clause = """
                        WHERE crypto_id = (
                            SELECT id FROM cryptos 
                            WHERE symbol = %s AND name = %s 
                            LIMIT 1
                        )
                    """
                    identifier = f"{symbol} ({name})"
                    
                elif symbol:
                    # Fallback: usar solo symbol pero con LIMIT 1 para evitar error
                    params.append(symbol)
                    where_clause = """
                        WHERE crypto_id = (
                            SELECT id FROM cryptos 
                            WHERE symbol = %s 
                            ORDER BY id 
                            LIMIT 1
                        )
                    """
                    identifier = f"{symbol} (por symbol - primer match)"
                    logger.warning(f"⚠️ Usando solo symbol para {symbol}, puede no ser único")
                    
                else:
                    logger.error("❌ No se proporcionó crypto_id, symbol o name para identificar la crypto")
                    return
                
                sql = f"""
                    UPDATE coingecko_cryptos 
                    SET {', '.join(update_fields)}
                    {where_clause}
                """
                
                cursor.execute(sql, params)
                rows_affected = cursor.rowcount
                self.connection.commit()
                
                if rows_affected > 0:
                    logger.debug(f"✅ Estado de rangos actualizado para {identifier}: {status}")
                else:
                    logger.warning(f"⚠️ No se encontró crypto para actualizar rangos: {identifier}")
                
        except Exception as e:
            logger.error(f"❌ Error actualizando estado de rangos: {e}")
            if self.connection:
                self.connection.rollback()
    
    def create_range_scraping_log_entry(self, crypto_id: int, success: bool, 
                                       start_date: str, end_date: str, data_points: int = 0, 
                                       error_message: str = None, duration_seconds: int = None):
        """Crea entrada en log de scraping para rangos usando esquema normalizado"""
        if not self.connection or not self.coingecko_source_id:
            return
        
        try:
            with self.connection.cursor() as cursor:
                # ARREGLADO: Removida columna scraping_type que no existe en la tabla
                cursor.execute("""
                    INSERT INTO crypto_scraping_log (
                        crypto_id, source_id, date_range_start, date_range_end,
                        data_points_fetched, fetch_duration_seconds, success, error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    crypto_id, self.coingecko_source_id, start_date, end_date,
                    data_points, duration_seconds, success, error_message
                ))
                self.connection.commit()
                logger.debug(f"✅ Log de scraping de rangos creado para crypto_id {crypto_id}")
                
        except Exception as e:
            logger.error(f"❌ Error creando log de scraping de rangos: {e}")
            if self.connection:
                self.connection.rollback()
    
    def get_coingecko_range_scraping_stats(self) -> Dict:
        """Obtiene estadísticas de scraping de rangos CoinGecko"""
        if not self.connection:
            return {}
        
        try:
            with self.connection.cursor() as cursor:
                # Consulta directa sin depender de vistas
                cursor.execute("""
                    SELECT 
                        cg.scraping_status,
                        COUNT(*) as count,
                        SUM(cg.total_data_points) as total_points,
                        AVG(cg.fetch_error_count) as avg_errors,
                        COUNT(CASE WHEN cg.last_values_update IS NOT NULL THEN 1 END) as with_data
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
                        'avg_errors': float(row['avg_errors'] or 0),
                        'with_data': row['with_data'] or 0
                    }
                
                return stats
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas de rangos CoinGecko: {e}")
            return {}
    
    def close(self):
        """Cerrar conexión a PostgreSQL"""
        if self.connection:
            self.connection.close()
            logger.info("🔐 Conexión PostgreSQL cerrada")

class InfluxDBManager:
    """Manejador de InfluxDB para datos históricos por rangos CoinGecko con validaciones robustas"""
    
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
    
    def save_coingecko_range_data(self, crypto_data: Dict, start_date: str, end_date: str, 
                                 combined_data: List[Dict]) -> bool:
        """Guarda datos de rango CoinGecko en InfluxDB con tags específicos y validaciones robustas"""
        if not combined_data or not self.write_api:
            logger.warning("⚠️ No hay datos o write_api no disponible")
            return False
        
        try:
            points = []
            
            symbol = crypto_data.get('simbolo', 'UNKNOWN')
            name = crypto_data.get('nombre', 'Unknown')
            slug = crypto_data.get('slug', '')
            coingecko_rank = crypto_data.get('coingecko_rank')
            
            logger.info(f"🔄 Preparando {len(combined_data)} puntos para rango CoinGecko {start_date} → {end_date}")
            
            for data_point in combined_data:
                try:
                    # ARREGLADO: Validar timestamp antes de conversión
                    timestamp_ms = data_point.get('timestamp')
                    if timestamp_ms is None:
                        logger.debug(f"⚠️ Timestamp None para {symbol}")
                        continue
                    
                    # Asegurar que timestamp_ms es numérico
                    try:
                        timestamp_ms = float(timestamp_ms)
                        if timestamp_ms <= 0:
                            continue
                    except (ValueError, TypeError):
                        logger.debug(f"⚠️ Timestamp no numérico para {symbol}: {timestamp_ms}")
                        continue
                    
                    # CoinGecko timestamps están en milisegundos
                    timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    
                    price = data_point.get('price')
                    market_cap = data_point.get('market_cap')
                    
                    # Validar que tenemos al menos un dato válido
                    if price is None and market_cap is None:
                        continue
                    
                    # Crear punto para InfluxDB con tags específicos de rangos CoinGecko
                    point = Point("coingecko_historical_ranges")
                    point.tag("symbol", symbol)
                    point.tag("name", name)
                    point.tag("slug", slug)
                    point.tag("source", "coingecko_ranges_normalized")
                    point.tag("range_start", start_date)
                    point.tag("range_end", end_date)
                    
                    # ARREGLADO: Tags adicionales específicos de CoinGecko con validación
                    if coingecko_rank is not None:
                        try:
                            rank_int = int(float(coingecko_rank))  # Convertir vía float para manejar strings
                            point.tag("rank_category", self._get_rank_category(rank_int))
                            point.field("coingecko_rank", rank_int)
                        except (ValueError, TypeError):
                            logger.debug(f"⚠️ Ranking inválido para {symbol}: {coingecko_rank}")
                    
                    # ARREGLADO: Campos de datos con validación mejorada
                    if price is not None:
                        try:
                            price_float = float(price)
                            if price_float >= 0:  # Permitir precio 0
                                point.field("price", price_float)
                        except (ValueError, TypeError):
                            logger.debug(f"⚠️ Precio inválido para {symbol}: {price}")
                    
                    if market_cap is not None:
                        try:
                            market_cap_float = float(market_cap)
                            if market_cap_float >= 0:  # Permitir market cap 0
                                point.field("market_cap", market_cap_float)
                        except (ValueError, TypeError):
                            logger.debug(f"⚠️ Market cap inválido para {symbol}: {market_cap}")
                    
                    # Campos calculados
                    point.field("data_quality_score", self._calculate_data_quality(price, market_cap))
                    
                    point.time(timestamp_dt)
                    points.append(point)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error procesando punto de datos para {symbol}: {e}")
                    continue
            
            if points:
                logger.info(f"🔄 Escribiendo {len(points)} puntos del rango CoinGecko a InfluxDB...")
                
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
    
    def _get_rank_category(self, rank) -> str:
        """Categoriza el ranking de CoinGecko con validación"""
        try:
            # ARREGLADO: Validar y convertir rank antes de usar
            if rank is None:
                return "unranked"
            
            rank_int = int(float(rank))
            
            if rank_int <= 10:
                return "top_10"
            elif rank_int <= 50:
                return "top_50"
            elif rank_int <= 100:
                return "top_100"
            elif rank_int <= 500:
                return "top_500"
            else:
                return "others"
        except (ValueError, TypeError):
            return "unranked"
    
    def _calculate_data_quality(self, price, market_cap) -> float:
        """Calcula un score de calidad de datos con validación"""
        score = 0.0
        
        # ARREGLADO: Validar valores antes de usar
        try:
            if price is not None and float(price) > 0:
                score += 0.5
        except (ValueError, TypeError):
            pass
        
        try:
            if market_cap is not None and float(market_cap) > 0:
                score += 0.5
        except (ValueError, TypeError):
            pass
        
        return score
    
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
        
        # Manejadores de base de datos con esquema normalizado
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
        """Conectar a PostgreSQL e InfluxDB con esquema normalizado"""
        postgres_connected = False
        influx_connected = False
        
        try:
            postgres_connected = self.postgres_manager.connect()
            if postgres_connected:
                # Mostrar estadísticas iniciales de rangos
                stats = self.postgres_manager.get_coingecko_range_scraping_stats()
                if stats:
                    logger.info("📊 Estado actual scraping rangos CoinGecko:")
                    for status, data in stats.items():
                        logger.info(f"   {status}: {data['count']} cryptos, {data['total_points']} puntos, {data['with_data']} con datos")
        except Exception as e:
            logger.error(f"❌ No se pudo conectar a PostgreSQL: {e}")
        
        try:
            influx_connected = self.influx_manager.connect()
        except Exception as e:
            logger.error(f"❌ No se pudo conectar a InfluxDB: {e}")
        
        return postgres_connected, influx_connected
    
    def load_cryptocurrencies(self) -> List[Dict]:
        """Carga lista de criptomonedas CoinGecko desde esquema normalizado"""
        try:
            cryptocurrencies = self.postgres_manager.get_coingecko_cryptocurrencies_for_ranges(limit=self.crypto_limit)
            
            if not cryptocurrencies:
                logger.warning("⚠️ No se encontraron criptomonedas CoinGecko en PostgreSQL")
                logger.info("💡 Intentando cargar desde archivo JSON como respaldo...")
                
                # Fallback al archivo JSON si existe
                try:
                    with open("criptomonedas.json", 'r', encoding='utf-8') as f:
                        json_cryptos = json.load(f)
                        # Adaptar formato para esquema normalizado
                        cryptocurrencies = []
                        for crypto in json_cryptos[:self.crypto_limit] if self.crypto_limit else json_cryptos:
                            adapted = {
                                'nombre': crypto.get('nombre', ''),
                                'simbolo': crypto.get('simbolo', ''),
                                'enlace': crypto.get('enlace', ''),
                                'slug': crypto.get('enlace', '').split('/')[-1] if crypto.get('enlace') else '',
                                'scraping_status': 'pending',
                                'priority_category': 'URGENT'
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
        """Obtiene las fechas disponibles del archivo de valores generales con validación"""
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
                    try:
                        # ARREGLADO: Validar timestamp antes de conversión
                        timestamp_num = float(timestamp)
                        # Convertir timestamp a fecha
                        dt = datetime.fromtimestamp(timestamp_num / 1000)
                        date_str = dt.strftime('%Y-%m-%d')
                        dates.add(date_str)
                    except (ValueError, TypeError, OSError):
                        logger.debug(f"⚠️ Timestamp inválido para {symbol}: {timestamp}")
                        continue
            
            logger.info(f"📅 Encontradas {len(dates)} fechas únicas para {symbol}")
            return dates
            
        except Exception as e:
            logger.error(f"❌ Error leyendo fechas de {symbol}: {e}")
            return set()
    
    def timestamp_for_date(self, date_str: str, is_end: bool = False) -> int:
        """Convierte fecha string a timestamp Unix con validación"""
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
        """Combina datos de precios y capitalización por timestamp para un rango con validaciones robustas"""
        combined_data = []
        
        # ARREGLADO: Convertir market_cap_data a diccionario con validación
        market_cap_dict = {}
        if market_cap_data:
            for item in market_cap_data:
                try:
                    if len(item) >= 2 and item[0] is not None:
                        timestamp = int(float(item[0]))  # Validar antes de convertir
                        market_cap_dict[timestamp] = item[1]
                except (ValueError, TypeError, IndexError):
                    continue  # Saltar items inválidos
        
        # ARREGLADO: Procesar datos de precios con validación
        if price_data:
            for price_item in price_data:
                try:
                    if len(price_item) >= 2 and price_item[0] is not None:
                        timestamp = int(float(price_item[0]))  # Validar antes de convertir
                        price = price_item[1]
                        market_cap = market_cap_dict.get(timestamp, None)
                        
                        # Solo añadir si tenemos datos válidos
                        if price is not None or market_cap is not None:
                            combined_data.append({
                                'timestamp': timestamp,
                                'price': price,
                                'market_cap': market_cap
                            })
                except (ValueError, TypeError, IndexError):
                    continue  # Saltar items inválidos
        
        logger.info(f"✅ Combinados {len(combined_data)} puntos de datos válidos para rango")
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
            crypto_data['data'].sort(key=lambda x: x.get('timestamp', 0))
            
            # Guardar archivo consolidado
            if self.save_crypto_daily_data(symbol, crypto_data):
                logger.debug(f"📁 Respaldo JSON actualizado para {symbol} (hasta {end_date})")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Error añadiendo datos de rango de {symbol} (hasta {end_date}): {e}")
            return False
    
    def download_range_data_selenium(self, url_name: str, start_date: str, end_date: str, data_type: str) -> Optional[List]:
        """Descarga datos para un rango de fechas usando Selenium con validación de timestamps"""
        timestamp_from = self.timestamp_for_date(start_date)
        timestamp_to = self.timestamp_for_date(end_date, True)
        
        if timestamp_from == 0 or timestamp_to == 0:
            logger.error(f"❌ Timestamps inválidos para {url_name} - {start_date} a {end_date}")
            return None
        
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
    
    def process_cryptocurrency_ranges_normalized(self, crypto: Dict) -> Dict:
        """Procesa una criptomoneda por rangos usando esquema normalizado con todas las correcciones"""
        symbol = crypto.get('simbolo', '').upper()
        enlace = crypto.get('enlace', '')
        nombre = crypto.get('nombre', '')
        priority_category = crypto.get('priority_category', 'UNKNOWN')
        current_status = crypto.get('scraping_status', 'pending')
        crypto_id = crypto.get('crypto_id')  # IMPORTANTE: Obtener crypto_id
        
        if not symbol or not enlace:
            logger.warning(f"⚠️ Datos incompletos para {nombre}")
            return {'symbol': symbol, 'success': 0, 'failed': 0, 'skipped': 0}
        
        logger.info(f"\n📊 Procesando rangos {nombre} ({symbol}) - Prioridad: {priority_category}")
        logger.info(f"🔄 Estado actual: {current_status} | ID: {crypto_id}")
        
        start_time = time.time()
        
        # ARREGLADO: Marcar como en progreso usando crypto_id
        self.postgres_manager.update_coingecko_range_scraping_status(
            crypto_id=crypto_id, symbol=symbol, name=nombre, status='in_progress',
            notes=f'Iniciando descarga de rangos (prioridad: {priority_category})'
        )
        
        # Extraer nombre de URL
        url_name = self.extract_url_name(enlace)
        logger.info(f"🔍 URL name: {url_name}")
        
        # Obtener fechas disponibles del archivo general
        available_dates = self.get_available_dates_from_values(symbol)
        if not available_dates:
            logger.warning(f"⚠️ No hay fechas disponibles para {symbol}")
            self.postgres_manager.update_coingecko_range_scraping_status(
                crypto_id=crypto_id, symbol=symbol, name=nombre, status='error',
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
                self.postgres_manager.update_coingecko_range_scraping_status(
                    crypto_id=crypto_id, symbol=symbol, name=nombre, status='completed',
                    last_date=last_processed,
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
            self.postgres_manager.update_coingecko_range_scraping_status(
                crypto_id=crypto_id, symbol=symbol, name=nombre, status='completed',
                last_date=last_processed,
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
            range_start_time = time.time()
            
            try:
                logger.info(f"📦 [{i}/{len(date_ranges)}] Rango {symbol}: {start_date} → {end_date}")
                
                # Descargar datos de precios para el rango
                price_data = self.download_range_data_selenium(url_name, start_date, end_date, 'price_charts')
                if not price_data:
                    logger.error(f"❌ No se pudieron obtener datos de precios para {symbol} - {start_date} a {end_date}")
                    failed_count += 1
                    
                    # Log de error para este rango
                    if crypto_id:
                        range_duration = int(time.time() - range_start_time)
                        self.postgres_manager.create_range_scraping_log_entry(
                            crypto_id, False, start_date, end_date, 0,
                            'Error obteniendo datos de precios', range_duration
                        )
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
                    # Guardar en InfluxDB con esquema normalizado
                    influx_success = False
                    if self.influx_manager.write_api:
                        influx_success = self.influx_manager.save_coingecko_range_data(
                            crypto, start_date, end_date, combined_data
                        )
                    
                    # Guardar respaldo JSON
                    backup_success = self.add_range_data_to_crypto(symbol, end_date, combined_data)
                    
                    range_duration = int(time.time() - range_start_time)
                    
                    if influx_success or backup_success:
                        success_count += 1
                        total_points_saved += len(combined_data)
                        logger.info(f"✅ Rango completado: {symbol} hasta {end_date} ({len(combined_data)} puntos)")
                        
                        # ARREGLADO: Actualizar progreso usando crypto_id
                        self.postgres_manager.update_coingecko_range_scraping_status(
                            crypto_id=crypto_id, symbol=symbol, name=nombre,
                            status='in_progress', last_date=end_date, total_points=len(combined_data),
                            notes=f'Procesando rango {i}/{len(date_ranges)}: {start_date} → {end_date}'
                        )
                        
                        # Log de éxito para este rango
                        if crypto_id:
                            self.postgres_manager.create_range_scraping_log_entry(
                                crypto_id, True, start_date, end_date, len(combined_data),
                                None, range_duration
                            )
                    else:
                        failed_count += 1
                        logger.error(f"❌ Error guardando datos de {symbol} - {start_date} a {end_date}")
                        
                        # Log de error para este rango
                        if crypto_id:
                            self.postgres_manager.create_range_scraping_log_entry(
                                crypto_id, False, start_date, end_date, 0,
                                'Error guardando datos combinados', range_duration
                            )
                else:
                    failed_count += 1
                    logger.error(f"❌ No se generaron datos combinados para {symbol} - {start_date} a {end_date}")
                    
                    # Log de error para este rango
                    if crypto_id:
                        range_duration = int(time.time() - range_start_time)
                        self.postgres_manager.create_range_scraping_log_entry(
                            crypto_id, False, start_date, end_date, 0,
                            'No se generaron datos combinados', range_duration
                        )
                
                # Delay entre rangos
                self.random_delay()
                
            except Exception as e:
                logger.error(f"❌ Error procesando {symbol} - {start_date} a {end_date}: {e}")
                failed_count += 1
                
                # Log de error para este rango
                if crypto_id:
                    range_duration = int(time.time() - range_start_time)
                    self.postgres_manager.create_range_scraping_log_entry(
                        crypto_id, False, start_date, end_date, 0,
                        f'Error procesando rango: {str(e)[:200]}', range_duration
                    )
        
        # ARREGLADO: Actualizar estado final usando crypto_id
        if failed_count == 0 and success_count > 0:
            final_date = max(pending_dates) if pending_dates else last_processed
            self.postgres_manager.update_coingecko_range_scraping_status(
                crypto_id=crypto_id, symbol=symbol, name=nombre,
                status='completed', last_date=final_date, total_points=total_points_saved,
                notes=f'Descarga completada exitosamente. {success_count} rangos procesados, {total_points_saved} puntos totales'
            )
        elif success_count > 0:
            final_date = max([end for _, end in date_ranges[:success_count]]) if success_count > 0 else last_processed
            self.postgres_manager.update_coingecko_range_scraping_status(
                crypto_id=crypto_id, symbol=symbol, name=nombre,
                status='error', last_date=final_date, total_points=total_points_saved,
                notes=f'Descarga parcial. {success_count} rangos exitosos, {failed_count} fallidos'
            )
        else:
            self.postgres_manager.update_coingecko_range_scraping_status(
                crypto_id=crypto_id, symbol=symbol, name=nombre,
                status='error', last_date=last_processed, total_points=0,
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
        """Ejecuta el proceso completo de rangos con esquema normalizado"""
        
        logger.info(f"🚀 Iniciando descarga por rangos CoinGecko (esquema normalizado FINAL)")
        
        # Conectar a bases de datos
        postgres_connected, influx_connected = self.connect_databases()
        
        if not postgres_connected:
            logger.error("❌ No se pudo conectar a PostgreSQL - Proceso abortado")
            return
        
        if influx_connected:
            logger.info("✅ InfluxDB conectado - Los datos se guardarán en InfluxDB")
        else:
            logger.warning("⚠️ InfluxDB no disponible - Solo se guardarán respaldos JSON y estados en PostgreSQL")
        
        # Cargar criptomonedas CoinGecko desde esquema normalizado
        cryptocurrencies = self.load_cryptocurrencies()
        
        if not cryptocurrencies:
            logger.error("❌ No se encontraron criptomonedas CoinGecko para procesar")
            return
        
        logger.info(f"📊 Procesando {len(cryptocurrencies)} criptomonedas CoinGecko desde esquema normalizado")
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
                priority = crypto.get('priority_category', 'UNKNOWN')
                status = crypto.get('scraping_status', 'unknown')
                
                logger.info(f"\n[{i}/{len(cryptocurrencies)}] {priority} | {status} ======================")
                
                try:
                    # Decidir si procesar según prioridad
                    if priority == 'CURRENT' and status == 'completed':
                        logger.info(f"⏭️ Saltando {crypto.get('simbolo')} - Ya está actualizado")
                        total_stats['total_skipped'] += 1
                        continue
                    
                    result = self.process_cryptocurrency_ranges_normalized(crypto)
                    
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
        except Exception as e:
            logger.error(f"❌ Error inesperado durante el procesamiento: {e}")
        
        logger.info(f"\n\n📈 === RESUMEN FINAL ===")
        logger.info(f"🔢 Criptomonedas procesadas: {total_stats['processed']}")
        logger.info(f"✅ Rangos exitosos: {total_stats['total_success']}")
        logger.info(f"❌ Rangos fallidos: {total_stats['total_failed']}")
        logger.info(f"⏭️ Rangos saltados: {total_stats['total_skipped']}")
        logger.info(f"📊 Total de puntos guardados: {total_stats['total_points']}")
        logger.info(f"📊 Estados actualizados en PostgreSQL (esquema normalizado)")
        
        if influx_connected:
            logger.info(f"💾 Datos históricos guardados en InfluxDB (bucket: {self.influxdb_config.database})")
        
        logger.info(f"📁 Respaldos JSON en: {os.path.abspath(self.daily_dir)}")
        
        # Mostrar estadísticas finales usando esquema normalizado
        final_stats = self.postgres_manager.get_coingecko_range_scraping_stats()
        if final_stats:
            logger.info(f"\n📊 === ESTADO FINAL RANGOS COINGECKO ===")
            for status, data in final_stats.items():
                logger.info(f"{status}: {data['count']} cryptos, {data['total_points']} puntos, {data['with_data']} con datos")
    
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
        print("🚀 === Descargador de Rangos CoinGecko (VERSIÓN FINAL NORMALIZADA) ===")
        print("TODAS LAS CORRECCIONES APLICADAS:")
        print("✅ Esquema normalizado con separación por fuentes")
        print("✅ Manejo robusto de valores None en conversiones")
        print("✅ Identificación única por crypto_id (sin duplicados)")
        print("✅ Validaciones mejoradas de datos de entrada")
        print("✅ Logs detallados y tracking completo")
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