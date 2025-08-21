#!/usr/bin/env python3
"""
Script para descargar datos de criptomonedas en rangos de días desde CoinGecko usando Selenium
Adaptado al esquema normalizado de PostgreSQL con lógica de datos históricos hacia atrás
SOLO BASE DE DATOS - Sin archivos CSV/JSON

VERSIÓN NORMALIZADA COHERENTE:
- Misma lógica que SeleniumCryptoDataScraper
- Datos históricos hacia atrás desde oldest_data_fetched
- Solo PostgreSQL e InfluxDB (sin archivos)
- Compatible con InfluxDB 1.x y 2.x
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
        logging.FileHandler('coingecko_range_scraper_normalized.log'),
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
    port: int = int(os.getenv('INFLUXDB_EXTERNAL_PORT') or '8086')
    database: str = os.getenv('INFLUXDB_DB', 'quotes')
    token: str = os.getenv('INFLUXDB_TOKEN', '')
    org: str = os.getenv('INFLUXDB_ORG', 'CoinAdvisor')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        logger.info(f"🔧 InfluxDB Config: {self.host}:{self.port} | org='{self.org}', bucket='{self.database}'")

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
        logger.info(f"🔧 PostgreSQL Config: {self.host}:{self.port}/{self.database}")

class PostgreSQLManager:
    """Manejador de PostgreSQL con esquema normalizado para rangos"""
    
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
        """Obtiene criptomonedas CoinGecko con priorización para rangos (COHERENTE con datos históricos hacia atrás)"""
        if not self.connection:
            logger.error("❌ No hay conexión a PostgreSQL")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                # Consulta coherente con SeleniumCryptoDataScraper usando oldest_data_fetched
                sql = """
                SELECT 
                    c.id as crypto_id,
                    c.name,
                    c.symbol,
                    c.slug,
                    c.is_active,
                    
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
                    cg.next_fetch_priority,
                    cg.scraping_notes,
                    
                    -- Calcular días desde última actualización
                    CASE 
                        WHEN cg.last_fetch_attempt IS NOT NULL THEN
                            EXTRACT(DAY FROM (NOW() - cg.last_fetch_attempt))
                        ELSE 999
                    END as days_since_last_attempt,
                    
                    -- Determinar categoría de prioridad (COHERENTE: hacia atrás en el tiempo)
                    CASE 
                        WHEN cg.scraping_status = 'pending' OR cg.oldest_data_fetched IS NULL THEN 'URGENT'
                        WHEN cg.scraping_status = 'error' AND cg.fetch_error_count < 3 THEN 'RETRY'
                        WHEN cg.scraping_status = 'in_progress' AND 
                             cg.last_fetch_attempt < NOW() - INTERVAL '1 hour' THEN 'STUCK'
                        -- Para datos históricos: prioridad a cryptos con datos recientes (necesitan más historia)
                        WHEN cg.scraping_status = 'completed' AND 
                             (cg.oldest_data_fetched IS NULL OR 
                              cg.oldest_data_fetched > '2020-01-01') THEN 'UPDATE'
                        -- Cryptos con datos históricos antiguos (antes de 2020) están completas
                        WHEN cg.scraping_status = 'completed' AND 
                             cg.oldest_data_fetched <= '2020-01-01' THEN 'CURRENT'
                        ELSE 'UNKNOWN'
                    END as priority_category
                    
                FROM cryptos c
                INNER JOIN coingecko_cryptos cg ON c.id = cg.crypto_id
                WHERE c.is_active = true 
                AND cg.coin_url IS NOT NULL 
                AND cg.coin_url != ''
                ORDER BY 
                    -- Prioridad por categoría (COHERENTE: para datos históricos hacia atrás)
                    CASE 
                        WHEN cg.scraping_status = 'pending' OR cg.oldest_data_fetched IS NULL THEN 1
                        WHEN cg.scraping_status = 'in_progress' AND 
                             cg.last_fetch_attempt < NOW() - INTERVAL '1 hour' THEN 2
                        WHEN cg.scraping_status = 'error' AND cg.fetch_error_count < 3 THEN 3
                        -- Prioridad ALTA para cryptos con datos históricos recientes (necesitan más historia)
                        WHEN cg.scraping_status = 'completed' AND 
                             (cg.oldest_data_fetched IS NULL OR cg.oldest_data_fetched > '2020-01-01') THEN 4
                        -- Prioridad BAJA para cryptos con datos históricos completos (antes de 2020)
                        WHEN cg.scraping_status = 'completed' AND cg.oldest_data_fetched <= '2020-01-01' THEN 8
                        ELSE 6
                    END,
                    -- Prioridad secundaria: más reciente = mayor prioridad (necesita más historia)
                    cg.oldest_data_fetched DESC NULLS FIRST,
                    -- Prioridad terciaria por ranking
                    cg.coingecko_rank ASC NULLS LAST,
                    -- Última prioridad alfabética
                    c.symbol ASC
                """
                
                if limit:
                    sql += f" LIMIT {limit}"
                
                cursor.execute(sql)
                rows = cursor.fetchall()
                
                # Convertir a formato compatible
                cryptocurrencies = []
                priority_stats = {'URGENT': 0, 'STUCK': 0, 'RETRY': 0, 'UPDATE': 0, 'CURRENT': 0, 'UNKNOWN': 0}
                
                for row in rows:
                    # Construir URL del coin si no existe
                    coin_url = row['coin_url']
                    if not coin_url and row['slug']:
                        coin_url = f"https://www.coingecko.com/es/monedas/{row['slug']}"
                    
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
                        'oldest_data_fetched': row['oldest_data_fetched'],
                        'scraping_status': row['scraping_status'],
                        'total_data_points': row['total_data_points'] or 0,
                        'fetch_error_count': row['fetch_error_count'] or 0,
                        'priority_category': row['priority_category'],
                        'days_since_last_attempt': int(row['days_since_last_attempt'] or 0),
                        'scraping_notes': row['scraping_notes']
                    }
                    cryptocurrencies.append(crypto)
                    
                    # Contar estadísticas de prioridad
                    category = row['priority_category']
                    if category in priority_stats:
                        priority_stats[category] += 1
                
                logger.info(f"✅ Obtenidas {len(cryptocurrencies)} criptomonedas CoinGecko para rangos (esquema normalizado)")
                logger.info(f"📊 Distribución por prioridad (datos históricos hacia atrás): {priority_stats}")
                logger.info(f"🔍 URGENT: Sin datos históricos | UPDATE: Datos desde 2020+ | CURRENT: Datos desde <2020")
                
                return cryptocurrencies
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo criptomonedas CoinGecko para rangos: {e}")
            return []
    
    def update_coingecko_scraping_progress(self, crypto_id: int = None, symbol: str = None, name: str = None, 
                                         status: str = 'in_progress', total_points: int = 0, 
                                         oldest_date: str = None, latest_date: str = None, 
                                         notes: str = None):
        """Actualiza el progreso de scraping usando crypto_id (COHERENTE con SeleniumCryptoDataScraper)"""
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
                
                # Campos condicionales según éxito/fallo
                if status == 'completed':
                    update_fields.extend([
                        "fetch_error_count = 0",
                        "total_data_points = %s"
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
                
                # Notas descriptivas
                if notes:
                    update_fields.append("scraping_notes = %s")
                    params.append(notes)
                
                # Usar crypto_id directamente (método preferido)
                if crypto_id:
                    params.append(crypto_id)
                    where_clause = "WHERE crypto_id = %s"
                    identifier = f"crypto_id={crypto_id}"
                    
                elif symbol and name:
                    # Fallback: usar combinación de symbol y name
                    params.extend([symbol, name])
                    where_clause = """
                        WHERE crypto_id = (
                            SELECT id FROM cryptos 
                            WHERE symbol = %s AND name = %s 
                            LIMIT 1
                        )
                    """
                    identifier = f"{symbol} ({name})"
                    
                else:
                    logger.error("❌ No se proporcionó crypto_id válido para identificar la crypto")
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
                    logger.debug(f"✅ Progreso CoinGecko actualizado para {identifier}: {status}")
                else:
                    logger.warning(f"⚠️ No se encontró crypto para actualizar: {identifier}")
                
        except Exception as e:
            logger.error(f"❌ Error actualizando progreso CoinGecko: {e}")
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
            logger.error(f"❌ Error obteniendo estadísticas CoinGecko: {e}")
            return {}
    
    def close(self):
        """Cerrar conexión a PostgreSQL"""
        if self.connection:
            self.connection.close()
            logger.info("🔐 Conexión PostgreSQL cerrada")

class InfluxDBManager:
    """Manejador de InfluxDB para datos históricos por rangos (COHERENTE con SeleniumCryptoDataScraper)"""
    
    def __init__(self, config: InfluxDBConfig):
        self.config = config
        self.client = None
        self.write_api = None
        
    def connect(self):
        """Conectar a InfluxDB (compatible con 1.x y 2.x)"""
        try:
            # Para InfluxDB 1.x (sin token)
            if not self.config.token:
                # InfluxDB 1.x
                from influxdb import InfluxDBClient
                self.client = InfluxDBClient(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database
                )
                # Verificar conexión
                try:
                    self.client.ping()
                    logger.info(f"✅ Conectado a InfluxDB 1.x: {self.config.host}:{self.config.port}")
                    return True
                except Exception as e:
                    logger.error(f"❌ Error conectando a InfluxDB 1.x: {e}")
                    return False
            
            # InfluxDB 2.x (con token)
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
                                 combined_data: List[Dict]) -> Dict:
        """Guarda datos de rango CoinGecko en InfluxDB (COHERENTE con SeleniumCryptoDataScraper)"""
        if not combined_data:
            logger.warning("⚠️ No hay datos para guardar")
            return {'success': False, 'points_saved': 0}
        
        try:
            # Para InfluxDB 1.x
            if not self.config.token and hasattr(self.client, 'write_points'):
                return self._save_to_influxdb_1x(crypto_data, start_date, end_date, combined_data)
            
            # Para InfluxDB 2.x
            elif self.write_api:
                return self._save_to_influxdb_2x(crypto_data, start_date, end_date, combined_data)
            
            else:
                logger.error("❌ Cliente InfluxDB no configurado correctamente")
                return {'success': False, 'points_saved': 0}
                
        except Exception as e:
            logger.error(f"❌ Error guardando datos de rango: {e}")
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
                
                # CoinGecko timestamps están en milisegundos
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                timestamps.append(timestamp_dt)
                
                price = data_point.get('price')
                market_cap = data_point.get('market_cap')
                
                # Validar que tenemos al menos un dato válido
                if price is None and market_cap is None:
                    continue
                
                # Punto para InfluxDB 1.x
                point = {
                    "measurement": "coingecko_historical_ranges",
                    "tags": {
                        "symbol": symbol,
                        "name": name,
                        "slug": slug,
                        "source": "coingecko_ranges_normalized",
                        "range_start": start_date,
                        "range_end": end_date
                    },
                    "time": timestamp_dt.isoformat(),
                    "fields": {}
                }
                
                # Agregar campos válidos
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
                
                # Solo agregar si tiene campos válidos
                if point["fields"]:
                    points.append(point)
                    
            except Exception as e:
                logger.warning(f"⚠️ Error procesando punto de datos para {symbol}: {e}")
                continue
        
        if points:
            # Escribir en lotes
            batch_size = 1000
            points_written = 0
            
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                
                success = self.client.write_points(batch)
                if success:
                    points_written += len(batch)
                    logger.debug(f"✅ Lote escrito ({len(batch)} puntos)")
                else:
                    logger.error(f"❌ Error escribiendo lote {i//batch_size + 1}")
            
            # Calcular estadísticas
            oldest_date = min(timestamps).strftime('%Y-%m-%d') if timestamps else None
            latest_date = max(timestamps).strftime('%Y-%m-%d') if timestamps else None
            
            logger.info(f"✅ Guardados {points_written} puntos de rango para {symbol}")
            logger.info(f"📅 Rango temporal: {oldest_date} → {latest_date}")
            
            return {
                'success': True,
                'points_saved': points_written,
                'oldest_date': oldest_date,
                'latest_date': latest_date,
                'date_range_days': (max(timestamps) - min(timestamps)).days if len(timestamps) > 1 else 0
            }
        else:
            logger.warning(f"⚠️ No hay puntos válidos para insertar para {symbol}")
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
                
                # CoinGecko timestamps están en milisegundos
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                timestamps.append(timestamp_dt)
                
                price = data_point.get('price')
                market_cap = data_point.get('market_cap')
                
                # Validar que tenemos al menos un dato válido
                if price is None and market_cap is None:
                    continue
                
                # Crear punto para InfluxDB 2.x
                point = Point("coingecko_historical_ranges")
                point.tag("symbol", symbol)
                point.tag("name", name)
                point.tag("slug", slug)
                point.tag("source", "coingecko_ranges_normalized")
                point.tag("range_start", start_date)
                point.tag("range_end", end_date)
                
                # Agregar campos válidos
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
                logger.warning(f"⚠️ Error procesando punto de datos para {symbol}: {e}")
                continue
        
        if points:
            # Escribir en lotes
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
                logger.debug(f"✅ Lote escrito ({len(batch)} puntos)")
            
            # Calcular estadísticas
            oldest_date = min(timestamps).strftime('%Y-%m-%d') if timestamps else None
            latest_date = max(timestamps).strftime('%Y-%m-%d') if timestamps else None
            
            logger.info(f"✅ Guardados {points_written} puntos de rango para {symbol}")
            logger.info(f"📅 Rango temporal: {oldest_date} → {latest_date}")
            
            return {
                'success': True,
                'points_saved': points_written,
                'oldest_date': oldest_date,
                'latest_date': latest_date,
                'date_range_days': (max(timestamps) - min(timestamps)).days if len(timestamps) > 1 else 0
            }
        else:
            logger.warning(f"⚠️ No hay puntos válidos para insertar para {symbol}")
            return {'success': False, 'points_saved': 0}
    
    def close(self):
        """Cerrar conexión a InfluxDB"""
        if self.client:
            if hasattr(self.client, 'close'):
                self.client.close()
            logger.info("🔐 Conexión InfluxDB cerrada")

class SeleniumRangeCryptoDataScraper:
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
        
        # Manejadores de base de datos (COHERENTE con SeleniumCryptoDataScraper)
        self.influxdb_config = influxdb_config or InfluxDBConfig()
        self.postgres_config = postgres_config or PostgreSQLConfig()
        self.influx_manager = InfluxDBManager(self.influxdb_config)
        self.postgres_manager = PostgreSQLManager(self.postgres_config)
        
        self.setup_driver(headless)
    
    def setup_driver(self, headless: bool = True):
        """Configura el driver de Chrome con opciones anti-detección"""
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
        
        # Opciones anti-detección optimizadas
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
            raise
    
    def connect_databases(self):
        """Conectar a PostgreSQL e InfluxDB"""
        postgres_connected = False
        influx_connected = False
        
        try:
            postgres_connected = self.postgres_manager.connect()
            if postgres_connected:
                stats = self.postgres_manager.get_coingecko_scraping_stats()
                if stats:
                    logger.info("📊 Estado actual scraping CoinGecko:")
                    for status, data in stats.items():
                        logger.info(f"   {status}: {data['count']} cryptos, {data['total_points']} puntos")
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
                return []
            
            return cryptocurrencies
            
        except Exception as e:
            logger.error(f"❌ Error cargando criptomonedas: {e}")
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
            logger.error(f"❌ Error convirtiendo fecha {date_str}: {e}")
            return 0
    
    def download_range_data_selenium(self, url_name: str, start_date: str, end_date: str, data_type: str) -> Optional[List]:
        """Descarga datos para un rango de fechas usando Selenium"""
        timestamp_from = self.timestamp_for_date(start_date)
        timestamp_to = self.timestamp_for_date(end_date, True)
        
        if timestamp_from == 0 or timestamp_to == 0:
            logger.error(f"❌ Timestamps inválidos para {url_name} - {start_date} a {end_date}")
            return None
        
        url = f"https://www.coingecko.com/{data_type}/{url_name}/usd/custom.json?from={timestamp_from}&to={timestamp_to}"
        
        try:
            logger.info(f"🔄 Descargando {data_type} para {url_name} - {start_date} a {end_date}")
            
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
                return None
                
        except Exception as e:
            logger.error(f"❌ Error descargando datos para {url_name} - {start_date} a {end_date} ({data_type}): {e}")
            return None
    
    def combine_range_data(self, price_data: List, market_cap_data: List) -> List[Dict]:
        """Combina datos de precios y capitalización por timestamp para un rango"""
        combined_data = []
        
        # Convertir market_cap_data a diccionario para búsqueda rápida
        market_cap_dict = {}
        if market_cap_data:
            for item in market_cap_data:
                try:
                    if len(item) >= 2 and item[0] is not None:
                        timestamp = int(float(item[0]))
                        market_cap_dict[timestamp] = item[1]
                except (ValueError, TypeError, IndexError):
                    continue
        
        # Procesar datos de precios
        if price_data:
            for price_item in price_data:
                try:
                    if len(price_item) >= 2 and price_item[0] is not None:
                        timestamp = int(float(price_item[0]))
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
                    continue
        
        logger.info(f"✅ Combinados {len(combined_data)} puntos de datos válidos para rango")
        return combined_data
    
    def random_delay(self, min_delay: float = None, max_delay: float = None):
        """Delay aleatorio entre peticiones"""
        if min_delay is None:
            min_delay = self.delay
        if max_delay is None:
            max_delay = self.delay * 1.5
        
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def process_cryptocurrency_ranges_normalized(self, crypto: Dict) -> bool:
        """Procesa una criptomoneda por rangos hacia atrás desde oldest_data_fetched"""
        symbol = crypto.get('simbolo', '').upper()
        enlace = crypto.get('enlace', '')
        nombre = crypto.get('nombre', '')
        priority_category = crypto.get('priority_category', 'UNKNOWN')
        current_status = crypto.get('scraping_status', 'pending')
        crypto_id = crypto.get('crypto_id')
        
        if not symbol or not enlace:
            logger.warning(f"⚠️ Datos incompletos para {nombre}")
            return False
        
        logger.info(f"\n📊 Procesando rangos {nombre} ({symbol}) - Prioridad: {priority_category}")
        logger.info(f"🔄 Estado actual: {current_status} | ID: {crypto_id}")
        
        # Obtener fecha más antigua actual
        oldest_date_str = crypto.get('oldest_data_fetched')
        if oldest_date_str:
            logger.info(f"📅 Datos históricos actuales desde: {oldest_date_str}")
            logger.info(f"🔙 Continuando hacia atrás desde {oldest_date_str}")
            # Convertir a datetime para cálculos
            try:
                oldest_date = datetime.strptime(str(oldest_date_str), '%Y-%m-%d')
            except ValueError:
                logger.warning(f"⚠️ Fecha inválida en oldest_data_fetched: {oldest_date_str}, usando hoy")
                oldest_date = datetime.now()
        else:
            oldest_date = datetime.now()
            logger.info(f"📅 Sin datos históricos - empezando desde hoy hacia atrás")
        
        start_time = time.time()
        
        # Marcar como en progreso
        self.postgres_manager.update_coingecko_scraping_progress(
            crypto_id=crypto_id,
            symbol=symbol,
            name=nombre,
            status='in_progress',
            notes=f'Buscando datos hacia atrás desde {oldest_date.strftime("%Y-%m-%d")} (prioridad: {priority_category})'
        )
        
        # Extraer nombre de URL
        url_name = self.extract_url_name(enlace)
        logger.info(f"🔍 URL name: {url_name}")
        
        try:
            # Calcular rango hacia atrás desde oldest_data_fetched
            # End date: fecha más antigua actual (o un día antes si ya tenemos datos)
            if oldest_date_str:
                # Si ya tenemos datos, empezar un día antes de la fecha más antigua
                end_date = (oldest_date - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                # Si no tenemos datos, empezar desde hoy
                end_date = oldest_date.strftime('%Y-%m-%d')
            
            # Start date: ir hacia atrás N días desde end_date
            start_date_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=self.range_days - 1)
            start_date = start_date_dt.strftime('%Y-%m-%d')
            
            logger.info(f"📦 Procesando rango histórico {symbol}: {start_date} → {end_date}")
            logger.info(f"🔙 Buscando {self.range_days} días hacia atrás")
            
            # Validar que no vamos demasiado al pasado (límite razonable)
            min_date = datetime(2009, 1, 1)  # Bitcoin empezó en 2009
            if start_date_dt < min_date:
                logger.info(f"📅 Alcanzado límite histórico mínimo (2009-01-01) para {symbol}")
                # Ajustar start_date al límite mínimo
                start_date = min_date.strftime('%Y-%m-%d')
                
                # Si el rango resultante es muy pequeño, considerar completo
                if (datetime.strptime(end_date, '%Y-%m-%d') - min_date).days < 30:
                    logger.info(f"✅ {symbol} tiene datos históricos completos hasta los orígenes")
                    self.postgres_manager.update_coingecko_scraping_progress(
                        crypto_id=crypto_id,
                        symbol=symbol,
                        name=nombre,
                        status='completed',
                        oldest_date=min_date.strftime('%Y-%m-%d'),
                        notes='Datos históricos completos hasta el límite mínimo (2009)'
                    )
                    return True
            
            # Descargar datos de precios para el rango
            price_data = self.download_range_data_selenium(url_name, start_date, end_date, 'price_charts')
            if not price_data:
                logger.error(f"❌ No se pudieron obtener datos de precios para {symbol} ({start_date} → {end_date})")
                
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    symbol=symbol,
                    name=nombre,
                    status='error',
                    notes=f'Error obteniendo datos de precios para rango {start_date} → {end_date}'
                )
                
                return False
            
            # Delay entre peticiones
            self.random_delay()
            
            # Descargar datos de capitalización de mercado para el rango
            market_cap_data = self.download_range_data_selenium(url_name, start_date, end_date, 'market_cap')
            if not market_cap_data:
                logger.warning(f"⚠️ No se pudieron obtener datos de capitalización para {symbol} ({start_date} → {end_date})")
                market_cap_data = []
            
            # Combinar datos
            combined_data = self.combine_range_data(price_data, market_cap_data)
            
            if not combined_data:
                logger.error(f"❌ No hay datos combinados para {symbol} ({start_date} → {end_date})")
                
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    symbol=symbol,
                    name=nombre,
                    status='error',
                    notes=f'No se generaron datos combinados para rango {start_date} → {end_date}'
                )
                
                return False
            
            # Guardar SOLO en InfluxDB (sin archivos)
            influx_result = {'success': False, 'points_saved': 0}
            if self.influx_manager.client:
                influx_result = self.influx_manager.save_coingecko_range_data(
                    crypto, start_date, end_date, combined_data
                )
            
            duration = int(time.time() - start_time)
            
            # Calcular nueva fecha más antigua basada en los datos obtenidos
            if combined_data:
                # Encontrar el timestamp más antiguo de los datos obtenidos
                oldest_timestamp = min(item['timestamp'] for item in combined_data if item.get('timestamp'))
                new_oldest_date = datetime.fromtimestamp(oldest_timestamp / 1000).strftime('%Y-%m-%d')
                logger.info(f"🕰️ Nueva fecha más antigua encontrada: {new_oldest_date}")
            else:
                new_oldest_date = start_date
            
            # Actualizar estado en PostgreSQL
            if influx_result.get('success'):
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    symbol=symbol, 
                    name=nombre,
                    status='completed',
                    total_points=influx_result['points_saved'],
                    oldest_date=new_oldest_date,  # Actualizar con la fecha más antigua obtenida
                    latest_date=influx_result.get('latest_date'),
                    notes=f'Rango histórico exitoso: {influx_result["points_saved"]} puntos, '
                          f'rango {start_date} → {end_date}, '
                          f'nueva oldest_date: {new_oldest_date}, duración {duration}s'
                )
                
                logger.info(f"✅ {symbol}: Guardado en InfluxDB ({influx_result['points_saved']} puntos)")
                logger.info(f"📅 Rango procesado: {start_date} → {end_date}")
                logger.info(f"🕰️ Oldest_date actualizada a: {new_oldest_date}")
                logger.info(f"⏱️ Duración: {duration}s")
            else:
                error_msg = influx_result.get('error', 'Unknown error')
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    symbol=symbol,
                    name=nombre,
                    status='error',
                    notes=f'Error guardando rango {start_date} → {end_date} en InfluxDB: {error_msg[:200]}'
                )
                
                logger.warning(f"⚠️ {symbol}: No se pudo guardar en InfluxDB: {error_msg}")
            
            return influx_result.get('success', False)
            
        except Exception as e:
            duration = int(time.time() - start_time)
            error_msg = str(e)[:200]
            
            logger.error(f"❌ Error procesando {symbol}: {e}")
            self.postgres_manager.update_coingecko_scraping_progress(
                crypto_id=crypto_id,
                symbol=symbol,
                name=nombre,
                status='error',
                notes=f'Error durante procesamiento de rango: {error_msg}'
            )
            
            return False
    
    def run(self) -> None:
        """Ejecuta el proceso completo de rangos SOLO BASE DE DATOS"""
        
        logger.info(f"🚀 Iniciando scraper de rangos CoinGecko con esquema normalizado (SOLO BASE DE DATOS)")
        logger.info(f"📈 DATOS HISTÓRICOS: Continúa hacia atrás desde oldest_data_fetched de cada crypto")
        logger.info(f"🔙 LÓGICA: Si crypto tiene oldest_data_fetched, busca {self.range_days} días antes de esa fecha")
        
        # Conectar a bases de datos
        postgres_connected, influx_connected = self.connect_databases()
        
        if not postgres_connected:
            logger.error("❌ No se pudo conectar a PostgreSQL - Proceso abortado")
            return
        
        if influx_connected:
            logger.info("✅ InfluxDB conectado - Los datos se guardarán en InfluxDB")
        else:
            logger.warning("⚠️ InfluxDB no disponible - Solo se actualizarán estados en PostgreSQL")
        
        # Cargar criptomonedas CoinGecko desde esquema normalizado
        cryptocurrencies = self.load_cryptocurrencies()
        
        if not cryptocurrencies:
            logger.error("❌ No se encontraron criptomonedas CoinGecko para procesar")
            return
        
        logger.info(f"📊 Procesando {len(cryptocurrencies)} criptomonedas CoinGecko con priorización")
        if self.crypto_limit:
            logger.info(f"🔢 Límite aplicado: {self.crypto_limit} criptomonedas")
        
        logger.info(f"📦 Tamaño de rango: {self.range_days} días hacia atrás desde oldest_data_fetched")
        logger.info(f"🔙 Cada ejecución busca datos {self.range_days} días antes de la fecha más antigua conocida")
        logger.info(f"💾 SOLO BASE DE DATOS - Sin archivos CSV/JSON")
        
        # Estadísticas de ejecución
        successful = 0
        failed = 0
        skipped = 0
        
        try:
            for i, crypto in enumerate(cryptocurrencies, 1):
                priority = crypto.get('priority_category', 'UNKNOWN')
                status = crypto.get('scraping_status', 'unknown')
                
                print(f"\n[{i}/{len(cryptocurrencies)}] {priority} | {status} ", end="")
                
                try:
                    # Decidir si procesar según prioridad HISTÓRICA (hacia atrás en el tiempo)
                    if priority == 'CURRENT' and status == 'completed':
                        oldest_date = crypto.get('oldest_data_fetched')
                        if oldest_date and oldest_date <= '2020-01-01':
                            logger.info(f"⏭️ Saltando {crypto.get('simbolo')} - Datos históricos completos hasta {oldest_date}")
                            skipped += 1
                            continue
                        else:
                            logger.info(f"🔄 Procesando {crypto.get('simbolo')} - Datos históricos incompletos (solo hasta {oldest_date})")
                    
                    if self.process_cryptocurrency_ranges_normalized(crypto):
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"❌ Error procesando {crypto.get('nombre', 'Desconocido')}: {e}")
                    failed += 1
                
                # Pausa entre criptomonedas
                if i < len(cryptocurrencies):
                    self.random_delay(self.delay * 1.5, self.delay * 2.5)
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Proceso interrumpido por el usuario")
        except Exception as e:
            logger.error(f"❌ Error inesperado durante el procesamiento: {e}")
        
        # Estadísticas finales
        logger.info(f"\n\n📈 === RESUMEN FINAL (SOLO BASE DE DATOS) ===")
        logger.info(f"✅ Exitosos: {successful}")
        logger.info(f"❌ Fallidos: {failed}")
        logger.info(f"⏭️ Saltados: {skipped}")
        logger.info(f"📊 Estados actualizados en PostgreSQL (esquema normalizado)")
        
        if influx_connected:
            logger.info(f"💾 Datos históricos guardados en InfluxDB (bucket: {self.influxdb_config.database})")
        
        logger.info(f"🚫 Sin archivos CSV/JSON - Solo base de datos")
        
        # Mostrar estadísticas finales
        final_stats = self.postgres_manager.get_coingecko_scraping_stats()
        if final_stats:
            logger.info(f"\n📊 === ESTADO FINAL SCRAPING COINGECKO ===")
            for status, data in final_stats.items():
                logger.info(f"{status}: {data['count']} cryptos, {data['total_points']} puntos")
    
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
        print("🚀 === CoinGecko Range Scraper (ESQUEMA NORMALIZADO COHERENTE) ===")
        print("Adaptado al mismo esquema que SeleniumCryptoDataScraper")
        print("📈 DATOS HISTÓRICOS: Continúa hacia atrás desde oldest_data_fetched de cada crypto")
        print("🔙 LÓGICA: Busca datos más antiguos partiendo desde donde se quedó cada crypto")
        print("SOLO BASE DE DATOS - Sin archivos CSV/JSON")
        print("Instalando dependencias:")
        print("pip install selenium webdriver-manager influxdb-client psycopg2-binary")
        print("ChromeDriver se descarga automáticamente\n")
        
        # Cargar variables de entorno
        load_env_file()
        
        # Configuración
        headless = input("¿Ejecutar en modo headless? (s/N): ").lower().startswith('s')
        delay = float(input("Delay entre peticiones en segundos (recomendado: 2-4): ") or "2.5")
        range_days = int(input("Días hacia atrás desde oldest_data_fetched (recomendado: 30): ") or "30")
        
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
        except Exception as e:
            logger.error(f"❌ Error en configuración de InfluxDB: {e}")
            logger.info("Continuando solo con PostgreSQL...")
            influxdb_config = None
        
        # Crear scraper
        scraper = SeleniumRangeCryptoDataScraper(
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