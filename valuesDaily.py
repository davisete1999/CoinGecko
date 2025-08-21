#!/usr/bin/env python3
"""
Script para obtener datos históricos de precios y capitalización de mercado de criptomonedas 
desde CoinGecko usando Selenium. Usa el esquema normalizado de PostgreSQL con tablas separadas
por fuente y guarda los datos históricos en InfluxDB.

ARREGLADO: Error de símbolos duplicados en update_coingecko_scraping_progress
ARREGLADO: Error de conversión int() con valores None - manejo robusto de datos faltantes
"""

import json
import os
import time
import random
import logging
from urllib.parse import urlparse
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone, date
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
    user: str = os.getenv('POSTGRES_USER', 'crypto-user')
    password: str = os.getenv('POSTGRES_PASSWORD', 'davisete453')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        logger.info(f"🔧 PostgreSQL Config: {self.host}:{self.port}/{self.database}")

class PostgreSQLManager:
    """Manejador de PostgreSQL con esquema normalizado por fuentes"""
    
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
    
    def get_coingecko_cryptocurrencies_with_priority(self, limit: Optional[int] = None) -> List[Dict]:
        """Obtiene criptomonedas de CoinGecko con priorización inteligente usando esquema normalizado"""
        if not self.connection:
            logger.error("❌ No hay conexión a PostgreSQL")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                # Consulta usando esquema normalizado con vista y joins
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
                    
                    -- Calcular días desde última actualización
                    CASE 
                        WHEN cg.last_fetch_attempt IS NOT NULL THEN
                            EXTRACT(DAY FROM (NOW() - cg.last_fetch_attempt))
                        ELSE 999
                    END as days_since_last_attempt,
                    
                    -- Determinar categoría de prioridad
                    CASE 
                        WHEN cg.scraping_status = 'pending' THEN 'URGENT'
                        WHEN cg.scraping_status = 'error' AND cg.fetch_error_count < 3 THEN 'RETRY'
                        WHEN cg.scraping_status = 'in_progress' AND 
                             cg.last_fetch_attempt < NOW() - INTERVAL '1 hour' THEN 'STUCK'
                        WHEN cg.scraping_status = 'completed' AND 
                             (cg.last_values_update IS NULL OR 
                              cg.last_values_update < CURRENT_DATE - INTERVAL '7 days') THEN 'UPDATE'
                        WHEN cg.scraping_status = 'completed' THEN 'CURRENT'
                        ELSE 'UNKNOWN'
                    END as priority_category
                    
                FROM cryptos c
                INNER JOIN coingecko_cryptos cg ON c.id = cg.crypto_id
                WHERE c.is_active = true 
                AND c.slug IS NOT NULL 
                AND c.slug != ''
                ORDER BY 
                    -- Prioridad por categoría
                    CASE 
                        WHEN cg.scraping_status = 'pending' THEN 1
                        WHEN cg.scraping_status = 'in_progress' AND 
                             cg.last_fetch_attempt < NOW() - INTERVAL '1 hour' THEN 2
                        WHEN cg.scraping_status = 'error' AND cg.fetch_error_count < 3 THEN 3
                        WHEN cg.scraping_status = 'completed' AND 
                             (cg.last_values_update IS NULL OR 
                              cg.last_values_update < CURRENT_DATE - INTERVAL '7 days') THEN 4
                        WHEN cg.scraping_status = 'completed' THEN 5
                        ELSE 6
                    END,
                    -- Prioridad secundaria por sistema calculado
                    cg.next_fetch_priority ASC,
                    -- Prioridad terciaria por ranking
                    cg.coingecko_rank ASC NULLS LAST,
                    -- Última prioridad alfabética
                    c.symbol ASC
                """
                
                if limit:
                    sql += f" LIMIT {limit}"
                
                cursor.execute(sql)
                rows = cursor.fetchall()
                
                # Convertir a formato compatible y calcular estadísticas
                cryptocurrencies = []
                priority_stats = {'URGENT': 0, 'STUCK': 0, 'RETRY': 0, 'UPDATE': 0, 'CURRENT': 0, 'UNKNOWN': 0}
                
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
                        'days_since_last_attempt': int(row['days_since_last_attempt'] or 0),
                        'scraping_notes': row['scraping_notes']
                    }
                    cryptocurrencies.append(crypto)
                    
                    # Contar estadísticas de prioridad
                    category = row['priority_category']
                    if category in priority_stats:
                        priority_stats[category] += 1
                
                logger.info(f"✅ Obtenidas {len(cryptocurrencies)} criptomonedas CoinGecko desde PostgreSQL")
                logger.info(f"📊 Distribución por prioridad: {priority_stats}")
                
                return cryptocurrencies
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo criptomonedas CoinGecko desde PostgreSQL: {e}")
            return []
    
    def create_or_update_coingecko_crypto(self, crypto_data: Dict) -> Tuple[int, int]:
        """Crea o actualiza crypto en esquema normalizado y retorna (crypto_id, coingecko_id)"""
        if not self.connection:
            return None, None
        
        try:
            with self.connection.cursor() as cursor:
                # Usar función auxiliar para obtener o crear crypto principal
                cursor.execute(
                    "SELECT get_or_create_crypto(%s, %s, %s)",
                    (crypto_data['nombre'], crypto_data['simbolo'], crypto_data['slug'])
                )
                crypto_id = cursor.fetchone()[0]
                
                # Insertar o actualizar datos específicos de CoinGecko
                cursor.execute("""
                    INSERT INTO coingecko_cryptos (
                        crypto_id, coingecko_rank, coingecko_url, icon_url, coin_url,
                        tags, badges, scraping_status, scraping_notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (crypto_id) DO UPDATE SET
                        coingecko_rank = EXCLUDED.coingecko_rank,
                        coingecko_url = EXCLUDED.coingecko_url,
                        icon_url = EXCLUDED.icon_url,
                        coin_url = EXCLUDED.coin_url,
                        tags = EXCLUDED.tags,
                        badges = EXCLUDED.badges,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (
                    crypto_id,
                    crypto_data.get('coingecko_rank'),
                    crypto_data.get('coingecko_url'),
                    crypto_data.get('icon_url'),
                    crypto_data.get('enlace'),
                    crypto_data.get('tags', []),
                    crypto_data.get('badges', []),
                    'pending',
                    'Crypto añadida desde scraper histórico'
                ))
                
                coingecko_id = cursor.fetchone()[0]
                self.connection.commit()
                
                logger.debug(f"✅ Crypto creada/actualizada: {crypto_data['simbolo']} (crypto_id: {crypto_id}, coingecko_id: {coingecko_id})")
                return crypto_id, coingecko_id
                
        except Exception as e:
            logger.error(f"❌ Error creando/actualizando crypto {crypto_data.get('simbolo', 'UNKNOWN')}: {e}")
            if self.connection:
                self.connection.rollback()
            return None, None
    
    def update_coingecko_scraping_progress(self, crypto_id: int = None, symbol: str = None, name: str = None, 
                                         status: str = 'in_progress', total_points: int = 0, 
                                         oldest_date: str = None, latest_date: str = None, 
                                         notes: str = None):
        """
        Actualiza el progreso de scraping en tabla coingecko_cryptos
        
        ARREGLADO: Usa crypto_id directamente cuando está disponible para evitar duplicados por símbolo
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
                
                elif status == 'in_progress':
                    # No incrementar error count para in_progress
                    pass
                
                # Notas descriptivas
                if notes:
                    update_fields.append("scraping_notes = %s")
                    params.append(notes)
                
                # SOLUCIÓN AL ERROR: Usar crypto_id directamente cuando esté disponible
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
                    logger.debug(f"✅ Progreso CoinGecko actualizado para {identifier}: {status}")
                else:
                    logger.warning(f"⚠️ No se encontró crypto para actualizar: {identifier}")
                
        except Exception as e:
            logger.error(f"❌ Error actualizando progreso CoinGecko: {e}")
            if self.connection:
                self.connection.rollback()
    
    
    
    def get_coingecko_scraping_stats(self) -> Dict:
        """Obtiene estadísticas de scraping de CoinGecko usando consultas directas"""
        if not self.connection:
            return {}
        
        try:
            with self.connection.cursor() as cursor:
                # Consulta directa a las tablas base sin depender de vistas
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
    """Manejador de InfluxDB para datos históricos de CoinGecko"""
    
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
    
    def save_coingecko_historical_data(self, crypto_data: Dict, combined_data: List[Dict]) -> Dict:
        """Guarda datos históricos de CoinGecko en InfluxDB con metadatos específicos"""
        if not combined_data or not self.write_api:
            logger.warning("⚠️ No hay datos o write_api no disponible")
            return {'success': False, 'points_saved': 0}
        
        try:
            points = []
            timestamps = []
            
            symbol = crypto_data.get('simbolo', 'UNKNOWN')
            name = crypto_data.get('nombre', 'Unknown')
            slug = crypto_data.get('slug', '')
            coingecko_rank = crypto_data.get('coingecko_rank')
            
            logger.info(f"🔄 Preparando {len(combined_data)} puntos históricos CoinGecko para {symbol}...")
            
            for data_point in combined_data:
                try:
                    # ARREGLADO: Validar timestamp antes de conversión
                    timestamp_ms = data_point.get('timestamp')
                    if timestamp_ms is None or timestamp_ms == 0:
                        logger.debug(f"⚠️ Timestamp inválido para {symbol}: {timestamp_ms}")
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
                    timestamps.append(timestamp_dt)
                    
                    price = data_point.get('price')
                    market_cap = data_point.get('market_cap')
                    
                    # Validar que tenemos al menos un dato válido
                    if price is None and market_cap is None:
                        continue
                    
                    # Crear punto para InfluxDB con tags específicos de CoinGecko
                    point = Point("coingecko_historical")
                    point.tag("symbol", symbol)
                    point.tag("name", name)
                    point.tag("slug", slug)
                    point.tag("source", "coingecko_full_historical")
                    
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
                logger.info(f"🔄 Escribiendo {len(points)} puntos históricos CoinGecko a InfluxDB...")
                
                # Escribir en lotes para mejor rendimiento
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
                    logger.debug(f"✅ Lote {i//batch_size + 1} escrito ({len(batch)} puntos)")
                
                # Calcular estadísticas de fechas
                oldest_date = min(timestamps).strftime('%Y-%m-%d') if timestamps else None
                latest_date = max(timestamps).strftime('%Y-%m-%d') if timestamps else None
                
                logger.info(f"✅ Guardados {points_written} puntos históricos CoinGecko para {symbol}")
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
                
        except Exception as e:
            logger.error(f"❌ Error guardando datos históricos CoinGecko para {symbol}: {e}")
            return {'success': False, 'points_saved': 0, 'error': str(e)}
    
    def _get_rank_category(self, rank) -> str:
        """Categoriza el ranking de CoinGecko"""
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
        """Calcula un score de calidad de datos"""
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
        """Conectar a PostgreSQL e InfluxDB con verificación de esquema normalizado"""
        postgres_connected = False
        influx_connected = False
        
        try:
            postgres_connected = self.postgres_manager.connect()
            if postgres_connected:
                # Mostrar estadísticas iniciales usando vistas del esquema normalizado
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
    
    def random_delay(self, min_delay: float = 1.0, max_delay: float = 3.0):
        """
        Implementa un delay aleatorio entre requests
        """
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def load_cryptocurrencies(self) -> List[Dict]:
        """Carga lista de criptomonedas CoinGecko desde esquema normalizado"""
        try:
            cryptocurrencies = self.postgres_manager.get_coingecko_cryptocurrencies_with_priority(limit=self.crypto_limit)
            
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
        market_cap_dict = {}
        if market_cap_data:
            for item in market_cap_data:
                try:
                    if len(item) >= 2 and item[0] is not None:
                        timestamp = int(float(item[0]))  # ARREGLADO: Validar antes de convertir
                        market_cap_dict[timestamp] = item[1]
                except (ValueError, TypeError, IndexError):
                    continue  # Saltar items inválidos
        
        # Procesar datos de precios
        if price_data:
            for price_item in price_data:
                try:
                    if len(price_item) >= 2 and price_item[0] is not None:
                        # ARREGLADO: Validar timestamp antes de convertir
                        timestamp = int(float(price_item[0]))
                        price = price_item[1]
                        market_cap = market_cap_dict.get(timestamp)
                        
                        # Solo añadir si tenemos datos válidos
                        if price is not None or market_cap is not None:
                            combined_data.append({
                                'timestamp': timestamp,
                                'price': price,
                                'market_cap': market_cap
                            })
                except (ValueError, TypeError, IndexError):
                    continue  # Saltar items inválidos
        
        logger.info(f"✅ Combinados {len(combined_data)} puntos de datos válidos")
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
                    'total_records': len(combined_data),
                    'scraped_at': datetime.now().isoformat(),
                    'source': 'coingecko_normalized_schema'
                }, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"📁 Respaldo JSON guardado para {symbol} ({len(combined_data)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error al guardar respaldo JSON para {symbol}: {e}")
            return False
    
    def process_cryptocurrency_normalized(self, crypto: Dict) -> bool:
        """Procesa criptomoneda usando esquema normalizado"""
        symbol = crypto.get('simbolo', '').upper()
        enlace = crypto.get('enlace', '')
        nombre = crypto.get('nombre', '')
        priority_category = crypto.get('priority_category', 'UNKNOWN')
        current_status = crypto.get('scraping_status', 'pending')
        error_count = crypto.get('fetch_error_count', 0)
        crypto_id = crypto.get('crypto_id')  # IMPORTANTE: Obtener crypto_id
        
        if not symbol or not enlace:
            logger.warning(f"⚠️ Datos incompletos para {nombre}")
            return False
        
        logger.info(f"\n📊 Procesando {nombre} ({symbol}) - Prioridad: {priority_category}")
        logger.info(f"🔄 Estado actual: {current_status} | Errores: {error_count} | ID: {crypto_id}")
        
        start_time = time.time()
        
        # ARREGLADO: Marcar como en progreso usando crypto_id cuando esté disponible
        self.postgres_manager.update_coingecko_scraping_progress(
            crypto_id=crypto_id,
            symbol=symbol,
            name=nombre,
            status='in_progress',
            notes=f'Iniciando descarga histórica (prioridad: {priority_category})'
        )
        
        # Extraer nombre de URL
        url_name = self.extract_url_name(enlace)
        logger.info(f"🔍 Nombre URL extraído: {url_name}")
        
        try:
            # Obtener datos de precios
            price_data = self.get_crypto_data(url_name, 'price_charts')
            if not price_data:
                logger.error(f"❌ No se pudieron obtener datos de precios para {symbol}")
                duration = int(time.time() - start_time)
                
                # ARREGLADO: Usar crypto_id en la actualización
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    symbol=symbol,
                    name=nombre,
                    status='error',
                    notes='Error obteniendo datos de precios desde CoinGecko'
                )
                
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
                duration = int(time.time() - start_time)
                
                # ARREGLADO: Usar crypto_id en la actualización
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    symbol=symbol,
                    name=nombre,
                    status='error',
                    notes='No se generaron datos combinados válidos'
                )
                
                return False
            
            # Guardar en InfluxDB con esquema específico de CoinGecko
            influx_result = {'success': False, 'points_saved': 0}
            if self.influx_manager.write_api:
                influx_result = self.influx_manager.save_coingecko_historical_data(crypto, combined_data)
            
            # Guardar respaldo JSON
            backup_success = self.save_crypto_data_backup(symbol, combined_data)
            
            duration = int(time.time() - start_time)
            
            # ARREGLADO: Actualizar estado en PostgreSQL con información detallada usando crypto_id
            if influx_result.get('success'):
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    symbol=symbol, 
                    name=nombre,
                    status='completed',
                    total_points=influx_result['points_saved'],
                    oldest_date=influx_result.get('oldest_date'),
                    latest_date=influx_result.get('latest_date'),
                    notes=f'Descarga exitosa: {influx_result["points_saved"]} puntos, '
                          f'rango {influx_result.get("date_range_days", 0)} días'
                )
                
                logger.info(f"✅ {symbol}: Guardado en InfluxDB ({influx_result['points_saved']} puntos)")
                logger.info(f"📅 Rango: {influx_result.get('oldest_date')} → {influx_result.get('latest_date')}")
            else:
                error_msg = influx_result.get('error', 'Unknown error')
                # ARREGLADO: Usar crypto_id en la actualización de error
                self.postgres_manager.update_coingecko_scraping_progress(
                    crypto_id=crypto_id,
                    symbol=symbol,
                    name=nombre,
                    status='error',
                    notes=f'Error guardando en InfluxDB: {error_msg}'
                )
                
                logger.warning(f"⚠️ {symbol}: No se pudo guardar en InfluxDB")
            
            return influx_result.get('success') or backup_success
            
        except Exception as e:
            duration = int(time.time() - start_time)
            error_msg = str(e)[:200]
            
            logger.error(f"❌ Error procesando {symbol}: {e}")
            # ARREGLADO: Usar crypto_id en la actualización de error
            self.postgres_manager.update_coingecko_scraping_progress(
                crypto_id=crypto_id,
                symbol=symbol,
                name=nombre,
                status='error',
                notes=f'Error durante procesamiento: {error_msg}'
            )
            
            return False
    
    def run(self) -> None:
        """Ejecuta el proceso completo usando esquema normalizado"""
        
        logger.info(f"🚀 Iniciando scraper CoinGecko con esquema normalizado (VERSIÓN ARREGLADA - None values)")
        
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
        
        logger.info(f"📊 Procesando {len(cryptocurrencies)} criptomonedas CoinGecko con priorización")
        if self.crypto_limit:
            logger.info(f"🔢 Límite aplicado: {self.crypto_limit} criptomonedas")
        
        logger.info(f"📁 Los archivos de respaldo se guardarán en: {os.path.abspath(self.output_dir)}")
        
        # Estadísticas de ejecución
        successful = 0
        failed = 0
        skipped = 0
        total_points_saved = 0
        
        try:
            for i, crypto in enumerate(cryptocurrencies, 1):
                priority = crypto.get('priority_category', 'UNKNOWN')
                status = crypto.get('scraping_status', 'unknown')
                
                print(f"\n[{i}/{len(cryptocurrencies)}] {priority} | {status} ", end="")
                
                try:
                    # Decidir si procesar según prioridad
                    if priority == 'CURRENT' and status == 'completed':
                        logger.info(f"⏭️ Saltando {crypto.get('simbolo')} - Ya está actualizado")
                        skipped += 1
                        continue
                    
                    if self.process_cryptocurrency_normalized(crypto):
                        successful += 1
                        # Estimar puntos guardados basado en respuesta
                        total_points_saved += crypto.get('total_data_points', 0)
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
        
        # Estadísticas finales
        logger.info(f"\n\n📈 === RESUMEN FINAL ===")
        logger.info(f"✅ Exitosos: {successful}")
        logger.info(f"❌ Fallidos: {failed}")
        logger.info(f"⏭️ Saltados: {skipped}")
        logger.info(f"📊 Estados actualizados en PostgreSQL (esquema normalizado)")
        
        if influx_connected:
            logger.info(f"💾 Datos históricos guardados en InfluxDB (bucket: {self.influxdb_config.database})")
        
        logger.info(f"📁 Respaldos JSON en: {os.path.abspath(self.output_dir)}")
        
        # Mostrar estadísticas finales usando vistas del esquema normalizado
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
        print("🚀 === CoinGecko Historical Scraper (ARREGLADO - None values + duplicados) ===")
        print("Con tracking avanzado y separación por fuentes de datos")
        print("CORRECCIÓN: Resuelto error de símbolos duplicados usando crypto_id")
        print("CORRECCIÓN: Manejo robusto de valores None en conversiones numéricas")
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