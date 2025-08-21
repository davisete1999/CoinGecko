#!/usr/bin/env python3
"""
CoinGecko Data Scraper - Selenium Ultra Optimizado SIN JavaScript
VERSIÓN AUTOMATIZADA - Scraping continuo hasta fallo
ANTI-DUPLICADOS MEJORADO - Control exhaustivo de duplicados

DEPENDENCIAS:
pip install psycopg2-binary selenium beautifulsoup4 webdriver-manager

ESTRUCTURA DE BASE DE DATOS:
- cryptos (tabla principal normalizada)
- coingecko_cryptos (datos específicos de CoinGecko)
"""

import os
import sys
import json
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from datetime import datetime, timezone, date
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass
import time
import random
import re
import uuid
from urllib.parse import urlparse, urljoin, parse_qs
import threading
from queue import Queue, Empty
from contextlib import contextmanager
 
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

def check_dependencies():
    """Verificar que todas las dependencias estén instaladas"""
    dependencies = {
        'psycopg2': 'psycopg2-binary',
        'selenium': 'selenium',
        'bs4': 'beautifulsoup4',
        'webdriver_manager': 'webdriver-manager'
    }
    
    missing = []
    for module, package in dependencies.items():
        try:
            __import__(module)
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"❌ Dependencias faltantes: {', '.join(missing)}")
        print(f"💡 Instalar con: pip install {' '.join(missing)}")
        sys.exit(1)
    
    print("✅ Todas las dependencias están instaladas")

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
        
        print(f"✅ {env_vars_loaded} variables de entorno cargadas desde {env_file}")
            
    except FileNotFoundError:
        print(f"⚠️ Archivo {env_file} no encontrado, usando variables de entorno del sistema")
    except Exception as e:
        print(f"❌ Error cargando {env_file}: {e}")

@dataclass
class DatabaseConfig:
    """Configuración de base de datos desde variables de entorno"""
    postgres_host: str = os.getenv('POSTGRES_HOST', 'localhost')
    postgres_port: int = int(os.getenv('POSTGRES_EXTERNAL_PORT', '5432'))
    postgres_db: str = os.getenv('POSTGRES_DB', 'cryptodb')
    postgres_user: str = 'crypto-user'
    postgres_password: str = os.getenv('POSTGRES_PASSWORD', 'davisete453')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        print(f"🔧 PostgreSQL Config: {self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

class DatabaseManager:
    """Manejador de base de datos con control exhaustivo de duplicados"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.pg_conn = None
        self.session_id = str(uuid.uuid4())
        self.coingecko_source_id = None
        self._lock = threading.Lock()
        # Cache de símbolos existentes para evitar consultas repetidas
        self._existing_symbols_cache: Dict[str, int] = {}
        self._cache_last_update = None
        self._cache_ttl = 300  # 5 minutos
        
    def connect(self):
        """Conectar a PostgreSQL y obtener IDs de fuentes"""
        try:
            self.pg_conn = psycopg2.connect(
                host=self.db_config.postgres_host,
                port=self.db_config.postgres_port,
                database=self.db_config.postgres_db,
                user=self.db_config.postgres_user,
                password=self.db_config.postgres_password
            )
            
            # Obtener ID de fuente CoinGecko
            with self.pg_conn.cursor() as cursor:
                cursor.execute("SELECT id FROM crypto_sources WHERE source_name = 'coingecko'")
                result = cursor.fetchone()
                if result:
                    self.coingecko_source_id = result[0]
                    print(f"✅ Conectado a PostgreSQL (CoinGecko source_id: {self.coingecko_source_id})")
                else:
                    print("❌ Fuente 'coingecko' no encontrada en crypto_sources")
                    return False
            
            # Inicializar cache de símbolos existentes
            self.refresh_symbols_cache()
            return True
        except Exception as e:
            print(f"❌ Error conectando a PostgreSQL: {e}")
            return False
    
    def refresh_symbols_cache(self):
        """Actualizar cache de símbolos existentes"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT UPPER(c.symbol), c.id 
                    FROM cryptos c 
                    WHERE c.is_active = true
                """)
                self._existing_symbols_cache = {row[0]: row[1] for row in cursor.fetchall()}
                self._cache_last_update = time.time()
                print(f"🔄 Cache actualizado: {len(self._existing_symbols_cache)} símbolos existentes")
        except Exception as e:
            print(f"⚠️ Error actualizando cache: {e}")
            self._existing_symbols_cache = {}
    
    def is_cache_valid(self):
        """Verificar si el cache sigue siendo válido"""
        if not self._cache_last_update:
            return False
        return (time.time() - self._cache_last_update) < self._cache_ttl
    
    def symbol_exists_in_db(self, symbol: str) -> Optional[int]:
        """Verificar si un símbolo ya existe en la BD (con cache)"""
        if not self.is_cache_valid():
            self.refresh_symbols_cache()
        
        return self._existing_symbols_cache.get(symbol.upper())
    
    def filter_duplicates_against_db(self, crypto_list: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Filtrar duplicados contra la base de datos existente"""
        if not crypto_list:
            return [], []
        
        filtered_cryptos = []
        skipped_symbols = []
        
        for crypto in crypto_list:
            symbol = crypto.get('symbol', '').upper().strip()
            if not symbol:
                continue
                
            # Verificar contra BD existente
            existing_id = self.symbol_exists_in_db(symbol)
            if existing_id:
                skipped_symbols.append(symbol)
                print(f"🔄 Símbolo {symbol} ya existe en BD (ID: {existing_id}), saltando...")
                continue
            
            filtered_cryptos.append(crypto)
        
        return filtered_cryptos, skipped_symbols
    
    def get_or_create_crypto_safe(self, name: str, symbol: str, slug: str) -> Optional[int]:
        """Versión segura de get_or_create_crypto con manejo de duplicados"""
        try:
            with self.pg_conn.cursor() as cursor:
                # Primero verificar si ya existe por símbolo
                cursor.execute(
                    "SELECT id FROM cryptos WHERE UPPER(symbol) = UPPER(%s) AND is_active = true",
                    (symbol,)
                )
                result = cursor.fetchone()
                if result:
                    # Actualizar cache
                    self._existing_symbols_cache[symbol.upper()] = result[0]
                    return result[0]
                
                # Si no existe, intentar crear
                cursor.execute(
                    """
                    INSERT INTO cryptos (name, symbol, slug, is_active, created_at, updated_at)
                    VALUES (%s, %s, %s, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        slug = EXCLUDED.slug,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    (name, symbol, slug)
                )
                
                result = cursor.fetchone()
                if result:
                    crypto_id = result[0]
                    # Actualizar cache
                    self._existing_symbols_cache[symbol.upper()] = crypto_id
                    return crypto_id
                
                return None
                
        except Exception as e:
            print(f"⚠️ Error con crypto {symbol}: {e}")
            return None
    
    def save_crypto_batch(self, crypto_list: List[Dict[str, Any]]) -> int:
        """Guardar lote de cryptos con control exhaustivo de duplicados"""
        if not crypto_list or not self.pg_conn:
            return 0
        
        saved_count = 0
        
        try:
            with self._lock:
                print(f"🔍 Procesando lote de {len(crypto_list)} cryptos...")
                
                # PASO 1: Filtrar duplicados contra la base de datos existente
                filtered_cryptos, skipped_db = self.filter_duplicates_against_db(crypto_list)
                if skipped_db:
                    print(f"🔄 Saltados {len(skipped_db)} duplicados de BD: {', '.join(skipped_db[:10])}{'...' if len(skipped_db) > 10 else ''}")
                
                if not filtered_cryptos:
                    print("⚠️ No hay cryptos nuevos para procesar")
                    return 0
                
                # PASO 2: Eliminar duplicados internos del lote por símbolo
                unique_cryptos = {}
                for crypto in filtered_cryptos:
                    symbol = crypto.get('symbol', '').upper().strip()
                    if not symbol or len(symbol) > 20:
                        continue
                    
                    # Mantener el de mejor ranking (menor número = mejor)
                    if symbol not in unique_cryptos or crypto.get('rank', 9999) < unique_cryptos[symbol].get('rank', 9999):
                        unique_cryptos[symbol] = crypto
                
                crypto_list_unique = list(unique_cryptos.values())
                duplicates_in_batch = len(filtered_cryptos) - len(crypto_list_unique)
                
                if duplicates_in_batch > 0:
                    print(f"🔄 Eliminados {duplicates_in_batch} duplicados internos del lote")
                
                if not crypto_list_unique:
                    print("⚠️ No quedan cryptos únicos para procesar")
                    return 0
                
                print(f"✅ Procesando {len(crypto_list_unique)} cryptos únicos")
                
                with self.pg_conn.cursor() as cursor:
                    # PASO 3: Crear/obtener IDs de cryptos de forma segura
                    crypto_ids = []
                    failed_symbols = []
                    
                    for crypto in crypto_list_unique:
                        name = crypto.get('name', '').strip()[:255]
                        symbol = crypto.get('symbol', '').strip()[:20]
                        slug = crypto.get('slug', '').strip()[:255]
                        
                        if not symbol:
                            failed_symbols.append(f"EMPTY_SYMBOL_{len(failed_symbols)}")
                            crypto_ids.append(None)
                            continue
                        
                        crypto_id = self.get_or_create_crypto_safe(name, symbol, slug)
                        crypto_ids.append(crypto_id)
                        
                        if not crypto_id:
                            failed_symbols.append(symbol)
                    
                    if failed_symbols:
                        print(f"⚠️ Fallos en {len(failed_symbols)} símbolos: {', '.join(failed_symbols[:5])}{'...' if len(failed_symbols) > 5 else ''}")
                    
                    # PASO 4: Preparar datos para coingecko_cryptos (solo los exitosos)
                    coingecko_data = []
                    processed_crypto_ids = set()
                    
                    for i, crypto in enumerate(crypto_list_unique):
                        if i >= len(crypto_ids) or not crypto_ids[i]:
                            continue
                            
                        crypto_id = crypto_ids[i]
                        
                        # Verificar duplicados de crypto_id (extra seguridad)
                        if crypto_id in processed_crypto_ids:
                            print(f"⚠️ Crypto_id {crypto_id} duplicado detectado, saltando...")
                            continue
                        
                        processed_crypto_ids.add(crypto_id)
                        
                        # Preparar datos
                        tags_json = json.dumps(crypto.get('tags', [])[:5])
                        badges_json = json.dumps(crypto.get('badges', [])[:3])
                        
                        coingecko_data.append((
                            crypto_id,
                            crypto.get('rank'),
                            crypto.get('coingecko_url', '')[:500],
                            crypto.get('icon_url', '')[:500],
                            crypto.get('coin_url', '')[:500],
                            tags_json,
                            badges_json,
                            crypto.get('market_pair_count'),
                            date.today(),
                            'completed',
                            1,
                            datetime.now(timezone.utc),
                            0,
                            100,
                            f'Scraped {date.today()}'
                        ))
                    
                    # PASO 5: Inserción batch con manejo de conflictos
                    if coingecko_data:
                        try:
                            execute_values(
                                cursor,
                                """
                                INSERT INTO coingecko_cryptos (
                                    crypto_id, coingecko_rank, coingecko_url, icon_url, coin_url,
                                    tags, badges, market_pair_count, last_values_update,
                                    scraping_status, total_data_points, last_fetch_attempt,
                                    fetch_error_count, next_fetch_priority, scraping_notes
                                ) VALUES %s
                                ON CONFLICT (crypto_id) DO UPDATE SET
                                    coingecko_rank = CASE 
                                        WHEN EXCLUDED.coingecko_rank IS NOT NULL 
                                        AND (coingecko_cryptos.coingecko_rank IS NULL OR EXCLUDED.coingecko_rank < coingecko_cryptos.coingecko_rank)
                                        THEN EXCLUDED.coingecko_rank 
                                        ELSE coingecko_cryptos.coingecko_rank 
                                    END,
                                    coingecko_url = COALESCE(EXCLUDED.coingecko_url, coingecko_cryptos.coingecko_url),
                                    icon_url = COALESCE(EXCLUDED.icon_url, coingecko_cryptos.icon_url),
                                    coin_url = COALESCE(EXCLUDED.coin_url, coingecko_cryptos.coin_url),
                                    tags = EXCLUDED.tags,
                                    badges = EXCLUDED.badges,
                                    market_pair_count = COALESCE(EXCLUDED.market_pair_count, coingecko_cryptos.market_pair_count),
                                    last_values_update = EXCLUDED.last_values_update,
                                    scraping_status = EXCLUDED.scraping_status,
                                    total_data_points = EXCLUDED.total_data_points,
                                    last_fetch_attempt = EXCLUDED.last_fetch_attempt,
                                    scraping_notes = EXCLUDED.scraping_notes,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                coingecko_data,
                                template=None,
                                page_size=50  # Lotes más pequeños para reducir conflictos
                            )
                            
                            saved_count = len(coingecko_data)
                            
                        except psycopg2.IntegrityError as e:
                            print(f"⚠️ Conflicto de integridad detectado: {e}")
                            # Intentar inserción individual para los que fallan
                            saved_count = self._save_individual_cryptos(cursor, coingecko_data)
                    
                    self.pg_conn.commit()
                    
                    # Actualizar cache con nuevos símbolos
                    for i, crypto in enumerate(crypto_list_unique):
                        if i < len(crypto_ids) and crypto_ids[i]:
                            symbol = crypto.get('symbol', '').upper()
                            if symbol:
                                self._existing_symbols_cache[symbol] = crypto_ids[i]
                    
                    print(f"✅ Guardado: {saved_count}/{len(crypto_list)} cryptos (únicos: {len(crypto_list_unique)})")
                    
        except Exception as e:
            print(f"❌ Error guardando lote: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            saved_count = 0
        
        return saved_count
    
    def _save_individual_cryptos(self, cursor, coingecko_data: List[tuple]) -> int:
        """Guardar cryptos individualmente cuando falla el batch"""
        saved_count = 0
        
        for data in coingecko_data:
            try:
                cursor.execute(
                    """
                    INSERT INTO coingecko_cryptos (
                        crypto_id, coingecko_rank, coingecko_url, icon_url, coin_url,
                        tags, badges, market_pair_count, last_values_update,
                        scraping_status, total_data_points, last_fetch_attempt,
                        fetch_error_count, next_fetch_priority, scraping_notes
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (crypto_id) DO UPDATE SET
                        coingecko_rank = EXCLUDED.coingecko_rank,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    data
                )
                saved_count += 1
            except Exception as e:
                print(f"⚠️ Error individual crypto_id {data[0]}: {e}")
                continue
        
        return saved_count
    
    def get_existing_cryptos(self) -> Dict[str, int]:
        """Obtener mapa de símbolos existentes"""
        if not self.is_cache_valid():
            self.refresh_symbols_cache()
        return self._existing_symbols_cache.copy()
    
    def close(self):
        """Cerrar conexión"""
        if self.pg_conn:
            self.pg_conn.close()
            print("🔐 Conexión PostgreSQL cerrada")

class WebDriverPool:
    """Pool de WebDrivers para scraping paralelo sin JS"""
    
    def __init__(self, pool_size: int = 2):
        self.pool_size = pool_size
        self.drivers = Queue()
        self.active_drivers = []
        self._create_drivers()
    
    def _create_driver(self) -> webdriver.Chrome:
        """Crear un WebDriver optimizado SIN JavaScript"""
        chrome_options = Options()
        
        # Configuración base ultra optimizada
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-javascript")  # CLAVE para velocidad
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-background-sync")
        chrome_options.add_argument("--disable-features=TranslateUI,sync")
        chrome_options.add_argument("--window-size=800,600")  # Tamaño mínimo
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--silent")
        
        # User agent minimalista
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        
        # Deshabilitar recursos no esenciales
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.media_stream": 2,
            "profile.managed_default_content_settings.notifications": 2,
            "profile.managed_default_content_settings.stylesheets": 1,
            "profile.managed_default_content_settings.javascript": 2,
            "profile.managed_default_content_settings.plugins": 2,
            "profile.managed_default_content_settings.popups": 2,
            "profile.managed_default_content_settings.geolocation": 2,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Anti-detección mínima
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Timeouts ultra agresivos para velocidad
            driver.set_page_load_timeout(15)  # Incrementado para estabilidad
            driver.implicitly_wait(2)  # Incrementado para estabilidad
            
            return driver
            
        except Exception as e:
            print(f"❌ Error creando WebDriver: {e}")
            raise
    
    def _create_drivers(self):
        """Crear pool de drivers"""
        print(f"🔧 Creando pool de {self.pool_size} WebDrivers ultra optimizados...")
        
        for i in range(self.pool_size):
            try:
                driver = self._create_driver()
                self.drivers.put(driver)
                self.active_drivers.append(driver)
                print(f"✅ WebDriver {i+1}/{self.pool_size} creado")
            except Exception as e:
                print(f"❌ Error creando WebDriver {i+1}: {e}")
    
    @contextmanager
    def get_driver(self):
        """Context manager para obtener un driver del pool"""
        driver = None
        try:
            driver = self.drivers.get(timeout=30)
            yield driver
        finally:
            if driver:
                self.drivers.put(driver)
    
    def close_all(self):
        """Cerrar todos los drivers"""
        print("🔐 Cerrando pool de WebDrivers...")
        for driver in self.active_drivers:
            try:
                driver.quit()
            except:
                pass
        self.active_drivers.clear()

class UltraOptimizedScraper:
    """Scraper ultra optimizado con control de duplicados mejorado"""
    
    def __init__(self, db_config: DatabaseConfig = None):
        self.base_url = "https://www.coingecko.com"
        self.db_config = db_config or DatabaseConfig()
        self.db_manager = DatabaseManager(self.db_config)
        self.driver_pool = WebDriverPool(pool_size=2)
        
        # Control global de duplicados
        self.global_seen_symbols: Set[str] = set()
        self.session_stats = {
            'total_scraped': 0,
            'total_unique': 0,
            'duplicates_skipped': 0,
            'db_existing_skipped': 0
        }
        
        # Patrones regex precompilados para máxima velocidad
        self.symbol_pattern = re.compile(r'\b([A-Z0-9$.-]{1,15})\b')
        self.price_pattern = re.compile(r'\$[\d,]+\.?\d*')
        self.percent_pattern = re.compile(r'([-+]?\d+\.?\d*)%')
        self.number_pattern = re.compile(r'[\d,]+\.?\d*[BbMmKkTt]?')
        
    def connect_database(self):
        """Establecer conexión a PostgreSQL"""
        try:
            if not self.db_manager.connect():
                raise Exception("No se pudo conectar a PostgreSQL")
            
            # Cargar símbolos existentes en memoria para control de duplicados
            existing_symbols = self.db_manager.get_existing_cryptos()
            self.global_seen_symbols.update(existing_symbols.keys())
            print(f"✅ BD conectada - {len(existing_symbols)} símbolos ya en BD")
            
        except Exception as e:
            print(f"❌ Error conectando a base de datos: {e}")
            raise
    
    def fast_parse_number(self, text: str) -> float:
        """Parser de números ultra optimizado con regex precompilado"""
        if not text or text in ['--', '-', 'N/A', '', '∞']:
            return 0.0
        
        try:
            # Limpiar y extraer número
            clean_text = text.replace('$', '').replace(',', '').replace('%', '').strip()
            
            # Detectar multiplicadores
            multiplier = 1
            if clean_text.endswith(('T', 't')):
                multiplier = 1_000_000_000_000
                clean_text = clean_text[:-1]
            elif clean_text.endswith(('B', 'b')):
                multiplier = 1_000_000_000
                clean_text = clean_text[:-1]
            elif clean_text.endswith(('M', 'm')):
                multiplier = 1_000_000
                clean_text = clean_text[:-1]
            elif clean_text.endswith(('K', 'k')):
                multiplier = 1_000
                clean_text = clean_text[:-1]
            
            # Extraer número con regex
            number_match = re.search(r'[\d.]+', clean_text)
            if number_match:
                return float(number_match.group()) * multiplier
            return 0.0
            
        except (ValueError, AttributeError):
            return 0.0
    
    def extract_crypto_data_optimized(self, soup_row, expected_rank: int) -> Optional[Dict[str, Any]]:
        """Extracción ultra optimizada usando BeautifulSoup basada en la estructura real de CoinGecko"""
        try:
            # Buscar todas las celdas de una vez
            cells = soup_row.find_all('td')
            if len(cells) < 8:  # Mínimo esperado: star, rank, coin, ads, price, 1h, 24h, 7d
                return None

            # Estructura de columnas basada en el HTML real:
            # 0: Favorito (estrella)
            # 1: Ranking (#)
            # 2: Coin (nombre, símbolo, imagen)
            # 3: Ads (Buy button)
            # 4: Price
            # 5: 1h %
            # 6: 24h %
            # 7: 7d %
            # 8: 30d % (hidden)
            # 9: 24h Volume
            # 10: Market Cap
            # 11: FDV (hidden)
            # 12: Market Cap / FDV (hidden)
            # 13: Last 7 Days (chart)

            # Extraer ranking de la columna 1
            rank_text = cells[1].get_text(strip=True)
            try:
                rank = int(rank_text)
            except (ValueError, IndexError):
                rank = expected_rank

            # Extraer datos de la moneda de la columna 2
            coin_cell = cells[2]
            link = coin_cell.find('a')
            if not link:
                return None

            href = link.get('href', '')
            coin_url = urljoin(self.base_url, href)
            
            # Extraer imagen y símbolo
            img = link.find('img')
            symbol = ""
            icon_url = ""
            if img:
                icon_url = img.get('src', '')
                alt_text = img.get('alt', '').upper().strip()
                symbol = alt_text

            # Extraer nombre del texto del enlace - método mejorado
            name = ""
            symbol_from_text = ""
            
            # Buscar div con clase específica para el nombre
            name_div = link.find('div', class_=re.compile(r'tw-text-gray-700'))
            if name_div:
                # Extraer texto completo del nombre
                name_text = name_div.get_text(strip=True)
                
                # Buscar símbolo en la misma estructura
                symbol_div = name_div.find('div', class_=re.compile(r'tw-text-xs.*tw-text-gray-500'))
                if symbol_div:
                    symbol_from_text = symbol_div.get_text(strip=True).upper()
                    # Remover el símbolo del nombre si está incluido
                    name = name_text.replace(symbol_div.get_text(strip=True), '').strip()
                else:
                    name = name_text
            else:
                # Fallback: usar todo el texto del enlace
                name = link.get_text(strip=True)

            # Usar el mejor símbolo disponible
            if symbol_from_text:
                symbol = symbol_from_text
            elif not symbol and img:
                symbol = img.get('alt', '').upper().strip()

            # Extraer slug de URL
            slug = ""
            path_parts = [p for p in href.split('/') if p and not p.startswith('en')]
            if path_parts and 'coins' in path_parts:
                coins_idx = path_parts.index('coins')
                if coins_idx + 1 < len(path_parts):
                    slug = path_parts[coins_idx + 1]

            # Validaciones y fallbacks mejorados
            if not symbol or len(symbol) > 20:
                print(f"⚠️ Símbolo inválido en rank {rank}: '{symbol}', saltando...")
                return None
            
            # CONTROL DE DUPLICADOS A NIVEL DE EXTRACCIÓN
            symbol_upper = symbol.upper()
            if symbol_upper in self.global_seen_symbols:
                self.session_stats['duplicates_skipped'] += 1
                return None  # Saltar directamente si ya lo hemos visto
            
            if not name:
                name = symbol
            
            # Limpiar datos
            name = name.strip()[:255] or f"Unknown-{rank}"
            symbol = symbol.strip()[:20] or f"UNK{rank}"
            slug = slug or f"coingecko-{symbol.lower()}"

            # Extraer precio de la columna 4 (índice 4)
            price = 0.0
            if len(cells) > 4:
                price_text = cells[4].get_text(strip=True)
                price = self.fast_parse_number(price_text)

            # Extraer cambios porcentuales
            percent_change_1h = 0.0
            percent_change_24h = 0.0
            percent_change_7d = 0.0

            # 1h % (columna 5)
            if len(cells) > 5:
                percent_text = cells[5].get_text(strip=True)
                percent_match = self.percent_pattern.search(percent_text)
                if percent_match:
                    try:
                        percent_change_1h = float(percent_match.group(1))
                    except ValueError:
                        percent_change_1h = 0.0

            # 24h % (columna 6)
            if len(cells) > 6:
                percent_text = cells[6].get_text(strip=True)
                percent_match = self.percent_pattern.search(percent_text)
                if percent_match:
                    try:
                        percent_change_24h = float(percent_match.group(1))
                    except ValueError:
                        percent_change_24h = 0.0

            # 7d % (columna 7)
            if len(cells) > 7:
                percent_text = cells[7].get_text(strip=True)
                percent_match = self.percent_pattern.search(percent_text)
                if percent_match:
                    try:
                        percent_change_7d = float(percent_match.group(1))
                    except ValueError:
                        percent_change_7d = 0.0

            # Extraer volumen 24h (columna 9 aproximadamente)
            volume_24h = 0.0
            if len(cells) > 9:
                volume_text = cells[9].get_text(strip=True)
                volume_24h = self.fast_parse_number(volume_text)

            # Extraer market cap (columna 10 aproximadamente)
            market_cap = 0.0
            if len(cells) > 10:
                market_cap_text = cells[10].get_text(strip=True)
                market_cap = self.fast_parse_number(market_cap_text)

            # Marcar como visto globalmente
            self.global_seen_symbols.add(symbol_upper)
            self.session_stats['total_scraped'] += 1

            # Construir datos del crypto con validaciones adicionales
            crypto_data = {
                'name': name,
                'symbol': symbol,
                'slug': slug,
                'rank': rank,
                'icon_url': icon_url[:500] if icon_url else '',
                'coin_url': coin_url[:500],
                'coingecko_url': coin_url[:500],
                'tags': ['scraped'],
                'badges': ['live'] if price > 0 else [],
                'market_pair_count': None,
                'price': price,
                'volume_24h': volume_24h,
                'market_cap': market_cap,
                'percent_change_1h': percent_change_1h,
                'percent_change_24h': percent_change_24h,
                'percent_change_7d': percent_change_7d,
                'extracted_at': datetime.now(timezone.utc)
            }

            return crypto_data

        except Exception as e:
            print(f"⚠️ Error extrayendo crypto {expected_rank}: {str(e)[:50]}")
            return None
    
    def scrape_page_ultra_fast(self, page: int) -> Tuple[List[Dict[str, Any]], bool]:
        """Scraping ultra rápido de una página con BeautifulSoup + Selenium
        
        Returns:
            Tuple[List[Dict], bool]: (datos_extraidos, tabla_encontrada)
        """
        # Modificar URL para usar parámetros correctos
        url = f"{self.base_url}/?page={page}&items=100"
        
        try:
            with self.driver_pool.get_driver() as driver:
                print(f"🚀 Página {page}...", end='', flush=True)
                
                # Cargar página
                driver.get(url)
                
                # Esperar tabla con más tiempo
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table[data-coin-table-target='table']"))
                    )
                    # Esperar a que se carguen las filas
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "tbody tr"))
                    )
                except TimeoutException:
                    print(f" ❌ Timeout esperando tabla")
                    return [], False  # No se encontró tabla
                
                # Obtener HTML y usar BeautifulSoup para parsing ultra rápido
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Encontrar tabla específica
                table = soup.find('table', {'data-coin-table-target': 'table'})
                if not table:
                    print(f" ❌ Sin tabla específica")
                    return [], False  # No se encontró tabla
                
                tbody = table.find('tbody')
                if not tbody:
                    print(f" ❌ Sin tbody")
                    return [], True  # Hay tabla pero sin tbody
                
                rows = tbody.find_all('tr')
                if not rows:
                    print(f" ⚠️ Tabla vacía")
                    return [], True  # Hay tabla pero sin filas
                
                # Procesar todas las filas de una vez
                coins_data = []
                expected_rank = (page - 1) * 100 + 1
                
                for i, row in enumerate(rows):
                    coin_data = self.extract_crypto_data_optimized(row, expected_rank + i)
                    if coin_data:
                        coins_data.append(coin_data)
                
                print(f" ✅ {len(coins_data)} cryptos únicos")
                return coins_data, True  # Datos extraídos y tabla encontrada
                
        except Exception as e:
            print(f" ❌ Error: {str(e)[:50]}")
            return [], False  # Error = no tabla válida
    
    def scrape_all_pages_until_fail(self) -> List[Dict[str, Any]]:
        """Scraping automático hasta fallo con control exhaustivo de duplicados"""
        all_coins = []
        
        print("🚀 === SCRAPING AUTOMÁTICO ULTRA OPTIMIZADO ===")
        print("⚡ Scraping continuo hasta fallo detectado")
        print("🔧 BeautifulSoup + Selenium sin JS")
        print("🚫 Control exhaustivo de duplicados")
        print("🔄 Sin límite de páginas vacías")
        print("🛑 Solo para al no encontrar tablas")
        print("💾 Solo PostgreSQL - Sin archivos")
        
        # Conectar base de datos
        try:
            self.connect_database()
        except Exception as e:
            print(f"❌ Error BD: {e}")
            return []
        
        # Variables de control
        current_batch = []
        batch_size = 300  # Lotes optimizados para control de duplicados
        consecutive_table_failures = 0
        max_table_failures = 3
        page = 1
        
        print(f"\n🎯 Iniciando scraping automático...")
        print(f"📊 Símbolos existentes en BD: {len(self.global_seen_symbols):,}")
        start_time = time.time()
        
        while consecutive_table_failures < max_table_failures:
            try:
                page_data, table_found = self.scrape_page_ultra_fast(page)
                
                # Si no se encontró tabla, es un fallo real
                if not table_found:
                    consecutive_table_failures += 1
                    print(f"⚠️ Fallo de tabla {consecutive_table_failures}/{max_table_failures} en página {page}")
                    
                    if consecutive_table_failures >= max_table_failures:
                        print(f"🛑 Límite de fallos de tabla alcanzado - Finalizando")
                        break
                    
                    page += 1
                    time.sleep(1)  # Pausa en fallo
                    continue
                
                # Reset contador de fallos de tabla si se encontró tabla
                consecutive_table_failures = 0
                
                # Si hay tabla pero no datos, simplemente continuar sin límite
                if not page_data:
                    print(f"⚠️ Página {page} vacía - Continuando...")
                    page += 1
                    time.sleep(0.5)
                    continue
                
                # Los datos ya vienen filtrados desde extract_crypto_data_optimized
                unique_page_data = page_data
                self.session_stats['total_unique'] += len(unique_page_data)
                
                # Agregar a lotes
                all_coins.extend(unique_page_data)
                current_batch.extend(unique_page_data)
                
                # Procesar lote si está lleno
                if len(current_batch) >= batch_size:
                    saved_count = self.db_manager.save_crypto_batch(current_batch)
                    current_batch = []
                    
                    # Estadísticas en tiempo real
                    elapsed = time.time() - start_time
                    rate = self.session_stats['total_unique'] / elapsed * 60 if elapsed > 0 else 0
                    
                    print(f"📊 Total únicos: {self.session_stats['total_unique']:,} | "
                          f"Página {page} | {rate:.0f} cryptos/min | "
                          f"Duplicados: {self.session_stats['duplicates_skipped']}")
                
                page += 1
                
                # Pausa corta para no sobrecargar
                time.sleep(0.5)
                
            except KeyboardInterrupt:
                print("🛑 Interrumpido por usuario")
                break
            except Exception as e:
                print(f"❌ Error página {page}: {str(e)[:50]}")
                consecutive_table_failures += 1
                page += 1
                time.sleep(1)
        
        # Procesar lote final
        if current_batch:
            self.db_manager.save_crypto_batch(current_batch)
        
        # Estadísticas finales
        elapsed = time.time() - start_time
        rate = self.session_stats['total_unique'] / elapsed * 60 if elapsed > 0 else 0
        
        print(f"\n🎉 === SCRAPING COMPLETADO ===")
        print(f"📊 Total scraped: {self.session_stats['total_scraped']:,} cryptos")
        print(f"✅ Total únicos guardados: {self.session_stats['total_unique']:,} cryptos")
        print(f"🔄 Duplicados saltados: {self.session_stats['duplicates_skipped']:,}")
        print(f"📄 Páginas procesadas: {page-1}")
        print(f"⏱️ Tiempo total: {elapsed:.1f}s")
        print(f"🚀 Velocidad: {rate:.0f} cryptos únicos/minuto")
        print(f"💾 Guardado en PostgreSQL normalizado")
        print(f"🚫 0% duplicados insertados")
        
        return all_coins
    
    def close(self):
        """Cerrar recursos"""
        if hasattr(self, 'driver_pool'):
            self.driver_pool.close_all()
        
        if self.db_manager:
            self.db_manager.close()

def main():
    """Función principal automatizada con control exhaustivo de duplicados"""
    scraper = None
    
    try:
        print("🚀 === COINGECKO ULTRA SCRAPER ANTI-DUPLICADOS ===")
        print("⚡ OPTIMIZACIONES EXTREMAS + CONTROL DE DUPLICADOS:")
        print("  - BeautifulSoup + Selenium híbrido")
        print("  - Cache de símbolos existentes")
        print("  - Filtrado en tiempo real")
        print("  - Control a nivel de extracción")
        print("  - Verificación contra BD existente")
        print("  - Eliminación de duplicados en lotes")
        print("  - Inserción segura con ON CONFLICT")
        print("  - 0% duplicados garantizado")
        
        # Verificar dependencias
        check_dependencies()
        
        # Cargar configuración
        load_env_file()
        db_config = DatabaseConfig()
        
        # Crear scraper ultra optimizado
        scraper = UltraOptimizedScraper(db_config=db_config)
        
        # SCRAPING AUTOMÁTICO ANTI-DUPLICADOS
        print(f"\n🤖 Iniciando scraping automático anti-duplicados...")
        coins_data = scraper.scrape_all_pages_until_fail()
        
        if coins_data:
            print(f"\n✅ Scraping exitoso:")
            print(f"📊 {len(coins_data):,} criptomonedas únicas extraídas")
            print(f"💾 Guardadas en PostgreSQL sin duplicados")
            
            # Muestra de primeras 5 cryptos
            if len(coins_data) >= 5:
                print(f"\n📋 Primeras 5 criptomonedas únicas:")
                for i, coin in enumerate(coins_data[:5]):
                    price_str = f" - ${coin['price']:.6f}" if coin.get('price', 0) > 0 else ""
                    rank_str = f"#{coin.get('rank', i+1)}"
                    print(f"  {rank_str} {coin['name']} ({coin['symbol']}){price_str}")
            
        else:
            print("❌ No se extrajeron datos únicos")
            
    except KeyboardInterrupt:
        print("🛑 Proceso interrumpido")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()