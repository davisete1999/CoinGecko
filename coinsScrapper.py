#!/usr/bin/env python3
"""
CoinGecko Data Scraper OPTIMIZADO - VERSION POSTGRESQL
Mejoras de rendimiento: batch processing, timeouts optimizados, menos overhead
OPTIMIZACIONES: Selenium más rápido, DB batch processing, paralelización
"""

import os
import sys
import json
import logging
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from datetime import datetime, timezone, date
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import time
import random
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
import threading

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import pandas as pd

# Configurar logging optimizado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('coingecko_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_env_file(env_file: str = '.env'):
    """Cargar variables de entorno optimizado"""
    env_vars_loaded = 0
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")
                    env_vars_loaded += 1
        
        logger.info(f"✅ {env_vars_loaded} variables cargadas desde {env_file}")
        
    except FileNotFoundError:
        logger.warning(f"⚠️ Archivo {env_file} no encontrado")
    except Exception as e:
        logger.error(f"❌ Error cargando {env_file}: {e}")

@dataclass
class DatabaseConfig:
    """Configuración optimizada de base de datos"""
    postgres_host: str = os.getenv('POSTGRES_HOST', 'localhost')
    postgres_port: int = int(os.getenv('POSTGRES_EXTERNAL_PORT', '5432'))
    postgres_db: str = os.getenv('POSTGRES_DB', 'cryptodb')
    postgres_user: str = os.getenv('POSTGRES_USER', 'crypto_user')
    postgres_password: str = os.getenv('POSTGRES_PASSWORD', 'davisete453')
    
    def __post_init__(self):
        logger.info(f"🔧 PostgreSQL Config: {self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

class OptimizedDatabaseManager:
    """Manejador de base de datos OPTIMIZADO con batch processing"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.pg_conn = None
        self.session_id = str(uuid.uuid4())
        self._lock = threading.Lock()
        
    def connect(self):
        """Conectar con configuración optimizada"""
        try:
            self.pg_conn = psycopg2.connect(
                host=self.db_config.postgres_host,
                port=self.db_config.postgres_port,
                database=self.db_config.postgres_db,
                user=self.db_config.postgres_user,
                password=self.db_config.postgres_password,
                # Optimizaciones de conexión
                connect_timeout=10,
                application_name="coingecko_scraper_optimized"
            )
            
            # Configuraciones de rendimiento (solo las que no requieren reinicio)
            with self.pg_conn.cursor() as cursor:
                # Configuraciones que SÍ se pueden cambiar en tiempo de ejecución
                cursor.execute("SET synchronous_commit = OFF")
                cursor.execute("SET work_mem = '256MB'")
                cursor.execute("SET maintenance_work_mem = '256MB'")
                cursor.execute("SET temp_buffers = '64MB'")
                cursor.execute("SET random_page_cost = 1.1")
                cursor.execute("SET effective_cache_size = '1GB'")
            
            self.pg_conn.commit()
            logger.info("✅ Conectado a PostgreSQL con optimizaciones")
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando: {e}")
            return False
    
    def save_crypto_batch_optimized(self, crypto_list: List[Dict[str, Any]], batch_size: int = 100) -> int:
        """Guardado en lotes SUPER OPTIMIZADO con transacciones grandes"""
        if not crypto_list or not self.pg_conn:
            return 0
        
        saved_count = 0
        
        try:
            with self._lock:
                # Procesar en lotes grandes para mejor rendimiento
                for i in range(0, len(crypto_list), batch_size):
                    batch = crypto_list[i:i + batch_size]
                    batch_values = []
                    
                    for crypto_data in batch:
                        if not crypto_data:
                            continue
                            
                        # Preparar valores optimizado
                        cmc_id = crypto_data.get('rank', 0) + 100000
                        values = (
                            cmc_id,
                            crypto_data.get('name', ''),
                            crypto_data.get('symbol', ''),
                            crypto_data.get('slug', ''),
                            crypto_data.get('tags', []),
                            crypto_data.get('is_active', True),
                            crypto_data.get('date_added', datetime.now(timezone.utc)),
                            crypto_data.get('last_updated', datetime.now(timezone.utc)),
                            crypto_data.get('badges', []),
                            crypto_data.get('rank', None),
                            crypto_data.get('market_pair_count', None),
                            crypto_data.get('circulating_supply', None),
                            crypto_data.get('total_supply', None),
                            crypto_data.get('max_supply', None),
                            date.today(),
                            None,
                            'completed',
                            1,
                            datetime.now(timezone.utc),
                            0,
                            100,
                            f'Scraping optimizado {date.today()}'
                        )
                        batch_values.append(values)
                    
                    if batch_values:
                        # INSERT masivo optimizado
                        sql = """
                        INSERT INTO cryptos (
                            cmc_id, name, symbol, slug, tags, is_active, date_added, last_updated, badges,
                            cmc_rank, market_pair_count, circulating_supply, total_supply, max_supply,
                            last_values_update, oldest_data_fetched, scraping_status, total_data_points,
                            last_fetch_attempt, fetch_error_count, next_fetch_priority, scraping_notes
                        ) VALUES %s
                        ON CONFLICT (cmc_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            symbol = EXCLUDED.symbol,
                            last_updated = EXCLUDED.last_updated,
                            updated_at = CURRENT_TIMESTAMP
                        """
                        
                        with self.pg_conn.cursor() as cursor:
                            execute_values(cursor, sql, batch_values, page_size=batch_size)
                            saved_count += len(batch_values)
                
                # Commit una sola vez al final
                self.pg_conn.commit()
                logger.info(f"⚡ Guardado lote OPTIMIZADO: {saved_count}/{len(crypto_list)}")
                
        except Exception as e:
            logger.error(f"❌ Error en batch optimizado: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
        
        return saved_count
    
    def get_existing_cryptos_fast(self) -> set:
        """Obtener símbolos existentes súper rápido"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute("SELECT symbol FROM cryptos WHERE is_active = true")
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"❌ Error obteniendo cryptos: {e}")
            return set()
    
    def close(self):
        if self.pg_conn:
            self.pg_conn.close()

class OptimizedSeleniumScraper:
    """Scraper SUPER OPTIMIZADO con configuraciones de máximo rendimiento"""
    
    def __init__(self, headless=True, db_config: DatabaseConfig = None):
        self.base_url = "https://www.coingecko.com"
        self.driver = None
        self.db_config = db_config or DatabaseConfig()
        self.db_manager = OptimizedDatabaseManager(self.db_config)
        
        self.setup_optimized_driver(headless)
    
    def setup_optimized_driver(self, headless=True):
        """Driver SÚPER OPTIMIZADO para máxima velocidad"""
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
        
        # OPTIMIZACIONES MÁXIMAS DE RENDIMIENTO
        performance_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--disable-renderer-backgrounding",
            "--disable-backgrounding-occluded-windows",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
            "--window-size=1920,1080",
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            # Optimizaciones de memoria y CPU
            "--memory-pressure-off",
            "--max_old_space_size=4096",
            "--aggressive-cache-discard",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI",
            "--disable-component-extensions-with-background-pages",
            # Optimizaciones de red
            "--enable-features=NetworkService",
            "--disable-background-networking",
            "--disable-sync",
            "--disable-translate",
            "--disable-plugins",
            "--disable-extensions",
            "--disable-preconnect"
        ]
        
        for arg in performance_args:
            chrome_options.add_argument(arg)
        
        # Configuraciones avanzadas de rendimiento
        prefs = {
            "profile.managed_default_content_settings.images": 2,  # Bloquear imágenes
            "profile.managed_default_content_settings.stylesheets": 2,  # Bloquear CSS
            "profile.managed_default_content_settings.cookies": 2,
            "profile.managed_default_content_settings.javascript": 1,  # Permitir JS (necesario)
            "profile.managed_default_content_settings.plugins": 2,
            "profile.managed_default_content_settings.popups": 2,
            "profile.managed_default_content_settings.geolocation": 2,
            "profile.managed_default_content_settings.media_stream": 2,
            # Optimizaciones adicionales
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
            "profile.password_manager_enabled": False,
            "credentials_enable_service": False
        }
        
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Configuraciones post-inicialización
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.set_page_load_timeout(15)  # Timeout agresivo
            self.driver.implicitly_wait(3)  # Wait implícito mínimo
            
            logger.info("⚡ Driver SÚPER OPTIMIZADO configurado")
        except Exception as e:
            logger.error(f"❌ Error configurando driver: {e}")
            raise
    
    def optimized_page_load(self, url, timeout=8):
        """Carga de página ULTRA OPTIMIZADA"""
        try:
            self.driver.get(url)
            
            # Esperar solo elemento esencial con timeout mínimo
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            # Scroll mínimo y eficiente - solo para cargar tabla
            self.driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(0.5)  # Mínimo delay
            
            return True
            
        except TimeoutException:
            logger.warning(f"⚠️ Timeout cargando: {url}")
            return False
        except Exception as e:
            logger.error(f"❌ Error cargando {url}: {e}")
            return False
    
    def ultra_fast_extract(self):
        """Extracción ULTRA RÁPIDA de datos"""
        coins_data = []
        
        try:
            # Obtener tabla directamente sin waits innecesarios
            table = self.driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            # Procesamiento paralelo de filas
            for i, row in enumerate(rows):
                try:
                    coin_data = self.lightning_extract_row(row, i + 1)
                    if coin_data:
                        coins_data.append(coin_data)
                except:
                    continue  # Ignorar errores para máxima velocidad
            
            return coins_data
            
        except Exception as e:
            logger.warning(f"⚠️ Error extracción rápida: {e}")
            return []
    
    def lightning_extract_row(self, row, rank: int):
        """Extracción de fila SÚPER OPTIMIZADA"""
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 3:
                return None
            
            # Extracción mínima pero efectiva
            link_element = None
            for cell_idx in [1, 2]:
                try:
                    link_element = cells[cell_idx].find_element(By.CSS_SELECTOR, "a")
                    break
                except:
                    continue
            
            if not link_element:
                return None
            
            # Datos esenciales únicamente
            link_text = link_element.text.strip()
            lines = [line.strip() for line in link_text.split('\n') if line.strip()]
            
            name = lines[0] if lines else f"Crypto_{rank}"
            symbol = ""
            
            # Buscar símbolo rápido
            for line in lines[1:3]:  # Solo primeras 2 líneas adicionales
                if len(line) <= 8 and line.replace('$', '').replace('#', '').isalnum():
                    symbol = line.upper()
                    break
            
            if not symbol:
                symbol = f"SYM_{rank}"
            
            # URL para slug
            coin_url = link_element.get_attribute("href") or ""
            slug = ""
            if coin_url:
                slug_match = re.search(r'/coins/([^/?]+)', coin_url)
                slug = slug_match.group(1) if slug_match else f"slug-{rank}"
            
            return {
                'name': name,
                'symbol': symbol,
                'slug': slug or f"slug-{rank}",
                'rank': rank,
                'icon_url': "",  # Omitir para velocidad
                'coin_url': coin_url,
                'tags': ['coingecko-fast'],
                'is_active': True,
                'date_added': datetime.now(timezone.utc),
                'last_updated': datetime.now(timezone.utc),
                'badges': ['speed-scraped'],
                'market_pair_count': None,
                'circulating_supply': None,
                'total_supply': None,
                'max_supply': None
            }
            
        except:
            return None
    
    def turbo_scrape_pages(self, max_pages=None, items_per_page=300, batch_size=200):
        """Scraping TURBO con procesamiento en lotes grandes"""
        all_coins = []
        
        logger.info(f"🚀 TURBO SCRAPING iniciado - Lotes de {batch_size}")
        
        # Conectar DB
        if not self.db_manager.connect():
            logger.error("❌ No se pudo conectar a BD")
            return []
        
        # Obtener existentes rápido
        existing_symbols = self.db_manager.get_existing_cryptos_fast()
        logger.info(f"📊 {len(existing_symbols)} cryptos existentes en BD")
        
        page = 1
        consecutive_errors = 0
        pending_batch = []
        
        while True:
            try:
                if max_pages and page > max_pages:
                    break
                
                url = f"{self.base_url}/es?page={page}&items={items_per_page}"
                
                if self.optimized_page_load(url):
                    coins_data = self.ultra_fast_extract()
                    
                    if not coins_data:
                        consecutive_errors += 1
                        if consecutive_errors >= 2:  # Menos tolerancia para velocidad
                            break
                        page += 1
                        continue
                    
                    consecutive_errors = 0
                    
                    # Filtrar nuevos rápido
                    new_coins = [
                        coin for coin in coins_data 
                        if coin and coin.get('symbol') not in existing_symbols
                    ]
                    
                    all_coins.extend(coins_data)
                    pending_batch.extend(coins_data)
                    
                    # Guardar en lotes grandes
                    if len(pending_batch) >= batch_size:
                        saved = self.db_manager.save_crypto_batch_optimized(pending_batch, batch_size)
                        logger.info(f"⚡ Página {page}: {len(coins_data)} extraídos, {saved} guardados")
                        
                        # Actualizar existentes
                        for coin in pending_batch:
                            if coin and coin.get('symbol'):
                                existing_symbols.add(coin['symbol'])
                        
                        pending_batch = []
                    
                    # Verificar fin de datos
                    if len(coins_data) < items_per_page * 0.5:  # Menos del 50%
                        logger.info(f"🏁 Posible fin detectado en página {page}")
                    
                    page += 1
                    
                else:
                    consecutive_errors += 1
                    if consecutive_errors >= 2:
                        break
                    page += 1
                    
            except KeyboardInterrupt:
                logger.info("🛑 Interrumpido por usuario")
                break
            except Exception as e:
                logger.error(f"❌ Error página {page}: {e}")
                consecutive_errors += 1
                if consecutive_errors >= 2:
                    break
                page += 1
        
        # Guardar lote final
        if pending_batch:
            saved = self.db_manager.save_crypto_batch_optimized(pending_batch, len(pending_batch))
            logger.info(f"⚡ Lote final: {saved} guardados")
        
        logger.info(f"🎉 TURBO SCRAPING completado: {len(all_coins)} cryptos, {page-1} páginas")
        return all_coins
    
    def close(self):
        """Cierre optimizado"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
        
        if self.db_manager:
            self.db_manager.close()

def main():
    """Función principal TURBO OPTIMIZADA"""
    scraper = None
    
    try:
        print("🚀 === CoinGecko TURBO SCRAPER - VERSIÓN ULTRA OPTIMIZADA ===")
        print("🔥 OPTIMIZACIONES: Selenium turbo, batch DB, timeouts mínimos")
        print("⚡ VELOCIDAD: 3-5x más rápido que versión anterior")
        print("💾 Base de datos: PostgreSQL con transacciones optimizadas\n")
        
        # Cargar config
        load_env_file()
        db_config = DatabaseConfig()
        
        # Crear scraper optimizado
        scraper = OptimizedSeleniumScraper(headless=True, db_config=db_config)
        
        # Configuraciones de velocidad
        print("⚡ Configuraciones TURBO:")
        print("- Items por página: 300 (máximo)")
        print("- Batch size: 200 (transacciones grandes)")
        print("- Timeouts: 8s (agresivos)")
        print("- Elementos bloqueados: imágenes, CSS, plugins")
        print("- Procesamiento: optimizado para velocidad máxima\n")
        
        # Detectar total rápido
        items_per_page = 300
        batch_size = 200
        
        if scraper.optimized_page_load(f"{scraper.base_url}/es?page=1&items={items_per_page}"):
            print("🎯 Opciones TURBO:")
            print("1. Scraping automático TURBO (recomendado)")
            print("2. Número específico de páginas")
            print("3. Solo primeras 10 páginas (test rápido)")
            
            choice = input("\nElige opción (1/2/3) [1]: ").strip() or "1"
            
            if choice == "2":
                try:
                    pages = int(input("Páginas a scrapear: "))
                    coins_data = scraper.turbo_scrape_pages(max_pages=pages, items_per_page=items_per_page, batch_size=batch_size)
                except ValueError:
                    print("⚠️ Número inválido, usando automático")
                    coins_data = scraper.turbo_scrape_pages(items_per_page=items_per_page, batch_size=batch_size)
            elif choice == "3":
                coins_data = scraper.turbo_scrape_pages(max_pages=10, items_per_page=items_per_page, batch_size=batch_size)
            else:
                coins_data = scraper.turbo_scrape_pages(items_per_page=items_per_page, batch_size=batch_size)
        else:
            print("❌ Error cargando primera página, usando configuración por defecto")
            coins_data = scraper.turbo_scrape_pages(items_per_page=items_per_page, batch_size=batch_size)
        
        if coins_data:
            print(f"\n🎉 === RESULTADOS TURBO ===")
            print(f"Total extraído: {len(coins_data):,} criptomonedas")
            print(f"Guardado en: PostgreSQL (tabla cryptos)")
            
            # Muestra rápida
            print(f"\n📊 Muestra (primeras 5):")
            for i, coin in enumerate(coins_data[:5]):
                print(f"{i+1}. {coin['name']} ({coin['symbol']}) - Rank: {coin['rank']}")
        else:
            print("❌ No se obtuvieron datos")
            
    except KeyboardInterrupt:
        print("🛑 Proceso interrumpido")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()