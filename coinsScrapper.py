#!/usr/bin/env python3
"""
CoinGecko Data Scraper - Selenium Ultra Optimizado SIN JavaScript
VERSIÓN AUTOMATIZADA - Scraping continuo hasta fallo
Optimización extrema de extracción de filas con BeautifulSoup + Selenium

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
from typing import List, Dict, Any, Optional, Tuple
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
    """Manejador de base de datos optimizado para la nueva estructura"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.pg_conn = None
        self.session_id = str(uuid.uuid4())
        self.coingecko_source_id = None
        self._lock = threading.Lock()
        
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
            
            return True
        except Exception as e:
            print(f"❌ Error conectando a PostgreSQL: {e}")
            return False
    
    def save_crypto_batch(self, crypto_list: List[Dict[str, Any]]) -> int:
        """Guardar lote de cryptos usando la nueva estructura normalizada"""
        if not crypto_list or not self.pg_conn:
            return 0
        
        saved_count = 0
        
        try:
            with self._lock:
                with self.pg_conn.cursor() as cursor:
                    # Preparar datos para inserción batch
                    crypto_data = []
                    coingecko_data = []
                    
                    for crypto in crypto_list:
                        if not crypto or not crypto.get('symbol'):
                            continue
                        
                        # Datos para tabla principal cryptos
                        crypto_data.append((
                            crypto.get('name', ''),
                            crypto.get('symbol', ''),
                            crypto.get('slug', '')
                        ))
                    
                    # Insertar/actualizar cryptos principales usando función optimizada
                    crypto_ids = []
                    for name, symbol, slug in crypto_data:
                        try:
                            cursor.execute(
                                "SELECT get_or_create_crypto(%s, %s, %s)",
                                (name, symbol, slug)
                            )
                            crypto_id = cursor.fetchone()[0]
                            crypto_ids.append(crypto_id)
                        except Exception as e:
                            print(f"⚠️ Error con crypto {symbol}: {e}")
                            crypto_ids.append(None)
                    
                    # Preparar datos para coingecko_cryptos
                    for i, crypto in enumerate(crypto_list):
                        if i >= len(crypto_ids) or not crypto.get('symbol') or not crypto_ids[i]:
                            continue
                            
                        crypto_id = crypto_ids[i]
                        
                        # Extraer tags y badges como JSON compacto
                        tags_json = json.dumps(crypto.get('tags', [])[:5])  # Limitar a 5 tags
                        badges_json = json.dumps(crypto.get('badges', [])[:3])  # Limitar a 3 badges
                        
                        coingecko_data.append((
                            crypto_id,
                            crypto.get('rank'),
                            crypto.get('coingecko_url', ''),
                            crypto.get('icon_url', ''),
                            crypto.get('coin_url', ''),
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
                    
                    # Inserción batch optimizada para coingecko_cryptos
                    if coingecko_data:
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
                                coingecko_rank = EXCLUDED.coingecko_rank,
                                coingecko_url = EXCLUDED.coingecko_url,
                                icon_url = EXCLUDED.icon_url,
                                coin_url = EXCLUDED.coin_url,
                                tags = EXCLUDED.tags,
                                badges = EXCLUDED.badges,
                                market_pair_count = EXCLUDED.market_pair_count,
                                last_values_update = EXCLUDED.last_values_update,
                                scraping_status = EXCLUDED.scraping_status,
                                total_data_points = EXCLUDED.total_data_points,
                                last_fetch_attempt = EXCLUDED.last_fetch_attempt,
                                scraping_notes = EXCLUDED.scraping_notes,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            coingecko_data,
                            template=None,
                            page_size=100
                        )
                        
                        saved_count = len(coingecko_data)
                    
                    self.pg_conn.commit()
                    print(f"✅ Guardado lote de {saved_count}/{len(crypto_list)} cryptos")
                    
        except Exception as e:
            print(f"❌ Error guardando lote: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            saved_count = 0
        
        return saved_count
    
    def get_existing_cryptos(self) -> Dict[str, int]:
        """Obtener mapa de símbolos existentes usando vista materializada"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT symbol, id 
                    FROM mv_active_cryptos 
                    WHERE in_coingecko = true
                """)
                return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception:
            # Fallback a consulta normal
            try:
                with self.pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT c.symbol, c.id 
                        FROM cryptos c 
                        JOIN coingecko_cryptos cg ON c.id = cg.crypto_id 
                        WHERE c.is_active = true
                    """)
                    return {row[0]: row[1] for row in cursor.fetchall()}
            except Exception:
                return {}
    
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
    """Scraper ultra optimizado con BeautifulSoup + Selenium"""
    
    def __init__(self, db_config: DatabaseConfig = None):
        self.base_url = "https://www.coingecko.com"
        self.db_config = db_config or DatabaseConfig()
        self.db_manager = DatabaseManager(self.db_config)
        self.driver_pool = WebDriverPool(pool_size=2)
        
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
            print("✅ Base de datos conectada")
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
                alt_text = img.get('alt', '').upper()
                symbol = alt_text.strip()

            # Extraer nombre del texto del enlace
            name_div = link.find('div', class_=re.compile(r'tw-text-gray-700'))
            if name_div:
                name = name_div.get_text(strip=True)
                # Buscar símbolo en la misma estructura
                symbol_div = name_div.find('div', class_=re.compile(r'tw-text-xs'))
                if symbol_div:
                    symbol = symbol_div.get_text(strip=True)
            else:
                name = link.get_text(strip=True)

            # Extraer slug de URL
            slug = ""
            path_parts = [p for p in href.split('/') if p and not p.startswith('en')]
            if path_parts and 'coins' in path_parts:
                coins_idx = path_parts.index('coins')
                if coins_idx + 1 < len(path_parts):
                    slug = path_parts[coins_idx + 1]

            # Fallbacks para datos faltantes
            if not symbol and img:
                symbol = img.get('alt', f'UNK{rank}').upper()
            
            name = name or symbol or f"Unknown-{rank}"
            symbol = symbol or f"UNK{rank}"
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
                    percent_change_1h = float(percent_match.group(1))

            # 24h % (columna 6)
            if len(cells) > 6:
                percent_text = cells[6].get_text(strip=True)
                percent_match = self.percent_pattern.search(percent_text)
                if percent_match:
                    percent_change_24h = float(percent_match.group(1))

            # 7d % (columna 7)
            if len(cells) > 7:
                percent_text = cells[7].get_text(strip=True)
                percent_match = self.percent_pattern.search(percent_text)
                if percent_match:
                    percent_change_7d = float(percent_match.group(1))

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

            # Construir datos del crypto
            crypto_data = {
                'name': name[:255],
                'symbol': symbol[:20],
                'slug': slug[:255],
                'rank': rank,
                'icon_url': icon_url[:500],
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
    
    def scrape_page_ultra_fast(self, page: int) -> List[Dict[str, Any]]:
        """Scraping ultra rápido de una página con BeautifulSoup + Selenium"""
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
                    return []
                
                # Obtener HTML y usar BeautifulSoup para parsing ultra rápido
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Encontrar tabla específica
                table = soup.find('table', {'data-coin-table-target': 'table'})
                if not table:
                    print(f" ❌ Sin tabla específica")
                    return []
                
                tbody = table.find('tbody')
                if not tbody:
                    print(f" ❌ Sin tbody")
                    return []
                
                rows = tbody.find_all('tr')
                if not rows:
                    print(f" ❌ Sin filas")
                    return []
                
                # Procesar todas las filas de una vez
                coins_data = []
                expected_rank = (page - 1) * 100 + 1
                
                for i, row in enumerate(rows):
                    coin_data = self.extract_crypto_data_optimized(row, expected_rank + i)
                    if coin_data:
                        coins_data.append(coin_data)
                
                print(f" ✅ {len(coins_data)} cryptos")
                return coins_data
                
        except Exception as e:
            print(f" ❌ Error: {str(e)[:50]}")
            return []
    
    def scrape_all_pages_until_fail(self) -> List[Dict[str, Any]]:
        """Scraping automático hasta fallo - SIN OPCIONES"""
        all_coins = []
        
        print("🚀 === SCRAPING AUTOMÁTICO ULTRA OPTIMIZADO ===")
        print("⚡ Scraping continuo hasta fallo detectado")
        print("🔧 BeautifulSoup + Selenium sin JS")
        print("💾 Solo PostgreSQL - Sin archivos")
        
        # Conectar base de datos
        try:
            self.connect_database()
        except Exception as e:
            print(f"❌ Error BD: {e}")
            return []
        
        # Variables de control
        current_batch = []
        batch_size = 1000  # Lotes optimizados
        consecutive_failures = 0
        max_failures = 3
        page = 1
        
        print(f"\n🎯 Iniciando scraping automático...")
        start_time = time.time()
        
        while consecutive_failures < max_failures:
            try:
                page_data = self.scrape_page_ultra_fast(page)
                
                if not page_data:
                    consecutive_failures += 1
                    print(f"⚠️ Fallo {consecutive_failures}/{max_failures} en página {page}")
                    
                    if consecutive_failures >= max_failures:
                        print(f"🛑 Límite de fallos alcanzado - Finalizando")
                        break
                    
                    page += 1
                    time.sleep(1)  # Pausa en fallo
                    continue
                
                # Reset contador de fallos
                consecutive_failures = 0
                
                # Agregar datos
                all_coins.extend(page_data)
                current_batch.extend(page_data)
                
                # Procesar lote si está lleno
                if len(current_batch) >= batch_size:
                    saved_count = self.db_manager.save_crypto_batch(current_batch)
                    current_batch = []
                    
                    # Estadísticas en tiempo real
                    elapsed = time.time() - start_time
                    rate = len(all_coins) / elapsed * 60 if elapsed > 0 else 0
                    print(f"📊 Total: {len(all_coins):,} | Página {page} | {rate:.0f} cryptos/min")
                
                page += 1
                
                # Pausa corta para no sobrecargar
                time.sleep(0.5)
                
            except KeyboardInterrupt:
                print("🛑 Interrumpido por usuario")
                break
            except Exception as e:
                print(f"❌ Error página {page}: {str(e)[:50]}")
                consecutive_failures += 1
                page += 1
                time.sleep(1)
        
        # Procesar lote final
        if current_batch:
            self.db_manager.save_crypto_batch(current_batch)
        
        # Estadísticas finales
        elapsed = time.time() - start_time
        rate = len(all_coins) / elapsed * 60 if elapsed > 0 else 0
        
        print(f"\n🎉 === SCRAPING COMPLETADO ===")
        print(f"📊 Total extraídas: {len(all_coins):,} cryptos")
        print(f"📄 Páginas procesadas: {page-1}")
        print(f"⏱️ Tiempo total: {elapsed:.1f}s")
        print(f"🚀 Velocidad: {rate:.0f} cryptos/minuto")
        print(f"💾 Guardado en PostgreSQL normalizado")
        
        return all_coins
    
    def close(self):
        """Cerrar recursos"""
        if hasattr(self, 'driver_pool'):
            self.driver_pool.close_all()
        
        if self.db_manager:
            self.db_manager.close()

def main():
    """Función principal automatizada - SIN OPCIONES"""
    scraper = None
    
    try:
        print("🚀 === COINGECKO ULTRA SCRAPER AUTOMÁTICO ===")
        print("⚡ OPTIMIZACIONES EXTREMAS:")
        print("  - BeautifulSoup + Selenium híbrido")
        print("  - Regex precompilados")
        print("  - Pool de WebDrivers optimizado")
        print("  - JavaScript completamente desactivado")
        print("  - Timeouts agresivos")
        print("  - Parsing ultra rápido")
        print("  - Scraping automático hasta fallo")
        print("  - Estructura HTML real de CoinGecko")
        
        # Verificar dependencias
        check_dependencies()
        
        # Cargar configuración
        load_env_file()
        db_config = DatabaseConfig()
        
        # Crear scraper ultra optimizado
        scraper = UltraOptimizedScraper(db_config=db_config)
        
        # SCRAPING AUTOMÁTICO - SIN OPCIONES
        print(f"\n🤖 Iniciando scraping automático...")
        coins_data = scraper.scrape_all_pages_until_fail()
        
        if coins_data:
            print(f"\n✅ Scraping exitoso:")
            print(f"📊 {len(coins_data):,} criptomonedas extraídas")
            print(f"💾 Guardadas en PostgreSQL")
            
            # Muestra de primeras 5 cryptos
            if len(coins_data) >= 5:
                print(f"\n📋 Primeras 5 criptomonedas:")
                for i, coin in enumerate(coins_data[:5]):
                    price_str = f" - ${coin['price']:.6f}" if coin.get('price', 0) > 0 else ""
                    rank_str = f"#{coin.get('rank', i+1)}"
                    print(f"  {rank_str} {coin['name']} ({coin['symbol']}){price_str}")
            
        else:
            print("❌ No se extrajeron datos")
            
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