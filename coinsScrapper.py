#!/usr/bin/env python3
"""
CoinGecko Data Scraper - Optimizado para memoria limitada
Integrado con PostgreSQL usando esquema normalizado optimizado
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
from urllib.parse import urlparse, urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd

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
    postgres_user: str = os.getenv('POSTGRES_USER', 'crypto_user')
    postgres_password: str = os.getenv('POSTGRES_PASSWORD', 'davisete453')
    
    def __post_init__(self):
        """Validar configuración después de inicialización"""
        print(f"🔧 PostgreSQL Config: {self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

class DatabaseManager:
    """Manejador de base de datos optimizado para memoria"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.pg_conn = None
        self.session_id = str(uuid.uuid4())
        self.coingecko_source_id = None
        
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
    
    def get_or_create_crypto(self, name: str, symbol: str, slug: str = None) -> Optional[int]:
        """Obtener o crear crypto en tabla principal usando función SQL"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute(
                    "SELECT get_or_create_crypto(%s, %s, %s)",
                    (name, symbol, slug)
                )
                crypto_id = cursor.fetchone()[0]
                self.pg_conn.commit()
                return crypto_id
        except Exception as e:
            print(f"❌ Error obteniendo/creando crypto {symbol}: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            return None
    
    def save_coingecko_crypto(self, crypto_data: Dict[str, Any]) -> Optional[int]:
        """Guardar crypto en esquema optimizado"""
        try:
            # Obtener o crear en tabla principal
            crypto_id = self.get_or_create_crypto(
                crypto_data.get('name', ''),
                crypto_data.get('symbol', ''),
                crypto_data.get('slug', '')
            )
            
            if not crypto_id:
                return None
            
            # Insertar/actualizar en tabla específica
            with self.pg_conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO coingecko_cryptos (
                        crypto_id, coingecko_rank, coingecko_url, icon_url, coin_url,
                        tags, badges, market_pair_count, last_values_update,
                        oldest_data_fetched, scraping_status, total_data_points,
                        last_fetch_attempt, fetch_error_count, next_fetch_priority, scraping_notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                """, (
                    crypto_id,
                    crypto_data.get('rank', None),
                    crypto_data.get('coin_url', ''),
                    crypto_data.get('icon_url', ''),
                    crypto_data.get('coin_url', ''),
                    json.dumps(crypto_data.get('tags', [])),  # JSON string para reducir memoria
                    json.dumps(crypto_data.get('badges', [])),  # JSON string para reducir memoria
                    crypto_data.get('market_pair_count', None),
                    date.today(),
                    None,
                    'completed',
                    1,
                    datetime.now(timezone.utc),
                    0,
                    100,
                    f'Scraped {date.today()}'
                ))
                
                # Crear log de scraping con menor información para optimizar memoria
                self.create_scraping_log(crypto_id, 1, True)
                
                self.pg_conn.commit()
                return crypto_id
                
        except Exception as e:
            print(f"❌ Error guardando crypto {crypto_data.get('symbol', 'UNKNOWN')}: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            return None
    
    def create_scraping_log(self, crypto_id: int, data_points: int, success: bool, error_message: str = None):
        """Crear entrada en el log de scraping"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO crypto_scraping_log 
                       (crypto_id, source_id, session_id, date_range_start, date_range_end, 
                        data_points_fetched, success, error_message)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (crypto_id, self.coingecko_source_id, self.session_id, date.today(), date.today(), 
                     data_points, success, error_message)
                )
        except Exception:
            pass  # Silencioso para reducir ruido
    
    def save_crypto_batch(self, crypto_list: List[Dict[str, Any]]) -> int:
        """Guardar lote de cryptos de forma optimizada"""
        saved_count = 0
        
        if not crypto_list or not self.pg_conn:
            return saved_count
        
        try:
            for crypto_data in crypto_list:
                if crypto_data:
                    crypto_id = self.save_coingecko_crypto(crypto_data)
                    if crypto_id:
                        saved_count += 1
            
            print(f"✅ Guardado lote de {saved_count}/{len(crypto_list)} cryptos")
            
        except Exception as e:
            print(f"❌ Error guardando lote: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
        
        return saved_count
    
    def get_existing_cryptos(self) -> Dict[str, int]:
        """Obtener mapa de símbolos existentes"""
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

class CoinGeckoScraper:
    """Scraper de CoinGecko optimizado para memoria"""
    
    def __init__(self, headless=True, delay=1, db_config: DatabaseConfig = None):
        self.base_url = "https://www.coingecko.com"
        self.delay = delay
        self.driver = None
        self.db_config = db_config or DatabaseConfig()
        self.db_manager = DatabaseManager(self.db_config)
        
        self.setup_driver(headless)
    
    def setup_driver(self, headless=True):
        """Configura el driver de Chrome optimizado para memoria"""
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
        
        # Optimizaciones de memoria
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-javascript")  # Solo para páginas estáticas
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=512")
        chrome_options.add_argument("--window-size=1024,768")  # Tamaño reducido
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        
        # Deshabilitar funciones para ahorrar memoria
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.media_stream": 2,
            "profile.managed_default_content_settings.notifications": 2,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Anti-detección mínima
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            print("✅ Driver de Chrome optimizado configurado")
        except Exception as e:
            print(f"❌ Error configurando Chrome driver: {e}")
            raise
    
    def connect_databases(self):
        """Establecer conexión a PostgreSQL"""
        try:
            if not self.db_manager.connect():
                raise Exception("No se pudo conectar a PostgreSQL")
            print("✅ Base de datos conectada")
        except Exception as e:
            print(f"❌ Error conectando a base de datos: {e}")
            raise
    
    def random_delay(self, min_delay=0.5, max_delay=1.5):
        """Delay aleatorio optimizado"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def get_page(self, url, wait_time=5):
        """Navega a una URL de forma optimizada"""
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            return True
        except TimeoutException:
            print(f"❌ Timeout cargando: {url}")
            return False
        except Exception as e:
            print(f"❌ Error cargando {url}: {e}")
            return False
    
    def get_total_cryptocurrencies(self) -> int:
        """Obtiene el total de criptomonedas de forma optimizada"""
        selectors = [
            "span.tw-text-sm",
            "span.text-sm", 
            ".tw-text-sm",
            "span[class*='text-sm']"
        ]
        
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    text = element.text.strip()
                    
                    if any(keyword in text.lower() for keyword in ['coin', 'crypto', 'total']):
                        numbers = re.findall(r'\d{1,3}(?:,\d{3})*', text)
                        if numbers:
                            for num_str in sorted(numbers, key=len, reverse=True):
                                try:
                                    num = int(num_str.replace(',', ''))
                                    if 100 <= num <= 50000:
                                        print(f"✅ Total encontrado: {num} cryptos")
                                        return num
                                except ValueError:
                                    continue
            except Exception:
                continue
        
        # Fallback
        try:
            table = self.driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            if len(rows) > 0:
                estimated_total = len(rows) * 50
                print(f"⚠️ Estimando {estimated_total} basado en {len(rows)} filas")
                return estimated_total
        except Exception:
            pass
        
        print("⚠️ Usando fallback de 2000 cryptos")
        return 2000
    
    def is_page_empty(self) -> bool:
        """Verifica si la página está vacía de forma optimizada"""
        try:
            table = self.driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            if len(rows) == 0:
                return True
            
            # Verificar solo las primeras 3 filas para ahorrar tiempo
            valid_rows = 0
            for row in rows[:3]:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:
                    for cell in cells:
                        if any(indicator in cell.text.lower() for indicator in ['$', 'btc', 'eth', '%']):
                            valid_rows += 1
                            break
            
            return valid_rows == 0
            
        except Exception:
            return True
    
    def parse_number(self, text: str) -> float:
        """Parsear números de forma optimizada"""
        if not text or text.strip() in ['--', '-', 'N/A', '']:
            return 0.0
        
        try:
            clean_text = text.strip().replace('$', '').replace(',', '').replace('%', '')
            
            multiplier = 1
            if clean_text.endswith('B'):
                multiplier = 1_000_000_000
                clean_text = clean_text[:-1]
            elif clean_text.endswith('M'):
                multiplier = 1_000_000
                clean_text = clean_text[:-1]
            elif clean_text.endswith('K'):
                multiplier = 1_000
                clean_text = clean_text[:-1]
            
            return float(clean_text) * multiplier
            
        except (ValueError, AttributeError):
            return 0.0
    
    def extract_coins_from_page(self):
        """Extrae datos de la página de forma optimizada"""
        coins_data = []
        
        try:
            table = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            for i, row in enumerate(rows):
                try:
                    coin_data = self.extract_coin_from_row(row, i + 1)
                    if coin_data:
                        coins_data.append(coin_data)
                except Exception:
                    continue
            
            print(f"✅ Extraídos {len(coins_data)} cryptos de la página")
            return coins_data
            
        except Exception as e:
            print(f"❌ Error extrayendo datos: {e}")
            return []
    
    def extract_coin_from_row(self, row, rank: int):
        """Extrae datos de una fila de forma optimizada"""
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 6:
                return None

            # Obtener enlace principal
            link_element = None
            for cell_idx in [1, 2]:
                try:
                    link_element = cells[cell_idx].find_element(By.CSS_SELECTOR, "a")
                    break
                except NoSuchElementException:
                    continue
            
            if not link_element:
                return None

            raw_href = link_element.get_attribute("href") or ""
            coin_url = urljoin(self.base_url, raw_href)

            # Extraer symbol del alt de imagen
            icon_url = ""
            symbol = ""
            try:
                img_element = link_element.find_element(By.TAG_NAME, "img")
                icon_url = img_element.get_attribute("src") or ""
                alt = (img_element.get_attribute("alt") or "").strip()
                alt_clean = re.sub(r"[^A-Za-z0-9.$-]", "", alt)
                if 1 <= len(alt_clean) <= 15:
                    symbol = alt_clean.upper()
            except NoSuchElementException:
                pass

            # Extraer nombre (texto directo sin badge)
            name = ""
            try:
                name_block = link_element.find_element(By.XPATH, ".//*[contains(@class,'font-semibold')][1]")
                name = self.driver.execute_script(
                    "return (function(el){"
                    "  var t='';"
                    "  for (var i=0;i<el.childNodes.length;i++){"
                    "    var n=el.childNodes[i];"
                    "    if (n.nodeType===Node.TEXT_NODE){ t += n.textContent; }"
                    "  }"
                    "  return t.trim();"
                    "})(arguments[0]);",
                    name_block
                ) or ""
            except Exception:
                link_text = (link_element.text or "").strip()
                if symbol:
                    link_text = re.sub(rf"\b{re.escape(symbol)}\b", "", link_text)
                name = next((ln.strip() for ln in link_text.splitlines() if ln.strip()), "")

            # Fallback de symbol
            if not symbol:
                try:
                    smalls = link_element.find_elements(By.XPATH, ".//*[contains(@class,'text-xs')]")
                    for sm in smalls:
                        cand = re.sub(r"[^A-Za-z0-9.$-]", "", (sm.text or "").strip())
                        if 1 <= len(cand) <= 15 and cand.upper() != (name or "").upper():
                            symbol = cand.upper()
                            break
                except Exception:
                    pass

            name = name or ""
            symbol = symbol or ""

            # Extraer slug del URL
            slug = ""
            try:
                p = urlparse(coin_url)
                parts = [seg for seg in p.path.split("/") if seg]
                if parts:
                    if len(parts[0]) <= 5 and re.match(r"^[a-z]{2}(-[a-z]{2})?$", parts[0], re.I):
                        parts = parts[1:]
                    if parts and parts[0].lower() in {"coins", "monedas", "moedas", "monete", "monnaies"}:
                        parts = parts[1:]
                    slug = parts[0] if parts else ""
            except Exception:
                pass

            # Extraer datos numéricos de forma optimizada
            price = 0.0
            volume_24h = 0.0
            market_cap = 0.0
            percent_change_24h = 0.0

            try:
                # Buscar precio
                for cell_idx in [2, 3, 4]:
                    if cell_idx < len(cells):
                        cell_text = (cells[cell_idx].text or "").strip()
                        if ("$" in cell_text or "US$" in cell_text) and price == 0.0:
                            price = self.parse_number(cell_text)
                            break

                # Buscar porcentaje
                for cell_idx in range(len(cells)):
                    cell_text = (cells[cell_idx].text or "").strip()
                    if "%" in cell_text and percent_change_24h == 0.0:
                        percent_change_24h = self.parse_number(cell_text)
                        break

                # Buscar market cap y volumen
                for cell_idx in range(len(cells)):
                    cell_text = (cells[cell_idx].text or "").strip()
                    if "$" in cell_text or "US$" in cell_text:
                        parsed_value = self.parse_number(cell_text)
                        if parsed_value > 1_000_000 and market_cap == 0.0:
                            market_cap = parsed_value
                        elif parsed_value > 100_000 and volume_24h == 0.0:
                            volume_24h = parsed_value
            except Exception:
                pass

            coin_data = {
                'name': name,
                'symbol': symbol,
                'slug': slug or (f"coingecko-{symbol.lower()}" if symbol else f"unknown-{rank}"),
                'rank': rank,
                'icon_url': icon_url,
                'coin_url': coin_url,
                'tags': ['coingecko-scraped'],  # Reducido para memoria
                'badges': ['live-data'] if price > 0 else ['scraped'],
                'market_pair_count': None,
                'source': 'coingecko',
                'is_active': True,
                'extracted_at': datetime.now(timezone.utc),
                'quote': {
                    'price': price,
                    'volume_24h': volume_24h,
                    'market_cap': market_cap,
                    'percent_change_1h': 0.0,
                    'percent_change_24h': percent_change_24h,
                    'percent_change_7d': 0.0,
                    'last_updated': datetime.now(timezone.utc)
                } if price > 0 else None
            }

            return coin_data

        except Exception:
            return None
    
    def scrape_pages(self, max_pages=None, items_per_page=100):
        """Scrapea múltiples páginas de forma optimizada"""
        all_coins = []
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        print(f"🚀 Iniciando scraping {'de todas las páginas' if max_pages is None else f'de {max_pages} páginas'}")
        
        # Conectar a base de datos
        try:
            self.connect_databases()
        except Exception as e:
            print(f"❌ No se pudo conectar a la base de datos: {e}")
            return []
        
        # Obtener cryptos existentes para evitar duplicados
        existing_cryptos = self.db_manager.get_existing_cryptos()
        print(f"📊 Encontradas {len(existing_cryptos)} cryptos existentes")
        
        page = 1
        while True:
            try:
                if max_pages is not None and page > max_pages:
                    print(f"🏁 Alcanzado límite de {max_pages} páginas")
                    break
                
                url = f"{self.base_url}/es?page={page}&items={items_per_page}"
                
                if self.get_page(url):
                    if self.is_page_empty():
                        print(f"📄 Página {page} vacía - Fin del scraping")
                        break
                    
                    coins_data = self.extract_coins_from_page()
                    
                    if not coins_data:
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            print(f"❌ {consecutive_errors} errores consecutivos - Terminando")
                            break
                        page += 1
                        continue
                    
                    consecutive_errors = 0
                    
                    # Filtrar cryptos nuevas
                    new_coins = [
                        coin for coin in coins_data 
                        if coin and coin.get('symbol') not in existing_cryptos
                    ]
                    
                    all_coins.extend(coins_data)
                    
                    print(f"📄 Página {page}: {len(coins_data)} total, {len(new_coins)} nuevas")
                    
                    # Guardar en PostgreSQL
                    if coins_data and self.db_manager.pg_conn:
                        try:
                            saved_count = self.db_manager.save_crypto_batch(coins_data)
                            
                            # Actualizar mapa de existentes
                            for coin in coins_data:
                                if coin and coin.get('symbol'):
                                    existing_cryptos[coin['symbol']] = coin.get('rank', 0)
                            
                        except Exception as e:
                            print(f"❌ Error guardando página {page}: {e}")
                    
                    # Verificar si página incompleta indica final
                    if len(coins_data) < items_per_page:
                        print(f"🏁 Página {page} tiene solo {len(coins_data)} elementos - Posible fin")
                    
                    # Delay muy reducido para velocidad
                    if self.delay > 0:
                        self.random_delay(0.2, 0.5)
                    page += 1
                    
                else:
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"❌ {consecutive_errors} errores consecutivos - Terminando")
                        break
                    page += 1
                    
            except KeyboardInterrupt:
                print("🛑 Scraping interrumpido por el usuario")
                break
            except Exception as e:
                print(f"❌ Error en página {page}: {e}")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    break
                page += 1
                continue
        
        print(f"🎉 Scraping completado. Total: {len(all_coins)} cryptos, Páginas: {page-1}")
        
        return all_coins
    
    def save_to_csv(self, data, filename='cryptos_coingecko.csv'):
        """Guarda los datos en CSV de forma optimizada"""
        try:
            flattened_data = []
            for crypto in data:
                row = {
                    'name': crypto.get('name', ''),
                    'symbol': crypto.get('symbol', ''),
                    'slug': crypto.get('slug', ''),
                    'rank': crypto.get('rank', 0),
                    'icon_url': crypto.get('icon_url', ''),
                    'coin_url': crypto.get('coin_url', ''),
                    'tags': ','.join(crypto.get('tags', [])),
                    'source': crypto.get('source', 'coingecko'),
                }
                
                if crypto.get('quote'):
                    quote = crypto['quote']
                    row.update({
                        'price': quote.get('price', 0),
                        'volume_24h': quote.get('volume_24h', 0),
                        'market_cap': quote.get('market_cap', 0),
                        'percent_change_24h': quote.get('percent_change_24h', 0),
                    })
                else:
                    row.update({
                        'price': 0, 'volume_24h': 0, 'market_cap': 0, 'percent_change_24h': 0
                    })
                
                flattened_data.append(row)
            
            df = pd.DataFrame(flattened_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"✅ Datos guardados en {filename}")
        except Exception as e:
            print(f"❌ Error guardando CSV: {e}")
    
    def save_to_json(self, data, filename='cryptos_coingecko.json'):
        """Guarda los datos en JSON de forma optimizada"""
        try:
            json_data = []
            for crypto in data:
                crypto_copy = crypto.copy()
                
                # Convertir fechas a strings
                for field in ['extracted_at']:
                    if field in crypto_copy and isinstance(crypto_copy[field], datetime):
                        crypto_copy[field] = crypto_copy[field].isoformat()
                
                if crypto_copy.get('quote') and 'last_updated' in crypto_copy['quote']:
                    if isinstance(crypto_copy['quote']['last_updated'], datetime):
                        crypto_copy['quote']['last_updated'] = crypto_copy['quote']['last_updated'].isoformat()
                
                # Metadatos optimizados
                crypto_copy['schema_version'] = 'v1'
                crypto_copy['source_table'] = 'coingecko_cryptos'
                
                json_data.append(crypto_copy)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            print(f"✅ Datos guardados en {filename}")
        except Exception as e:
            print(f"❌ Error guardando JSON: {e}")
    
    def close(self):
        """Cierra el driver y conexiones"""
        if self.driver:
            self.driver.quit()
            print("🔐 Driver cerrado")
        
        if self.db_manager:
            self.db_manager.close()

def main():
    """Función principal optimizada"""
    scraper = None
    
    try:
        print("🚀 === CoinGecko Scraper Optimizado ===")
        print("🔥 OPTIMIZACIONES:")
        print("  - Sin logging verboso")
        print("  - Driver Chrome optimizado para memoria")
        print("  - Base de datos con esquema normalizado")
        print("  - Procesamiento eficiente de páginas")
        print("  - Delays mínimos para velocidad")
        print("💾 Base de datos: PostgreSQL optimizado\n")
        
        # Cargar variables de entorno
        load_env_file()
        
        # Crear configuración
        db_config = DatabaseConfig()
        
        # Crear scraper optimizado
        scraper = CoinGeckoScraper(headless=True, delay=0, db_config=db_config)
        
        # Detectar total de criptomonedas
        print("🔍 Detectando total de criptomonedas...")
        
        items_per_page = 250  # Aumentado para menos requests
        if scraper.get_page(f"{scraper.base_url}/es?page=1&items={items_per_page}"):
            total_cryptos = scraper.get_total_cryptocurrencies()
            max_pages = (total_cryptos + items_per_page - 1) // items_per_page
            
            print(f"📊 Total detectado: {total_cryptos:,} criptomonedas")
            print(f"📄 Páginas estimadas: {max_pages}")
            
            # Opciones optimizadas
            print(f"\n🎯 Opciones de scraping:")
            print(f"1. Todas las páginas ({max_pages} páginas)")
            print(f"2. Número específico de páginas")
            print(f"3. Detección automática (recomendado)")
            print(f"4. Solo primeras 10 páginas (rápido)")
            
            choice = input("\nElige una opción (1/2/3/4) [3]: ").strip() or "3"
            
            if choice == "1":
                coins_data = scraper.scrape_pages(max_pages=max_pages, items_per_page=items_per_page)
            elif choice == "2":
                try:
                    custom_pages = int(input(f"Páginas a scrapear (1-{max_pages}): "))
                    custom_pages = min(max(1, custom_pages), max_pages)
                    coins_data = scraper.scrape_pages(max_pages=custom_pages, items_per_page=items_per_page)
                except ValueError:
                    print("⚠️ Número inválido, usando detección automática")
                    coins_data = scraper.scrape_pages(items_per_page=items_per_page)
            elif choice == "4":
                print("🚀 Scraping rápido - solo primeras 10 páginas")
                coins_data = scraper.scrape_pages(max_pages=10, items_per_page=items_per_page)
            else:
                print("🤖 Detección automática activada")
                coins_data = scraper.scrape_pages(items_per_page=items_per_page)
        else:
            print("❌ No se pudo cargar la primera página")
            print("🔄 Continuando con detección automática...")
            coins_data = scraper.scrape_pages(items_per_page=items_per_page)
        
        if coins_data:
            print(f"\n🎉 === RESULTADOS ===")
            print(f"Total extraídas: {len(coins_data):,}")
            print(f"Base de datos: PostgreSQL optimizado")
            
            # Mostrar muestra
            print(f"\n📊 Primeras 5 criptomonedas:")
            for i, coin in enumerate(coins_data[:5]):
                quote_info = ""
                if coin.get('quote'):
                    q = coin['quote']
                    quote_info = f" - ${q['price']:.2f}"
                
                print(f"{i+1}. {coin['name']} ({coin['symbol']}){quote_info}")
            
            # Preguntar por archivos
            save_files = input(f"\n💾 ¿Guardar en CSV/JSON? (y/n) [n]: ").strip().lower()
            if save_files == 'y':
                scraper.save_to_csv(coins_data)
                scraper.save_to_json(coins_data)
                print(f"✅ Archivos guardados")
            
            print(f"\n✅ Datos guardados en PostgreSQL optimizado")
            
        else:
            print("❌ No se pudieron extraer datos")
            
    except KeyboardInterrupt:
        print("🛑 Proceso interrumpido")
    except Exception as e:
        print(f"❌ Error: {e}")
        
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()