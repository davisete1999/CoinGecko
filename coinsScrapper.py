#!/usr/bin/env python3
"""
CoinGecko Data Scraper con Selenium - ADAPTADO AL ESQUEMA NORMALIZADO
Integrado con PostgreSQL usando el nuevo esquema normalizado
VERSIÓN ACTUALIZADA: Compatible con el esquema normalizado con tablas separadas por fuente
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
from urllib.parse import urlparse, urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('coingecko_scraper_normalized.log'),
        logging.StreamHandler(sys.stdout)
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
        
        # Debug: mostrar variables críticas
        critical_vars = ['POSTGRES_HOST', 'POSTGRES_DB', 'POSTGRES_USER']
        for var in critical_vars:
            value = os.getenv(var, 'NOT_SET')
            logger.info(f"🔧 {var}: {'SET' if value != 'NOT_SET' and value else 'EMPTY/NOT_SET'}")
            
    except FileNotFoundError:
        logger.warning(f"⚠️ Archivo {env_file} no encontrado, usando variables de entorno del sistema")
    except Exception as e:
        logger.error(f"❌ Error cargando {env_file}: {e}")

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
        logger.info(f"🔧 PostgreSQL Config: {self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

class NormalizedDatabaseManager:
    """Manejador de base de datos NORMALIZADO con funciones específicas para el esquema separado por fuentes"""
    
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
                    logger.info(f"✅ Conectado a PostgreSQL normalizado (CoinGecko source_id: {self.coingecko_source_id})")
                else:
                    logger.error("❌ Fuente 'coingecko' no encontrada en crypto_sources")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando a PostgreSQL: {e}")
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
            logger.error(f"❌ Error obteniendo/creando crypto {symbol}: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            return None
    
    def save_coingecko_crypto(self, crypto_data: Dict[str, Any]) -> Optional[int]:
        """Guardar crypto en esquema NORMALIZADO: tabla principal + coingecko_cryptos"""
        try:
            # PASO 1: Obtener o crear en tabla principal 'cryptos'
            crypto_id = self.get_or_create_crypto(
                crypto_data.get('name', ''),
                crypto_data.get('symbol', ''),
                crypto_data.get('slug', '')
            )
            
            if not crypto_id:
                logger.error(f"❌ No se pudo obtener crypto_id para {crypto_data.get('symbol')}")
                return None
            
            # PASO 2: Insertar/actualizar en tabla específica 'coingecko_cryptos'
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
                    crypto_data.get('tags', []),
                    crypto_data.get('badges', []),
                    crypto_data.get('market_pair_count', None),
                    date.today(),  # last_values_update
                    None,  # oldest_data_fetched
                    'completed',  # scraping_status (ya tenemos datos actuales)
                    1,  # total_data_points
                    datetime.now(timezone.utc),  # last_fetch_attempt
                    0,  # fetch_error_count
                    100,  # next_fetch_priority (será calculado automáticamente por trigger)
                    f'Datos obtenidos via CoinGecko scraping el {date.today()}'  # scraping_notes
                ))
                
                # PASO 3: Crear log de scraping
                self.create_scraping_log(crypto_id, 1, True)
                
                self.pg_conn.commit()
                logger.debug(f"✅ Guardado crypto normalizado ID {crypto_id} ({crypto_data.get('symbol')})")
                
                return crypto_id
                
        except Exception as e:
            logger.error(f"❌ Error guardando crypto normalizado {crypto_data.get('symbol', 'UNKNOWN')}: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            return None
    
    def create_scraping_log(self, crypto_id: int, data_points: int, success: bool, error_message: str = None):
        """Crear entrada en el log de scraping NORMALIZADO"""
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
        except Exception as e:
            logger.warning(f"⚠️ Error creando log de scraping normalizado: {e}")
    
    def save_crypto_batch_normalized(self, crypto_list: List[Dict[str, Any]]) -> int:
        """Guardar lote de cryptos en esquema NORMALIZADO y devolver cantidad guardada"""
        saved_count = 0
        
        if not crypto_list or not self.pg_conn:
            return saved_count
        
        try:
            for crypto_data in crypto_list:
                if crypto_data:
                    crypto_id = self.save_coingecko_crypto(crypto_data)
                    if crypto_id:
                        saved_count += 1
            
            logger.info(f"✅ Guardado lote NORMALIZADO de {saved_count}/{len(crypto_list)} cryptos en PostgreSQL")
            
        except Exception as e:
            logger.error(f"❌ Error guardando lote normalizado: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
        
        return saved_count
    
    def get_existing_coingecko_cryptos(self) -> Dict[str, int]:
        """Obtener mapa de símbolos existentes en CoinGecko"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT c.symbol, c.id 
                    FROM cryptos c 
                    JOIN coingecko_cryptos cg ON c.id = cg.crypto_id 
                    WHERE c.is_active = true
                """)
                return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"❌ Error obteniendo cryptos CoinGecko existentes: {e}")
            return {}
    
    def get_crypto_sources_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de fuentes de datos"""
        try:
            with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        cs.source_name,
                        cs.description,
                        CASE 
                            WHEN cs.source_name = 'coingecko' THEN 
                                (SELECT COUNT(*) FROM coingecko_cryptos cg 
                                 JOIN cryptos c ON cg.crypto_id = c.id 
                                 WHERE c.is_active = true)
                            WHEN cs.source_name = 'coinmarketcap' THEN 
                                (SELECT COUNT(*) FROM coinmarketcap_cryptos cmc 
                                 JOIN cryptos c ON cmc.crypto_id = c.id 
                                 WHERE c.is_active = true)
                            ELSE 0
                        END as crypto_count,
                        cs.is_active
                    FROM crypto_sources cs
                    ORDER BY crypto_count DESC
                """)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas de fuentes: {e}")
            return []
    
    def close(self):
        """Cerrar conexión"""
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("🔐 Conexión PostgreSQL normalizada cerrada")

class SeleniumCoinGeckoScrapperNormalized:
    """Scraper de CoinGecko ADAPTADO al esquema normalizado"""
    
    def __init__(self, headless=True, delay=2, db_config: DatabaseConfig = None):
        self.base_url = "https://www.coingecko.com"
        self.delay = delay
        self.driver = None
        self.db_config = db_config or DatabaseConfig()
        
        # Manejador de base de datos NORMALIZADO
        self.db_manager = NormalizedDatabaseManager(self.db_config)
        
        self.setup_driver(headless)
    
    def setup_driver(self, headless=True):
        """Configura el driver de Chrome con opciones anti-detección"""
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
        
        # Opciones anti-detección
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Deshabilitar imágenes para velocidad
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Anti-detección adicional
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("✅ Driver de Chrome configurado correctamente para esquema normalizado")
        except Exception as e:
            logger.error(f"❌ Error al configurar Chrome driver: {e}")
            raise
    
    def connect_databases(self):
        """Establecer conexión a PostgreSQL NORMALIZADO"""
        try:
            if not self.db_manager.connect():
                raise Exception("No se pudo conectar a PostgreSQL normalizado")
            
            logger.info("✅ Base de datos normalizada conectada correctamente")
            
        except Exception as e:
            logger.error(f"❌ Error conectando a base de datos normalizada: {e}")
            raise
    
    def random_delay(self, min_delay=1, max_delay=3):
        """Implementa un delay aleatorio entre requests"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def scroll_page(self):
        """Hace scroll para cargar contenido dinámico"""
        try:
            # Scroll gradual hacia abajo
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            scroll_increment = 500
            
            while current_position < total_height:
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(0.5)
                current_position += scroll_increment
                
                # Actualizar altura total por si se carga más contenido
                total_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # Scroll de vuelta arriba
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
        except Exception as e:
            logger.warning(f"⚠️ Error durante scroll: {e}")
    
    def get_page(self, url, wait_time=10):
        """Navega a una URL y espera a que cargue"""
        try:
            logger.info(f"🔄 Navegando a: {url}")
            self.driver.get(url)
            
            # Esperar a que la página cargue
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            # Hacer scroll para cargar contenido
            self.scroll_page()
            
            return True
            
        except TimeoutException:
            logger.error(f"❌ Timeout esperando que cargue la página: {url}")
            return False
        except Exception as e:
            logger.error(f"❌ Error al cargar página {url}: {e}")
            return False
    
    def get_total_cryptocurrencies(self) -> int:
        """Obtiene el total de criptomonedas disponibles usando múltiples selectores"""
        selectors_to_try = [
            # Selectores comunes para el total
            "span.tw-text-sm",
            "span.text-sm", 
            "div.tw-text-sm",
            ".tw-text-sm",
            "span[class*='text-sm']",
            "div[class*='text-sm']",
            # Selectores para paginación
            "div.pagination",
            ".pagination span",
            "nav span",
            # Selectores más específicos de CoinGecko
            "span.tw-text-gray-500",
            "div.tw-text-gray-500",
            "p.tw-text-sm",
            # Fallback genéricos
            "span:contains('coins')",
            "div:contains('cryptocurrencies')",
            "span:contains('crypto')"
        ]
        
        for selector in selectors_to_try:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    text = element.text.strip()
                    logger.debug(f"🔍 Probando selector '{selector}': '{text}'")
                    
                    # Buscar números que indiquen total de cryptos
                    if any(keyword in text.lower() for keyword in ['coin', 'crypto', 'total', 'showing']):
                        # Extraer número más grande del texto
                        numbers = re.findall(r'\d{1,3}(?:,\d{3})*', text)
                        if numbers:
                            for num_str in sorted(numbers, key=len, reverse=True):
                                try:
                                    num = int(num_str.replace(',', ''))
                                    if 100 <= num <= 50000:  # Rango razonable para cryptos
                                        logger.info(f"✅ Total encontrado: {num} cryptos usando selector '{selector}'")
                                        return num
                                except ValueError:
                                    continue
                        
            except Exception as e:
                logger.debug(f"⚠️ Error con selector '{selector}': {e}")
                continue
        
        # Fallback: intentar contar filas de la primera página
        try:
            table = self.driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            if len(rows) > 0:
                # Estimar basado en filas de la primera página
                estimated_total = len(rows) * 50  # Estimación conservadora
                logger.warning(f"⚠️ No se pudo obtener total exacto, estimando {estimated_total} basado en {len(rows)} filas")
                return estimated_total
        except Exception as e:
            logger.warning(f"⚠️ No se pudo estimar total desde tabla: {e}")
        
        # Último fallback
        logger.warning("⚠️ No se pudo determinar total, usando fallback de 2000 cryptos")
        return 2000
    
    def is_page_empty(self) -> bool:
        """Verifica si la página actual está vacía (no tiene datos de cryptos)"""
        try:
            # Verificar si hay tabla
            table = self.driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            if len(rows) == 0:
                return True
            
            # Verificar si las filas tienen contenido válido
            valid_rows = 0
            for row in rows[:5]:  # Verificar solo las primeras 5 filas
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:  # Necesitamos al menos 3 columnas
                    # Verificar si hay contenido de crypto válido
                    for cell in cells:
                        if any(indicator in cell.text.lower() for indicator in ['$', 'btc', 'eth', '%']):
                            valid_rows += 1
                            break
            
            return valid_rows == 0
            
        except (NoSuchElementException, TimeoutException):
            return True
        except Exception as e:
            logger.warning(f"⚠️ Error verificando si página está vacía: {e}")
            return True
    
    def parse_number(self, text: str) -> float:
        """Parsear números con formato de moneda y abreviaciones"""
        if not text or text.strip() in ['--', '-', 'N/A', '']:
            return 0.0
        
        try:
            # Limpiar el texto
            clean_text = text.strip().replace('$', '').replace(',', '').replace('%', '')
            
            # Manejar abreviaciones (B = billones, M = millones, K = miles)
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
        """Extrae datos de criptomonedas de la página actual para esquema NORMALIZADO"""
        coins_data = []
        
        try:
            # Buscar la tabla
            table = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            # Buscar todas las filas de datos
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            logger.info(f"🔍 Encontradas {len(rows)} filas en la tabla")
            
            for i, row in enumerate(rows):
                try:
                    coin_data = self.extract_coin_from_row_normalized(row, i + 1)
                    if coin_data:
                        coins_data.append(coin_data)
                        
                except Exception as e:
                    logger.warning(f"⚠️ Error extrayendo datos de fila {i}: {e}")
                    continue
            
            logger.info(f"✅ Extraídos datos de {len(coins_data)} criptomonedas para esquema normalizado")
            return coins_data
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos de la página: {e}")
            return []
    
    def extract_coin_from_row_normalized(self, row, rank: int):
        """Extrae datos de una fila específica para el esquema NORMALIZADO
        - Symbol: primero <img alt="SYMBOL">, fallback a badge pequeño
        - Slug: del último segmento del path, independiente del idioma (/es/monedas/, /coins/, etc.)
        """
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 6:
                return None

            # === Enlace principal (suele estar en la 3ª columna visible) ===
            link_element = None
            for cell_idx in [1, 2]:
                try:
                    link_element = cells[cell_idx].find_element(By.CSS_SELECTOR, "a")
                    break
                except NoSuchElementException:
                    continue
            if not link_element:
                logger.warning(f"⚠️ No se encontró enlace en fila {rank}")
                return None

            raw_href = link_element.get_attribute("href") or ""
            # Normaliza a URL absoluta
            coin_url = urljoin(self.base_url, raw_href)

            # === Icono / symbol por <img alt="..."> ===
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

            # === Nombre (primer texto del bloque con font-semibold, sin el badge) ===
            name = ""
            try:
                # Ese div contiene "Bitcoin\nBTC"; tomamos sólo el texto directo (sin hijos) vía JS
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
                # Fallback: del texto completo del link, quitando el symbol si ya lo tenemos
                link_text = (link_element.text or "").strip()
                if symbol:
                    link_text = re.sub(rf"\b{re.escape(symbol)}\b", "", link_text)
                name = next((ln.strip() for ln in link_text.splitlines() if ln.strip()), "")

            # === Fallback de symbol si el alt no lo dio ===
            if not symbol:
                try:
                    # Buscar badges pequeños de símbolo (text-xs)
                    smalls = link_element.find_elements(By.XPATH, ".//*[contains(@class,'text-xs') or contains(@class,'tw-text-xs')]")
                    for sm in smalls:
                        cand = re.sub(r"[^A-Za-z0-9.$-]", "", (sm.text or "").strip())
                        if 1 <= len(cand) <= 15 and cand.upper() != (name or "").upper():
                            symbol = cand.upper()
                            break
                except Exception:
                    pass

            # Asegura valores mínimos
            name = name or ""
            symbol = symbol or ""

            # === Slug robusto: último segmento del path (independiente del idioma) ===
            slug = ""
            try:
                p = urlparse(coin_url)
                # p.path típicamente: /es/monedas/bitcoin  o /coins/bitcoin
                parts = [seg for seg in p.path.split("/") if seg]
                if parts:
                    # ignora prefijo de locale si lo hay (es, en, pt-br, zh-tw, etc.)
                    if len(parts[0]) <= 5 and re.match(r"^[a-z]{2}(-[a-z]{2})?$", parts[0], re.I):
                        parts = parts[1:]
                    # ignora secciones conocidas "coins", "monedas", etc.
                    if parts and parts[0].lower() in {"coins", "monedas", "moedas", "monete", "monnaies"}:
                        parts = parts[1:]
                    slug = parts[0] if parts else ""
            except Exception:
                pass

            # === Datos numéricos (igual que antes) ===
            price = 0.0
            volume_24h = 0.0
            market_cap = 0.0
            percent_change_24h = 0.0

            try:
                # Precio: busca primera celda con "$" o "US$"
                for cell_idx in [2, 3, 4]:
                    if cell_idx < len(cells):
                        cell_text = (cells[cell_idx].text or "").strip()
                        if ("$" in cell_text or "US$" in cell_text) and price == 0.0:
                            price = self.parse_number(cell_text)
                            break

                # % 24h
                for cell_idx in range(len(cells)):
                    cell_text = (cells[cell_idx].text or "").strip()
                    if "%" in cell_text and percent_change_24h == 0.0:
                        percent_change_24h = self.parse_number(cell_text)
                        break

                # Market cap y volumen (heurística)
                for cell_idx in range(len(cells)):
                    cell_text = (cells[cell_idx].text or "").strip()
                    if "$" in cell_text or "US$" in cell_text:
                        parsed_value = self.parse_number(cell_text)
                        if parsed_value > 1_000_000 and market_cap == 0.0:
                            market_cap = parsed_value
                        elif parsed_value > 100_000 and volume_24h == 0.0:
                            volume_24h = parsed_value
            except Exception as e:
                logger.warning(f"⚠️ Error extrayendo datos numéricos para {symbol or name}: {e}")

            coin_data = {
                'name': name,
                'symbol': symbol,
                'slug': slug or (f"coingecko-{symbol.lower()}" if symbol else f"unknown-{rank}"),

                'rank': rank,
                'icon_url': icon_url,
                'coin_url': coin_url,
                'tags': ['coingecko-scraped-normalized'],
                'badges': ['live-data-normalized'] if price > 0 else ['scraped-normalized'],
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

        except Exception as e:
            logger.warning(f"⚠️ Error extrayendo datos de fila {rank}: {e}")
            return None

    
    def scrape_pages_normalized(self, max_pages=None, items_per_page=50):
        """
        Scrapea múltiples páginas y guarda en PostgreSQL usando el esquema NORMALIZADO
        VERSIÓN MEJORADA: Detección automática de fin de páginas y mejor manejo de errores
        """
        all_coins = []
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        logger.info(f"🚀 Iniciando scraping NORMALIZADO {'de todas las páginas disponibles' if max_pages is None else f'de {max_pages} páginas'}")
        
        # Conectar a base de datos
        try:
            self.connect_databases()
        except Exception as e:
            logger.error(f"❌ No se pudo conectar a la base de datos normalizada: {e}")
            return []
        
        # Obtener cryptos existentes para evitar duplicados
        existing_cryptos = self.db_manager.get_existing_coingecko_cryptos()
        logger.info(f"📊 Encontradas {len(existing_cryptos)} cryptos CoinGecko existentes en BD normalizada")
        
        # Mostrar estadísticas de fuentes
        sources_stats = self.db_manager.get_crypto_sources_stats()
        if sources_stats:
            logger.info("📊 Estadísticas actuales de fuentes:")
            for stat in sources_stats:
                logger.info(f"  - {stat['source_name']}: {stat['crypto_count']} cryptos")
        
        page = 1
        while True:
            try:
                # Verificar si hemos alcanzado el máximo de páginas
                if max_pages is not None and page > max_pages:
                    logger.info(f"🏁 Alcanzado límite máximo de {max_pages} páginas")
                    break
                
                url = f"{self.base_url}/es?page={page}&items={items_per_page}"
                
                if self.get_page(url):
                    # Verificar si la página está vacía
                    if self.is_page_empty():
                        logger.info(f"📄 Página {page} está vacía - Fin del scraping normalizado")
                        break
                    
                    coins_data = self.extract_coins_from_page()
                    
                    # Si no hay datos en la página, terminar
                    if not coins_data:
                        logger.warning(f"⚠️ No se obtuvieron datos de la página {page}")
                        consecutive_errors += 1
                        
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(f"❌ {consecutive_errors} errores consecutivos - Terminando scraping normalizado")
                            break
                        
                        page += 1
                        continue
                    
                    # Reset contador de errores en página exitosa
                    consecutive_errors = 0
                    
                    # Filtrar cryptos nuevas
                    new_coins = [
                        coin for coin in coins_data 
                        if coin and coin.get('symbol') not in existing_cryptos
                    ]
                    
                    all_coins.extend(coins_data)
                    
                    logger.info(f"📄 Página {page}: {len(coins_data)} total, {len(new_coins)} nuevas")
                    
                    # Guardar en PostgreSQL NORMALIZADO
                    if coins_data and self.db_manager.pg_conn:
                        try:
                            saved_count = self.db_manager.save_crypto_batch_normalized(coins_data)
                            logger.info(f"💾 Guardadas {saved_count} cryptos en PostgreSQL normalizado")
                            
                            # Actualizar mapa de existentes
                            for coin in coins_data:
                                if coin and coin.get('symbol'):
                                    existing_cryptos[coin['symbol']] = coin.get('rank', 0)
                            
                        except Exception as e:
                            logger.error(f"❌ Error guardando página {page} en PostgreSQL normalizado: {e}")
                    
                    # Verificar si obtuvimos menos elementos de los esperados (indica final)
                    if len(coins_data) < items_per_page:
                        logger.info(f"🏁 Página {page} tiene solo {len(coins_data)} elementos (< {items_per_page}) - Posible fin")
                        # No romper inmediatamente, verificar siguiente página para confirmar
                    
                    # Delay entre páginas (comentado para velocidad)
                    # self.random_delay(self.delay, self.delay * 2)
                    page += 1
                    
                else:
                    logger.error(f"❌ No se pudo cargar la página {page}")
                    consecutive_errors += 1
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"❌ {consecutive_errors} errores consecutivos cargando páginas - Terminando")
                        break
                    
                    page += 1
                    
            except KeyboardInterrupt:
                logger.info("🛑 Scraping normalizado interrumpido por el usuario")
                break
            except Exception as e:
                logger.error(f"❌ Error en página {page}: {e}")
                consecutive_errors += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"❌ {consecutive_errors} errores consecutivos - Terminando")
                    break
                
                page += 1
                continue
        
        logger.info(f"🎉 Scraping NORMALIZADO completado. Total: {len(all_coins)} criptomonedas, Páginas procesadas: {page-1}")
        
        # Mostrar estadísticas finales
        final_stats = self.db_manager.get_crypto_sources_stats()
        if final_stats:
            logger.info("📊 Estadísticas finales de fuentes:")
            for stat in final_stats:
                logger.info(f"  - {stat['source_name']}: {stat['crypto_count']} cryptos")
        
        return all_coins
    
    def save_to_csv_normalized(self, data, filename='criptomonedas_coingecko_normalizado.csv'):
        """Guarda los datos en CSV con formato NORMALIZADO"""
        try:
            # Aplanar los datos para CSV
            flattened_data = []
            for crypto in data:
                row = {
                    # Datos tabla principal
                    'name': crypto.get('name', ''),
                    'symbol': crypto.get('symbol', ''),
                    'slug': crypto.get('slug', ''),
                    'source': crypto.get('source', 'coingecko'),
                    'is_active': crypto.get('is_active', True),
                    'extracted_at': crypto.get('extracted_at', ''),
                    
                    # Datos específicos CoinGecko
                    'coingecko_rank': crypto.get('rank', 0),
                    'icon_url': crypto.get('icon_url', ''),
                    'coin_url': crypto.get('coin_url', ''),
                    'tags': ','.join(crypto.get('tags', [])),
                    'badges': ','.join(crypto.get('badges', [])),
                    'market_pair_count': crypto.get('market_pair_count', ''),
                }
                
                # Añadir datos de quote si existen (solo para referencia)
                if crypto.get('quote'):
                    quote = crypto['quote']
                    row.update({
                        'price': quote.get('price', 0),
                        'volume_24h': quote.get('volume_24h', 0),
                        'market_cap': quote.get('market_cap', 0),
                        'percent_change_1h': quote.get('percent_change_1h', 0),
                        'percent_change_24h': quote.get('percent_change_24h', 0),
                        'percent_change_7d': quote.get('percent_change_7d', 0),
                    })
                else:
                    row.update({
                        'price': 0, 'volume_24h': 0, 'market_cap': 0,
                        'percent_change_1h': 0, 'percent_change_24h': 0, 'percent_change_7d': 0
                    })
                
                flattened_data.append(row)
            
            df = pd.DataFrame(flattened_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"✅ Datos normalizados guardados en {filename}")
        except Exception as e:
            logger.error(f"❌ Error guardando CSV normalizado: {e}")
    
    def save_to_json_normalized(self, data, filename='criptomonedas_coingecko_normalizado.json'):
        """Guarda los datos en JSON con formato NORMALIZADO"""
        try:
            # Convertir datetime a string para JSON
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
                
                # Añadir metadatos del esquema normalizado
                crypto_copy['schema_version'] = 'normalized_v1'
                crypto_copy['source_table'] = 'coingecko_cryptos'
                crypto_copy['main_table'] = 'cryptos'
                
                json_data.append(crypto_copy)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Datos normalizados guardados en {filename}")
        except Exception as e:
            logger.error(f"❌ Error guardando JSON normalizado: {e}")
    
    def close(self):
        """Cierra el driver y conexiones de base de datos"""
        if self.driver:
            self.driver.quit()
            logger.info("🔐 Driver cerrado")
        
        if self.db_manager:
            self.db_manager.close()

def main():
    """
    Función principal ADAPTADA AL ESQUEMA NORMALIZADO
    """
    scraper = None
    
    try:
        print("🚀 === CoinGecko Scraper NORMALIZADO - ESQUEMA SEPARADO POR FUENTES ===")
        print("🔥 CARACTERÍSTICAS NORMALIZADAS:")
        print("  - Tabla principal 'cryptos' unificada")
        print("  - Tabla específica 'coingecko_cryptos' con datos de CoinGecko")
        print("  - Función get_or_create_crypto() para evitar duplicados")
        print("  - Log de scraping unificado por fuente")
        print("  - Triggers automáticos para prioridad de fetch")
        print("💾 Base de datos: PostgreSQL con esquema normalizado")
        print("🔧 Compatibilidad: CoinGecko + CoinMarketCap en BD separadas\n")
        
        # Cargar variables de entorno
        load_env_file()
        
        # Crear configuración de base de datos
        db_config = DatabaseConfig()
        
        # Crear scraper NORMALIZADO
        scraper = SeleniumCoinGeckoScrapperNormalized(headless=True, delay=0, db_config=db_config)
        
        # Determinar número de páginas a scrapear
        print("🔍 Detectando total de criptomonedas disponibles...")
        
        # Cargar primera página para detectar total
        items_per_page = 300
        if scraper.get_page(f"{scraper.base_url}/es?page=1&items={items_per_page}"):
            total_cryptos = scraper.get_total_cryptocurrencies()
            max_pages = (total_cryptos + items_per_page - 1) // items_per_page
            
            print(f"📊 Total detectado: {total_cryptos:,} criptomonedas")
            print(f"📄 Páginas estimadas: {max_pages}")
            print(f"🔧 Esquema: NORMALIZADO (tablas separadas)")
            
            # Preguntar al usuario qué hacer
            print(f"\n🎯 Opciones de scraping NORMALIZADO:")
            print(f"1. Scraper TODAS las páginas ({max_pages} páginas) - Esquema normalizado")
            print(f"2. Scrapear un número específico de páginas - Esquema normalizado")
            print(f"3. Usar detección automática (recomendado) - Esquema normalizado")
            
            choice = input("\nElige una opción (1/2/3) [3]: ").strip() or "3"
            
            if choice == "1":
                # Scrapear todas las páginas detectadas
                coins_data = scraper.scrape_pages_normalized(max_pages=max_pages, items_per_page=items_per_page)
            elif choice == "2":
                # Número específico
                try:
                    custom_pages = int(input(f"Número de páginas a scrapear (1-{max_pages}): "))
                    custom_pages = min(max(1, custom_pages), max_pages)
                    coins_data = scraper.scrape_pages_normalized(max_pages=custom_pages, items_per_page=items_per_page)
                except ValueError:
                    print("⚠️ Número inválido, usando detección automática")
                    coins_data = scraper.scrape_pages_normalized(items_per_page=items_per_page)
            else:
                # Detección automática (opción 3 o default)
                print("🤖 Usando detección automática - el scraper parará cuando no encuentre más datos")
                coins_data = scraper.scrape_pages_normalized(items_per_page=items_per_page)
        else:
            print("❌ No se pudo cargar la primera página para detectar total")
            print("🔄 Continuando con detección automática...")
            coins_data = scraper.scrape_pages_normalized(items_per_page=items_per_page)
        
        if coins_data:
            print(f"\n🎉 === RESULTADOS NORMALIZADOS ===")
            print(f"Total de criptomonedas extraídas: {len(coins_data):,}")
            print(f"Esquema utilizado: NORMALIZADO")
            print(f"Tablas utilizadas:")
            print(f"  - cryptos (tabla principal)")
            print(f"  - coingecko_cryptos (datos específicos)")
            print(f"  - crypto_scraping_log (log unificado)")
            
            # Mostrar muestra
            print(f"\n📊 Primeras 5 criptomonedas (formato normalizado):")
            for i, coin in enumerate(coins_data[:5]):
                quote_info = ""
                if coin.get('quote'):
                    q = coin['quote']
                    quote_info = f" - ${q['price']:.2f} (MCap: ${q['market_cap']:,.0f})"
                
                print(f"{i+1}. {coin['name']} ({coin['symbol']}){quote_info}")
                print(f"   Fuente: {coin.get('source', 'coingecko')}")
                print(f"   Tags: {', '.join(coin.get('tags', []))}")
                print(f"   URL: {coin['coin_url']}")
                print(f"   Slug: {coin['slug']}")
            
            # Preguntar si guardar archivos adicionales
            save_files = input(f"\n💾 ¿Guardar también en archivos CSV/JSON? (y/n) [n]: ").strip().lower()
            if save_files == 'y':
                scraper.save_to_csv_normalized(coins_data)
                scraper.save_to_json_normalized(coins_data)
                print(f"✅ Archivos CSV y JSON guardados con formato normalizado")
            
            print(f"\n✅ Datos guardados en:")
            print(f"- PostgreSQL NORMALIZADO:")
            print(f"  ├── cryptos (tabla principal unificada)")
            print(f"  ├── coingecko_cryptos (datos específicos CoinGecko)")
            print(f"  ├── crypto_sources (catálogo de fuentes)")
            print(f"  └── crypto_scraping_log (log unificado por fuente)")
            
            if save_files == 'y':
                print(f"- Archivos:")
                print(f"  ├── criptomonedas_coingecko_normalizado.csv")
                print(f"  └── criptomonedas_coingecko_normalizado.json")
            
        else:
            print("❌ No se pudieron extraer datos")
            
    except KeyboardInterrupt:
        logger.info("🛑 Proceso interrumpido por el usuario")
    except Exception as e:
        logger.error(f"❌ Error en main normalizado: {e}")
        
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()