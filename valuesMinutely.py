#!/usr/bin/env python3
"""
Script para descargar datos de criptomonedas en rangos de 5 días desde CoinGecko usando Selenium
Guarda datos en formato continuo: [{timestamp, price, market_cap}, ...]
Con tracking de última fecha procesada para continuar desde donde se quedó
"""

import json
import os
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SeleniumRangeCryptoDataScraper:
    def __init__(self, 
                 crypto_file: str = "criptomonedas.json", 
                 values_dir: str = "values",
                 daily_dir: str = "daily_data", 
                 delay: float = 2.0,
                 headless: bool = True):
        self.crypto_file = crypto_file
        self.values_dir = values_dir
        self.daily_dir = daily_dir
        self.delay = delay
        self.driver = None
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
            logger.info("Driver de Chrome configurado correctamente")
        except Exception as e:
            logger.error(f"Error al configurar Chrome driver: {e}")
            logger.error("\n🚨 CHROME NO ESTÁ INSTALADO 🚨")
            logger.error("Instala Chrome en tu sistema:")
            logger.error("Ubuntu/Debian:")
            logger.error("  wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -")
            logger.error("  sudo sh -c 'echo \"deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main\" >> /etc/apt/sources.list.d/google-chrome.list'")
            logger.error("  sudo apt update && sudo apt install google-chrome-stable")
            logger.error("CentOS/RHEL/Fedora:")
            logger.error("  sudo dnf install google-chrome-stable")
            raise
    
    def load_download_log(self) -> Dict:
        """Carga el log de días ya descargados"""
        try:
            if os.path.exists(self.download_log):
                with open(self.download_log, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.warning(f"Error cargando log de descargas: {e}")
            return {}
    
    def save_download_log(self):
        """Guarda el log de días descargados"""
        try:
            with open(self.download_log, 'w', encoding='utf-8') as f:
                json.dump(self.downloaded_days, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error guardando log de descargas: {e}")
    
    def load_cryptocurrencies(self) -> List[Dict]:
        """Carga la lista de criptomonedas"""
        try:
            with open(self.crypto_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"No se encontró el archivo {self.crypto_file}")
            return []
        except json.JSONDecodeError:
            logger.error(f"El archivo {self.crypto_file} no tiene un formato JSON válido")
            return []
    
    def group_dates_into_ranges(self, dates: List[str], range_days: int = 30) -> List[Tuple[str, str]]:
        """Agrupa fechas en rangos de N días"""
        if not dates:
            return []
        
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
        """Extrae el nombre de la URL del enlace"""
        return enlace.rstrip('/').split('/')[-1]
    
    def get_available_dates_from_values(self, symbol: str) -> Set[str]:
        """Obtiene las fechas disponibles del archivo de valores generales"""
        try:
            values_file = os.path.join(self.values_dir, f"{symbol}.json")
            if not os.path.exists(values_file):
                logger.warning(f"No se encontró archivo de valores para {symbol}")
                return set()
            
            with open(values_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            dates = set()
            for item in data.get('data', []):
                timestamp = item.get('timestamp')
                if timestamp:
                    # Convertir timestamp a fecha
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    date_str = dt.strftime('%Y-%m-%d')
                    dates.add(date_str)
            
            logger.info(f"Encontradas {len(dates)} fechas únicas para {symbol}")
            return dates
            
        except Exception as e:
            logger.error(f"Error leyendo fechas de {symbol}: {e}")
            return set()
    
    def timestamp_for_date(self, date_str: str, is_end: bool = False) -> int:
        """Convierte fecha string a timestamp Unix"""
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
            logger.error(f"Error convirtiendo fecha {date_str}: {e}")
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
            logger.warning(f"Error cargando datos de {symbol}: {e}")
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
            logger.error(f"Error guardando datos consolidados de {symbol}: {e}")
            return False
    
    def is_already_downloaded(self, symbol: str, date: str, data_type: str = None) -> bool:
        """Verifica si ya se procesó esta fecha"""
        crypto_data = self.load_crypto_daily_data(symbol)
        last_processed = crypto_data.get('last_processed_date')
        
        if last_processed:
            return date <= last_processed
        
        return False
    
    def combine_range_data(self, price_data: List, market_cap_data: List) -> List[Dict]:
        """Combina datos de precios y capitalización por timestamp para un rango"""
        combined_data = []
        
        # Convertir market_cap_data a diccionario para búsqueda rápida
        market_cap_dict = {int(item[0]): item[1] for item in market_cap_data} if market_cap_data else {}
        
        # Procesar datos de precios y combinar con market cap
        for price_item in price_data:
            if len(price_item) >= 2:
                timestamp = int(price_item[0])
                price = price_item[1]
                market_cap = market_cap_dict.get(timestamp, None)
                
                combined_data.append({
                    'timestamp': timestamp,
                    'price': price,
                    'market_cap': market_cap
                })
        
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
            crypto_data['data'].sort(key=lambda x: x['timestamp'])
            
            # Guardar archivo consolidado
            if self.save_crypto_daily_data(symbol, crypto_data):
                logger.info(f"✓ Añadidos {len(combined_data)} registros para {symbol} (hasta {end_date})")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error añadiendo datos de rango de {symbol} (hasta {end_date}): {e}")
            return False
    
    def download_range_data_selenium(self, url_name: str, start_date: str, end_date: str, data_type: str) -> Optional[List]:
        """Descarga datos para un rango de fechas usando Selenium"""
        timestamp_from = self.timestamp_for_date(start_date)
        timestamp_to = self.timestamp_for_date(end_date, True)
        
        url = f"https://www.coingecko.com/{data_type}/{url_name}/usd/custom.json?from={timestamp_from}&to={timestamp_to}"
        
        try:
            logger.info(f"Descargando {data_type} para {url_name} - {start_date} a {end_date}")
            logger.debug(f"URL: {url}")
            
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
                try:
                    body_element = self.driver.find_element(By.TAG_NAME, "body")
                    json_text = body_element.text
                except Exception:
                    logger.error(f"No se pudo encontrar contenido JSON para {url_name} - {start_date} a {end_date}")
                    return None
            
            # Verificar si hay contenido
            if not json_text.strip():
                logger.warning(f"Respuesta vacía para {url_name} - {start_date} a {end_date} ({data_type})")
                return None
            
            # Parsear el JSON
            try:
                data = json.loads(json_text)
                stats = data.get('stats', [])
                
                logger.info(f"✓ {len(stats)} registros para {url_name} - {start_date} a {end_date} ({data_type})")
                return stats
                
            except json.JSONDecodeError as e:
                logger.error(f"Error al parsear JSON para {url_name} - {start_date} a {end_date} ({data_type}): {e}")
                logger.debug(f"Contenido recibido: {json_text[:500]}...")
                return None
                
        except TimeoutException:
            logger.error(f"Timeout al cargar datos para {url_name} - {start_date} a {end_date} ({data_type})")
            return None
        except WebDriverException as e:
            logger.error(f"Error de WebDriver para {url_name} - {start_date} a {end_date} ({data_type}): {e}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado para {url_name} - {start_date} a {end_date} ({data_type}): {e}")
            return None
    
    def save_daily_data(self, symbol: str, date: str, data_type: str, data: List) -> bool:
        """Guarda datos diarios en archivo"""
        try:
            # Crear directorio por tipo de datos
            type_dir = os.path.join(self.daily_dir, data_type)
            os.makedirs(type_dir, exist_ok=True)
            
            # Nombre de archivo: SYMBOL_YYYY-MM-DD.json
            filename = f"{symbol}_{date}.json"
            filepath = os.path.join(type_dir, filename)
            
            file_data = {
                'symbol': symbol,
                'date': date,
                'data_type': data_type,
                'timestamp_from': self.timestamp_for_date(date),
                'timestamp_to': self.timestamp_for_date(date, True),
                'records_count': len(data),
                'data': data,
                'downloaded_at': datetime.now().isoformat()
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(file_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✓ Guardado: {filename} ({len(data)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"Error guardando {symbol} - {date} ({data_type}): {e}")
            return False
    
    def random_delay(self, min_delay: float = None, max_delay: float = None):
        """Delay aleatorio entre peticiones"""
        if min_delay is None:
            min_delay = self.delay
        if max_delay is None:
            max_delay = self.delay * 1.5
        
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def process_cryptocurrency_daily(self, crypto: Dict) -> Dict:
        """Procesa una criptomoneda por rangos de 5 días"""
        symbol = crypto.get('simbolo', '').upper()
        enlace = crypto.get('enlace', '')
        nombre = crypto.get('nombre', '')
        
        if not symbol or not enlace:
            logger.warning(f"Datos incompletos para {nombre}")
            return {'symbol': symbol, 'success': 0, 'failed': 0, 'skipped': 0}
        
        logger.info(f"\n📊 Procesando {nombre} ({symbol})")
        
        # Extraer nombre de URL
        url_name = self.extract_url_name(enlace)
        logger.info(f"URL name: {url_name}")
        
        # Obtener fechas disponibles del archivo general
        available_dates = self.get_available_dates_from_values(symbol)
        if not available_dates:
            logger.warning(f"No hay fechas disponibles para {symbol}")
            return {'symbol': symbol, 'success': 0, 'failed': 0, 'skipped': 0}
        
        # Verificar última fecha procesada y determinar desde dónde continuar
        last_processed = self.get_last_processed_date(symbol)
        sorted_dates = sorted(list(available_dates))
        
        if last_processed:
            logger.info(f"Última fecha procesada: {last_processed}")
            
            # Obtener la siguiente fecha desde donde continuar
            next_start_date = self.get_next_start_date(sorted_dates, last_processed)
            
            if not next_start_date:
                logger.info(f"✅ {symbol} está completamente actualizado (hasta {last_processed})")
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
            return {'symbol': symbol, 'success': 0, 'failed': 0, 'skipped': len(available_dates)}
        
        # Agrupar fechas en rangos de 5 días
        date_ranges = self.group_dates_into_ranges(pending_dates, 30)
        logger.info(f"📦 Agrupado en {len(date_ranges)} rangos de hasta 30 días cada uno")
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for i, (start_date, end_date) in enumerate(date_ranges, 1):
            try:
                logger.info(f"📦 [{i}/{len(date_ranges)}] Rango {symbol}: {start_date} → {end_date}")
                
                # Descargar datos de precios para el rango
                price_data = self.download_range_data_selenium(url_name, start_date, end_date, 'price_charts')
                if not price_data:
                    logger.error(f"❌ No se pudieron obtener datos de precios para {symbol} - {start_date} a {end_date}")
                    failed_count += 1
                    continue
                
                # Delay entre peticiones
                self.random_delay()
                
                # Descargar datos de capitalización de mercado para el rango
                market_cap_data = self.download_range_data_selenium(url_name, start_date, end_date, 'market_cap')
                if not market_cap_data:
                    logger.warning(f"⚠️  No se pudieron obtener datos de capitalización para {symbol} - {start_date} a {end_date}")
                    market_cap_data = []
                
                # Combinar datos
                combined_data = self.combine_range_data(price_data, market_cap_data)
                
                if combined_data:
                    # Guardar datos combinados (actualiza last_processed_date al end_date)
                    if self.add_range_data_to_crypto(symbol, end_date, combined_data):
                        success_count += 1
                        logger.info(f"✅ Rango completado: {symbol} hasta {end_date}")
                    else:
                        failed_count += 1
                        logger.error(f"❌ Error guardando datos de {symbol} - {start_date} a {end_date}")
                else:
                    failed_count += 1
                    logger.error(f"❌ No se generaron datos combinados para {symbol} - {start_date} a {end_date}")
                
                # Delay entre rangos
                self.random_delay()
                
            except Exception as e:
                logger.error(f"❌ Error procesando {symbol} - {start_date} a {end_date}: {e}")
                failed_count += 1
        
        return {
            'symbol': symbol,
            'success': success_count,
            'failed': failed_count,
            'skipped': skipped_count
        }
    
    def run(self) -> None:
        """Ejecuta el proceso completo"""
        cryptocurrencies = self.load_cryptocurrencies()
        
        if not cryptocurrencies:
            logger.error("No se encontraron criptomonedas para procesar")
            return
        
        logger.info(f"🚀 Iniciando descarga por rangos de 5 días para {len(cryptocurrencies)} criptomonedas")
        logger.info(f"📁 Datos se guardarán en: {os.path.abspath(self.daily_dir)}")
        
        total_stats = {
            'processed': 0,
            'total_success': 0,
            'total_failed': 0,
            'total_skipped': 0
        }
        
        try:
            for i, crypto in enumerate(cryptocurrencies, 1):
                logger.info(f"\n[{i}/{len(cryptocurrencies)}] ======================")
                
                try:
                    result = self.process_cryptocurrency_daily(crypto)
                    
                    total_stats['processed'] += 1
                    total_stats['total_success'] += result['success']
                    total_stats['total_failed'] += result['failed']
                    total_stats['total_skipped'] += result['skipped']
                    
                    logger.info(f"Resultado {result['symbol']}: ✅{result['success']} ❌{result['failed']} ⏭️{result['skipped']}")
                    
                except Exception as e:
                    logger.error(f"Error procesando {crypto.get('nombre', 'Desconocido')}: {e}")
                    total_stats['total_failed'] += 1
                
                # Pausa entre criptomonedas
                if i < len(cryptocurrencies):
                    self.random_delay(self.delay * 2, self.delay * 3)
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Proceso interrumpido por el usuario")
        
        logger.info(f"\n\n📈 === RESUMEN FINAL ===")
        logger.info(f"🔢 Criptomonedas procesadas: {total_stats['processed']}")
        logger.info(f"✅ Rangos exitosos: {total_stats['total_success']}")
        logger.info(f"❌ Rangos fallidos: {total_stats['total_failed']}")
        logger.info(f"⏭️  Rangos saltados: {total_stats['total_skipped']}")
        logger.info(f"📁 Archivos consolidados en: {os.path.abspath(self.daily_dir)}")
        
        # Mostrar estadísticas finales de archivos consolidados
        final_stats = self.get_download_statistics()
        logger.info(f"\n📊 === ESTADÍSTICAS DE ARCHIVOS CONSOLIDADOS ===")
        logger.info(f"Total de archivos: {final_stats['total_crypto_files']}")
        logger.info(f"Total de registros: {final_stats['total_downloads']}")
        for symbol, info in final_stats['consolidation_info'].items():
            first_date = datetime.fromtimestamp(info['first_timestamp']/1000).strftime('%Y-%m-%d') if info['first_timestamp'] else 'N/A'
            last_date = datetime.fromtimestamp(info['last_timestamp']/1000).strftime('%Y-%m-%d') if info['last_timestamp'] else 'N/A'
            logger.info(f"  {symbol}: {info['total_records']} registros ({first_date} a {last_date})")
            logger.info(f"    └─ Última fecha procesada: {info['last_processed_date']}")
    
    def get_download_statistics(self) -> Dict:
        """Obtiene estadísticas de descargas desde archivos consolidados"""
        stats = {
            'total_downloads': 0,
            'by_symbol': {},
            'total_crypto_files': 0,
            'consolidation_info': {}
        }
        
        # Buscar todos los archivos consolidados
        for filename in os.listdir(self.daily_dir):
            if filename.endswith('_daily_data.json'):
                symbol = filename.replace('_daily_data.json', '')
                try:
                    crypto_data = self.load_crypto_daily_data(symbol)
                    data_list = crypto_data.get('data', [])
                    
                    stats['total_crypto_files'] += 1
                    stats['by_symbol'][symbol] = len(data_list)
                    stats['total_downloads'] += len(data_list)
                    
                    # Calcular estadísticas adicionales
                    price_count = sum(1 for item in data_list if item.get('price') is not None)
                    market_cap_count = sum(1 for item in data_list if item.get('market_cap') is not None)
                    
                    # Obtener rango de fechas
                    first_timestamp = None
                    last_timestamp = None
                    if data_list:
                        timestamps = [item['timestamp'] for item in data_list]
                        first_timestamp = min(timestamps)
                        last_timestamp = max(timestamps)
                    
                    stats['consolidation_info'][symbol] = {
                        'total_records': len(data_list),
                        'records_with_price': price_count,
                        'records_with_market_cap': market_cap_count,
                        'first_timestamp': first_timestamp,
                        'last_timestamp': last_timestamp,
                        'last_processed_date': crypto_data.get('last_processed_date', 'Unknown'),
                        'last_updated': crypto_data.get('last_updated', 'Unknown')
                    }
                    
                except Exception as e:
                    logger.warning(f"Error leyendo estadísticas de {symbol}: {e}")
        
        return stats
    
    def close(self):
        """Cierra el driver"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")


def main():
    """Función principal"""
    scraper = None
    
    try:
        print("=== Descargador de Datos de Criptomonedas en Rangos de 5 Días ===")
        print("Formato: [{timestamp, price, market_cap}, ...] con tracking de progreso")
        print("Instalando dependencias:")
        print("pip install selenium webdriver-manager")
        print("ChromeDriver se descarga automáticamente\n")
        
        # Configuración
        headless = input("¿Ejecutar en modo headless? (s/N): ").lower().startswith('s')
        delay = float(input("Delay entre peticiones en segundos (recomendado: 2-4): ") or "2.5")
        
        # Crear scraper
        scraper = SeleniumRangeCryptoDataScraper(
            crypto_file="criptomonedas.json",
            values_dir="values",
            daily_dir="daily_data",
            delay=delay,
            headless=headless
        )
        
        # Mostrar estadísticas existentes
        stats = scraper.get_download_statistics()
        if stats['total_downloads'] > 0:
            print(f"📊 Estado actual de los datos:")
            print(f"   📁 Archivos consolidados: {stats['total_crypto_files']}")
            print(f"   📈 Total de registros: {stats['total_downloads']}")
            print(f"   🪙 Criptomonedas con datos: {len(stats['by_symbol'])}")
            
            # Mostrar información detallada de progreso
            print(f"\n📅 Última fecha procesada por criptomoneda:")
            for symbol, info in list(stats['consolidation_info'].items())[:5]:
                last_date = info['last_processed_date'] or 'Ninguna'
                print(f"   • {symbol}: {last_date} ({info['total_records']} registros)")
            if len(stats['consolidation_info']) > 5:
                print(f"   ... y {len(stats['consolidation_info']) - 5} más")
            print()
        
        # Ejecutar
        scraper.run()
        
    except KeyboardInterrupt:
        print("\n🛑 Proceso interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error en main: {e}")
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    main()