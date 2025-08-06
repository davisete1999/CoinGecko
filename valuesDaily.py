#!/usr/bin/env python3
"""
Script para obtener datos de precios y capitalización de mercado de criptomonedas desde CoinGecko usando Selenium
"""

import json
import os
import time
import random
import logging
from urllib.parse import urlparse
from typing import Dict, List, Optional

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

class SeleniumCryptoDataScraper:
    def __init__(self, crypto_file: str = "criptomonedas.json", output_dir: str = "values", headless: bool = True, delay: float = 2.0):
        self.crypto_file = crypto_file
        self.output_dir = output_dir
        self.delay = delay
        self.driver = None
        self.setup_driver(headless)
        
        # Crear directorio de salida si no existe
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
            logger.info("Driver de Chrome configurado correctamente")
        except Exception as e:
            logger.error(f"Error al configurar Chrome driver: {e}")
            logger.error("Intenta instalar/actualizar: pip install webdriver-manager")
            raise
    
    def random_delay(self, min_delay: float = 1.0, max_delay: float = 3.0):
        """
        Implementa un delay aleatorio entre requests
        """
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def load_cryptocurrencies(self) -> List[Dict]:
        """Carga la lista de criptomonedas desde el archivo JSON"""
        try:
            with open(self.crypto_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"No se encontró el archivo {self.crypto_file}")
            return []
        except json.JSONDecodeError:
            logger.error(f"El archivo {self.crypto_file} no tiene un formato JSON válido")
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
            logger.info(f"Obteniendo {data_type} para {crypto_name} desde: {url}")
            
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
                    logger.info(f"✓ Obtenidos {len(stats)} puntos de datos para {crypto_name} ({data_type})")
                    return stats
                except json.JSONDecodeError as e:
                    logger.error(f"Error al parsear JSON para {crypto_name} ({data_type}): {e}")
                    return None
            else:
                logger.warning(f"Respuesta vacía para {crypto_name} ({data_type})")
                return None
                
        except TimeoutException:
            logger.error(f"Timeout al cargar datos para {crypto_name} ({data_type})")
            return None
        except WebDriverException as e:
            logger.error(f"Error de WebDriver para {crypto_name} ({data_type}): {e}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado para {crypto_name} ({data_type}): {e}")
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
        market_cap_dict = {int(item[0]): item[1] for item in market_cap_data} if market_cap_data else {}
        
        for price_item in price_data:
            if len(price_item) >= 2:
                timestamp = int(price_item[0])
                price = price_item[1]
                market_cap = market_cap_dict.get(timestamp)
                
                combined_data.append({
                    'timestamp': timestamp,
                    'price': price,
                    'market_cap': market_cap
                })
        
        return combined_data
    
    def save_crypto_data(self, symbol: str, combined_data: List[Dict]) -> bool:
        """Guarda los datos combinados en un archivo JSON"""
        filename = f"{symbol}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({
                    'symbol': symbol,
                    'data': combined_data,
                    'total_records': len(combined_data)
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✓ Datos guardados para {symbol} ({len(combined_data)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"Error al guardar datos para {symbol}: {e}")
            return False
    
    def process_cryptocurrency(self, crypto: Dict) -> bool:
        """Procesa una criptomoneda individual"""
        symbol = crypto.get('simbolo', '').upper()
        enlace = crypto.get('enlace', '')
        nombre = crypto.get('nombre', '')
        
        if not symbol or not enlace:
            logger.warning(f"Datos incompletos para {nombre}")
            return False
        
        logger.info(f"\n📊 Procesando {nombre} ({symbol})")
        
        # Extraer nombre de URL
        url_name = self.extract_url_name(enlace)
        logger.info(f"Nombre URL extraído: {url_name}")
        
        # Obtener datos de precios
        price_data = self.get_crypto_data(url_name, 'price_charts')
        if not price_data:
            logger.error(f"No se pudieron obtener datos de precios para {symbol}")
            return False
        
        # Delay entre peticiones
        self.random_delay(self.delay, self.delay * 1.5)
        
        # Obtener datos de capitalización de mercado
        market_cap_data = self.get_crypto_data(url_name, 'market_cap')
        if not market_cap_data:
            logger.warning(f"No se pudieron obtener datos de capitalización para {symbol}")
            market_cap_data = []
        
        # Combinar datos
        combined_data = self.combine_data(price_data, market_cap_data)
        
        # Guardar datos
        return self.save_crypto_data(symbol, combined_data)
    
    def run(self) -> None:
        """Ejecuta el proceso completo para todas las criptomonedas"""
        cryptocurrencies = self.load_cryptocurrencies()
        
        if not cryptocurrencies:
            logger.error("No se encontraron criptomonedas para procesar")
            return
        
        logger.info(f"🚀 Iniciando descarga de datos para {len(cryptocurrencies)} criptomonedas")
        logger.info(f"📁 Los archivos se guardarán en: {os.path.abspath(self.output_dir)}")
        
        successful = 0
        failed = 0
        
        try:
            for i, crypto in enumerate(cryptocurrencies, 1):
                print(f"\n[{i}/{len(cryptocurrencies)}] ", end="")
                
                try:
                    if self.process_cryptocurrency(crypto):
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"Error procesando {crypto.get('nombre', 'Desconocido')}: {e}")
                    failed += 1
                
                # Pausa entre criptomonedas para evitar rate limiting
                if i < len(cryptocurrencies):
                    self.random_delay(self.delay * 1.5, self.delay * 2.5)
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Proceso interrumpido por el usuario")
        except Exception as e:
            logger.error(f"Error inesperado durante el procesamiento: {e}")
        
        logger.info(f"\n\n📈 Resumen final:")
        logger.info(f"✅ Exitosos: {successful}")
        logger.info(f"❌ Fallidos: {failed}")
        logger.info(f"📁 Archivos guardados en: {os.path.abspath(self.output_dir)}")
    
    def close(self):
        """Cierra el driver"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")


def main():
    """Función principal"""
    scraper = None
    
    try:
        print("=== CoinGecko Data Scraper con Selenium ===")
        print("Instalando dependencias:")
        print("pip install selenium webdriver-manager")
        print("ChromeDriver se descarga automáticamente\n")
        
        # Configuración
        headless = input("¿Ejecutar en modo headless? (s/N): ").lower().startswith('s')
        delay = float(input("Delay entre peticiones en segundos (recomendado: 2-4): ") or "2.5")
        
        # Crear scraper
        scraper = SeleniumCryptoDataScraper(
            crypto_file="criptomonedas.json",
            output_dir="values",
            headless=headless,
            delay=delay
        )
        
        # Ejecutar scraping
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