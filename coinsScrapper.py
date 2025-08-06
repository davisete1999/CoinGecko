from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import logging
import random

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SeleniumCoinGeckoScrapper:
    def __init__(self, headless=True, delay=2):
        self.base_url = "https://www.coingecko.com"
        self.delay = delay
        self.driver = None
        self.setup_driver(headless)
    
    def setup_driver(self, headless=True):
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
            logger.info("Driver de Chrome configurado correctamente")
        except Exception as e:
            logger.error(f"Error al configurar Chrome driver: {e}")
            raise
    
    def random_delay(self, min_delay=1, max_delay=3):
        """
        Implementa un delay aleatorio entre requests
        """
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def scroll_page(self):
        """
        Hace scroll para cargar contenido dinámico
        """
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
            logger.warning(f"Error durante scroll: {e}")
    
    def get_page(self, url, wait_time=10):
        """
        Navega a una URL y espera a que cargue
        """
        try:
            logger.info(f"Navegando a: {url}")
            self.driver.get(url)
            
            # Esperar a que la página cargue
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            # Hacer scroll para cargar contenido
            self.scroll_page()
            
            return True
            
        except TimeoutException:
            logger.error(f"Timeout esperando que cargue la página: {url}")
            return False
        except Exception as e:
            logger.error(f"Error al cargar página {url}: {e}")
            return False
    
    def extract_coins_from_page(self):
        """
        Extrae datos de criptomonedas de la página actual
        """
        coins_data = []
        
        try:
            # Buscar la tabla
            table = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            # Buscar todas las filas de datos
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            logger.info(f"Encontradas {len(rows)} filas en la tabla")
            
            for i, row in enumerate(rows):
                try:
                    coin_data = self.extract_coin_from_row(row)
                    if coin_data:
                        coins_data.append(coin_data)
                        
                except Exception as e:
                    logger.warning(f"Error extrayendo datos de fila {i}: {e}")
                    continue
            
            logger.info(f"Extraídos datos de {len(coins_data)} criptomonedas")
            return coins_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de la página: {e}")
            return []
    
    def extract_coin_from_row(self, row):
        """
        Extrae datos de una fila específica
        """
        try:
            # Buscar enlace principal (nombre y símbolo)
            link_element = row.find_element(By.CSS_SELECTOR, "td:nth-child(2) a, td:nth-child(3) a")
            coin_url = link_element.get_attribute("href")
            
            # Buscar imagen
            try:
                img_element = link_element.find_element(By.TAG_NAME, "img")
                icon_url = img_element.get_attribute("src")
            except NoSuchElementException:
                icon_url = ""
            
            # Extraer texto del enlace
            link_text = link_element.text.strip()
            
            # Separar nombre y símbolo
            lines = [line.strip() for line in link_text.split('\n') if line.strip()]
            
            name = lines[0] if lines else ""
            symbol = ""
            
            # Buscar símbolo en las líneas siguientes
            for line in lines[1:]:
                if len(line) <= 10 and line.replace('$', '').replace('#', '').replace('@', '').isalnum():
                    symbol = line
                    break
            
            # Si no encontramos símbolo, intentar otro selector
            if not symbol:
                try:
                    symbol_element = link_element.find_element(By.CSS_SELECTOR, "[class*='text-xs'], [class*='symbol']")
                    symbol = symbol_element.text.strip()
                except NoSuchElementException:
                    pass
            
            coin_data = {
                'nombre': name,
                'simbolo': symbol,
                'icono_url': icon_url,
                'enlace': coin_url
            }
            
            return coin_data
            
        except Exception as e:
            logger.warning(f"Error extrayendo datos de fila: {e}")
            return None
    
    def scrape_pages(self, max_pages=5, items_per_page=50):
        """
        Scrapea múltiples páginas
        """
        all_coins = []
        
        logger.info(f"Iniciando scraping de {max_pages} páginas")
        
        for page in range(1, max_pages + 1):
            try:
                url = f"{self.base_url}/es?page={page}&items={items_per_page}"
                
                if self.get_page(url):
                    coins_data = self.extract_coins_from_page()
                    all_coins.extend(coins_data)
                    
                    logger.info(f"Página {page}/{max_pages} completada. Total: {len(all_coins)} monedas")
                    
                    # Delay entre páginas
                    if page < max_pages:
                        self.random_delay(self.delay, self.delay * 2)
                else:
                    logger.error(f"No se pudo cargar la página {page}")
                    
            except Exception as e:
                logger.error(f"Error en página {page}: {e}")
                continue
        
        logger.info(f"Scraping completado. Total: {len(all_coins)} criptomonedas")
        return all_coins
    
    def save_to_csv(self, data, filename='criptomonedas_selenium.csv'):
        """
        Guarda los datos en CSV
        """
        try:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Datos guardados en {filename}")
        except Exception as e:
            logger.error(f"Error guardando CSV: {e}")
    
    def save_to_json(self, data, filename='criptomonedas_selenium.json'):
        """
        Guarda los datos en JSON
        """
        try:
            import json
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Datos guardados en {filename}")
        except Exception as e:
            logger.error(f"Error guardando JSON: {e}")
    
    def close(self):
        """
        Cierra el driver
        """
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")

def main():
    """
    Función principal
    """
    scraper = None
    
    try:
        print("=== CoinGecko Scraper con Selenium ===")
        print("NOTA: Asegúrate de tener ChromeDriver instalado")
        print("Instalación: https://chromedriver.chromium.org/\n")
        
        # Crear scraper
        scraper = SeleniumCoinGeckoScrapper(headless=True, delay=3)
        
        # Scrapear páginas
        max_pages = int(input("¿Cuántas páginas quieres scrapear? (recomendado: 1-5): ") or "1")
        
        coins_data = scraper.scrape_pages(max_pages=max_pages)
        
        if coins_data:
            print(f"\n=== RESULTADOS ===")
            print(f"Total de criptomonedas extraídas: {len(coins_data)}")
            
            # Mostrar muestra
            print(f"\nPrimeras 5 criptomonedas:")
            for i, coin in enumerate(coins_data[:5]):
                print(f"{i+1}. {coin['nombre']} ({coin['simbolo']})")
                print(f"   URL: {coin['enlace']}")
            
            # Guardar datos
            scraper.save_to_csv(coins_data)
            scraper.save_to_json(coins_data)
            
        else:
            print("No se pudieron extraer datos")
            
    except Exception as e:
        logger.error(f"Error en main: {e}")
        
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()