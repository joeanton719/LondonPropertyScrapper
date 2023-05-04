
import logging
import os

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.logger import __logger as wdm_logger

wdm_logger.setLevel(logging.WARNING)

os.environ['WDM_LOG'] = '0'
os.environ['WDM_LOG_LEVEL'] = "false"
os.environ['WDM_PRINT_FIRST_LINE'] = 'False'
logging.getLogger('WDM').setLevel(logging.NOTSET)
logging.getLogger('selenium').setLevel(logging.WARNING)


def initialize_driver() -> webdriver.Chrome:
    """
    Initializes a Chrome webdriver with specific options.

    Returns:
        webdriver.Chrome: The Chrome webdriver with options set.
    """

    # Configure ChromeOptions
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')  # Run Chrome in headless mode
    options.add_argument('--blink-settings=imagesEnabled=false')  # Disable loading images
    options.add_argument("--start-maximized")  # Start Chrome in maximized mode
    options.page_load_strategy = 'eager'  # Set page load strategy
    options.add_argument('window-size=2000x1500')  # Set window size
    options.add_argument('--log-level=3')  # Set log level
    options.add_argument("--silent")  # Run Chrome silently
    options.add_argument('--disable-gpu')  # Disable GPU acceleration
    options.add_experimental_option("excludeSwitches", ["enable-automation", 'enable-logging'])  # Exclude switches
    options.add_experimental_option('useAutomationExtension', False)  # Disable automation extension
    options.add_argument('--ignore-certificate-errors')  # Ignore certificate errors
    options.add_argument('--ignore-ssl-errors')  # Ignore SSL errors

    # Initialize Chrome webdriver with configured options
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    return driver



def get_url(driver: webdriver.Chrome, long_url: str) -> webdriver.Chrome:
    """
    Loads a URL in a webdriver and waits for specific elements to appear before continuing.

    Args:
        driver (webdriver.Chrome): The Chrome webdriver to use.
        long_url (str): The URL to load.

    Returns:
        webdriver.Chrome: The Chrome webdriver after the URL has been loaded and elements have appeared.
    """

    # Load the URL in the Chrome webdriver
    driver.get(long_url)
    
    # When going to the 2nd page and forward, an ad pop-up appears. 
    # The following step is to close the pop if encountered.
    # Wait for one of two specific elements to become visible
    WebDriverWait(driver, 120).until(EC.any_of(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "iframe[title='Usabilla Feedback Button']")),
        EC.visibility_of_element_located((By.CSS_SELECTOR, "div[aria-label='Email alerts sign-up']"))
    ))
    
     # Try to find and click the "Close dialog" button, if it exists
    try:
        driver.find_element(by="css selector", value="button[aria-label='Close dialog']").click()
    except NoSuchElementException:
        # If the "Close dialog" button doesn't exist, just continue
        pass

    return driver