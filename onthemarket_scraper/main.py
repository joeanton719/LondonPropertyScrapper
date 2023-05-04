import os

from onthemarket_scraper.FetchPropertyData import fetch_all_property_data
from onthemarket_scraper.FetchPropertyLinks import fetch_all_property_urls
from onthemarket_scraper.onthemarket_utils import get_zipcodes

currentdir = os.path.dirname(os.path.realpath(__file__))

from utils.add_utils import Logger, time_it

logger = Logger('otm_main_log', 'otm_main.log', currentdir).get_logger()

@time_it(logger)
async def scrape_from_otm() -> list[dict]:

    logger.info("Started Scraping from OnTheMarket")

    # Get list of zipcodes
    subset=get_zipcodes()

    # Get all property listings urls
    property_urls = await fetch_all_property_urls(zipcode_list=subset)
    logger.info(f"Scraped total {len(property_urls)} links")

    # Scrape all Property Data from the Property Links
    property_lists = await fetch_all_property_data(property_urls=property_urls)
    
    logger.info(f"Total rows: {len(property_lists)}")

    return property_lists