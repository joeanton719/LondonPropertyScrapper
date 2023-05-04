import os

from rightmove_scraper.FetchData import fetch_all_property_data
from rightmove_scraper.rightmove_utils import get_outcode_list
from utils.add_utils import Logger, time_it

currentdir = os.path.dirname(os.path.realpath(__file__))

logger = Logger('rm_main_log', 'rm_main.log', currentdir).get_logger()


@time_it(logger)
async def scrape_from_rightmove() -> list[dict]:

    logger.info("Started Scraping from RightMove")

    # Call the fetch_all_property_data function and pass it the outcode list
    property_lists = await fetch_all_property_data(outcode_list=get_outcode_list())

    # Create a pandas DataFrame from the response data
    logger.info(f"Scraped {len(property_lists)} properties from RightMove.com")

    return property_lists