import concurrent.futures
import os

from utils.add_utils import Logger, time_it
from zoopla_scraper.FetchData import get_data

currentdir = os.path.dirname(os.path.realpath(__file__))
logger = Logger('zoopla_logger', 'main.log', currentdir).get_logger()


@time_it(logger)
def scrape_from_zoopla() -> list[dict]:
    
    logger.info(f"Started Scraping from Zoopla")

    # Use concurrent.futures to run both functions simultaneously
    with concurrent.futures.ThreadPoolExecutor() as executor:
        sales_task = executor.submit(get_data, True)
        rent_task = executor.submit(get_data, False)

    # Get the property lists separately
    sales_property_list = sales_task.result()
    rent_property_list = rent_task.result()
    
    full_list = sales_property_list + rent_property_list

    return full_list