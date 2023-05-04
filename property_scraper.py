import asyncio
import concurrent.futures

import pandas as pd

from onthemarket_scraper.main import scrape_from_otm
from rightmove_scraper.main import scrape_from_rightmove
from utils.add_utils import Logger, time_it
from zoopla_scraper.main import scrape_from_zoopla

logger = Logger('propertyScrapper_log', 'propertyScrapper.log').get_logger()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def async_scrape():

    otm_task = asyncio.create_task(scrape_from_otm())
    rm_task = asyncio.create_task(scrape_from_rightmove())
    all_tasks = await asyncio.gather(otm_task, rm_task)
    properties = [prop_dict for lists in all_tasks for prop_dict in lists]

    return properties


@time_it(logger)
async def main():

    logger.info("Started Scraping Latest Property Listings")
    # create a new event loop
    loop = asyncio.get_running_loop()
    # create an executor
    executor = concurrent.futures.ThreadPoolExecutor()
    # run the normal function in a separate thread
    list1 = loop.run_in_executor(executor, scrape_from_zoopla)
    # run the async function in the event loop
    list2 = await async_scrape()
    properties = list1 + list2

    df = pd.DataFrame(properties)

    return df


if __name__=="__main__":
    df = asyncio.run(scrape_from_rightmove())
    print(len(df))
    print()
    # print(df.info())
    # print()
    # print(df.head())