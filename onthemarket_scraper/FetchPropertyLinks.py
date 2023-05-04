import asyncio
import json
import os
import re
import sys
from typing import Union

import tenacity

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

import numpy as np
from aiohttp import ClientSession, ClientTimeout, TCPConnector

from onthemarket_scraper.onthemarket_utils import get_headers
from utils.add_utils import fetch, Logger


logger = Logger('otm_fetchlinks_log', 'otm_fetchlinks.log', currentdir).get_logger()



async def fetch_all_property_urls(zipcode_list: list[str]) -> list[str]:
    """
    Fetches property URLs for a list of zip codes using asyncio.
    Returns a list of unique property URLs.

    Args:
        zipcode_list (List[str]): A list of zip codes to search for.

    Returns:
        List[str]: A list of unique property URLs.
    """
    NUM_REQ=30
    async with ClientSession(
        timeout=ClientTimeout(total=60), 
        connector=TCPConnector(limit=NUM_REQ)
    ) as session:
        
        # create a semaphore to limit the number of concurrent requests
        sem = asyncio.Semaphore(NUM_REQ)
        
        async with sem:
            tasks = (asyncio.ensure_future(get_zc_property_urls(session=session, zipcode=zc)) for zc in zipcode_list)

        property_links_lists = await asyncio.gather(*tasks)
        property_links = [url for url_list in property_links_lists if len(url_list)!=0 for url in url_list]
        return np.unique(property_links)
    


async def get_zc_property_urls(session: ClientSession, zipcode: str) -> list[Union[str, None]]:
    """
    Retrieves property URLs for a given zip code using the given session.
    Returns a list of URLs or None.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        zipcode (str): The zip code to search for.

    Returns:
        List[Union[str, None]]: A list of URLs or None.
    """
    all_urls=[]
    for search_type in ["for-sale", "to-rent"]:
        page_num=0
        property_urls=[]
        while True:
            search_url=f"https://www.onthemarket.com/{search_type}/property/{zipcode}/?page={page_num}"\
                        "&recently-added=24-hours&view=grid"
            
            try:
                resp = await fetch(session=session, url=search_url, headers=get_headers())
                rows = json.loads(re.search('__OTM__\.jsonData = ({.+})', resp).group(1))['properties']
        
                # If the number of rows is not zero, that means there are search results. 
                if len(rows)!=0:
                    
                    #If there is only one search result and there is no "property-link" key, then its an ad, which we dont need.
                    if len(rows)==1 and "property-link" not in rows: pass
                    else:
                        for row in rows:
                            try: property_urls.append("https://www.onthemarket.com" + row["property-link"])
                            
                            except KeyError as e: 
                                logger.error(type(e).__name__, search_url)
                                pass

                    page_num+=1

                all_urls.extend(property_urls)

            except tenacity.RetryError as e:
                logger.error("RetryError", search_url)

            break

    return all_urls
