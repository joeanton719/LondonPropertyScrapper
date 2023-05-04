import asyncio
import json
import os
import re
import sys
from typing import Union

import numpy as np
import pandas as pd
import tenacity
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from bs4 import BeautifulSoup

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from rightmove_scraper.rightmove_utils import get_headers
from utils.add_utils import Logger, fetch

logger = Logger('rm_fetchData_log', 'rm_fetchData.log', currentdir).get_logger()


async def fetch_all_property_data(outcode_list: list[str]) -> list[dict[str, Union[int, float, str, bool, None]]]:
    """
    Fetches property data for a list of outcodes using async requests.

    Args:
        outcode_list: A list of strings representing outcodes to fetch property data for.

    Returns:
        A list of dictionaries representing property data.
    """
    NUM_REQ=30

    async with ClientSession(
        timeout=ClientTimeout(total=60), 
        connector=TCPConnector(limit=NUM_REQ, keepalive_timeout=30)
    ) as session:
        
        # create a semaphore to limit the number of concurrent requests
        sem = asyncio.Semaphore(NUM_REQ)
        async with sem:
            tasks = (asyncio.ensure_future(get_property_data(session=session, outcode=outcode)) for outcode in outcode_list)

        property_data_list = await asyncio.gather(*tasks)

        # Merge the list of property data dictionaries into a single list
        property_data = [data for data_list in property_data_list if len(data_list)!=0 for data in data_list]
        
        return property_data


async def get_property_data(
    session: ClientSession, 
    outcode: str
    ) -> list[dict[str, Union[int, float, str, bool, None]]]:
    """
    Retrieves property data for a given UK postcode using Rightmove's search functionality.

    Args:
        session (aiohttp.ClientSession): An aiohttp ClientSession object.
        outcode (str): The postcode outcode to search.

    Returns:
        A list of property data as dictionaries, with each dictionary representing a single property.

    Raises:
        None.
    """
    property_list=[]

    # loop through search types ('property-for-sale' and 'property-to-rent')
    for search_type in ["property-for-sale", "property-to-rent"]:

        IDX=24
        page_num=0

        # set initial last_page value to a large number (42 pages * 24 results per page)
        last_page=42*IDX

        search_url=f"https://www.rightmove.co.uk/{search_type}/find.html?"\
                   f"locationIdentifier={outcode.replace('^', '%5E')}&index={page_num}&maxDaysSinceAdded=1"

        try:
            # loop through pages of search results (24 results per page)
            while page_num<=last_page:

                # build search URL with specified query parameters
                url=f"https://www.rightmove.co.uk/{search_type}/find.html?"
                params = {"locationIdentifier" : outcode, "index" : str(page_num), "maxDaysSinceAdded" : "1"}

                # fetch search results with async HTTP GET request
                resp = await fetch(headers=get_headers(), session=session, url=url, params=params)
                
                # get maximum page number from first search results page
                if page_num==0: last_page = get_max_page(resp=resp)

                # if no search results were found, break out of the loop
                if last_page==0: break
                else:
                    # extract property data from search results page and append to list
                    property_list.extend(parse_data(resp=resp))
                    # increment page number by 24 for next search results page
                    page_num+=IDX

        except asyncio.exceptions.TimeoutError:
            logger.error("TimeoutError", search_url)

        except tenacity.RetryError:
            logger.error("RetryError", search_url)

    # return list of extracted property data
    return property_list


def get_max_page(resp: str) -> int: 
    """
    Get the maximum number of pages for a given search result.

    Parameters:
    -----------
    resp: str
        The html content of the search result page.

    Returns:
    --------
    int:
        The maximum number of pages.

    Raises:
    -------
    ValueError:
        If no search result is found.
    """

    # the page numbers in rightmove.com increments by 24, i.e: page 0, page 24, page 48, etc.
    IDX=24
    soup = BeautifulSoup(resp, "lxml")
    count_elem = soup.select("span.searchHeader-resultCount")

    if not count_elem: raise ValueError("No search result found.")
    
    count = count_elem[0].text

    # extract only the number as an integer
    result_count = int(count.replace(",", ""))

    # Calculate max number of pages of search results to be scraped, based on the total number of search results.
    # If more than 23 results, divide the result count by IDX and round down to the nearest integer to get the last page number.
    # Otherwise, divide the result count by IDX and round up to the nearest integer to get the last page number.
    last_num = int(np.floor(result_count/IDX)) if result_count>23 else int(np.ceil(result_count/IDX))

    # Rightmove limits the number of search results to 1000, which is equivalent to 42 pages of 24 results per page
    # Calculate the maximum number of search results to be scraped, based on the maximum number of pages of search results.
    # If the maximum number of pages is greater than or equal to 42, multiply IDX by 42 to get the maximum number of results to scrape.
    # Otherwise, multiply IDX by the maximum number of pages to get the maximum number of results to scrape.
    return 42*IDX if last_num>=42 else last_num*IDX


def parse_data(resp: str) -> list[dict[str, Union[int, float, str, bool, None]]]:
    """
    Parse the property data from a Rightmove search results page.

    Parameters:
    -----------
    resp : str
        The HTML response of a Rightmove search results page.

    Returns:
    --------
    A list of dictionaries, where each dictionary represents a property and contains the following keys:
    - property_id (int): The unique identifier of the property.
    - transactionType (str): The transaction type of the property (e.g., "sale" or "rent").
    - bedrooms (int): The number of bedrooms in the property.
    - bathrooms (int): The number of bathrooms in the property.
    - description (str): A summary description of the property.
    - propertySubType (str): The subtype of the property (e.g., "detached" or "semi-detached").
    - featuredProperty (bool): Whether the property is featured on Rightmove.
    - price (float): The price of the property.
    - price_currency (str): The currency of the price (e.g., "GBP").
    - rent_freq (str): The frequency of rent payments (if transactionType is "rent", otherwise NaN).
    - displayAddress (str): The display address of the property.
    - latitude (float): The latitude of the property location.
    - longitude (float): The longitude of the property location.
    - agent (str): The name of the agent marketing the property.
    - listing_url (str): The URL of the property listing on Rightmove.
    - listing_source (str): The source of the property listing ("rightmove").
    - firstVisibleDate (pandas.Timestamp): The date when the property was first listed on Rightmove.
    - commercial (bool): Whether the property is commercial.
    - development (bool): Whether the property is a development.
    - residential (bool): Whether the property is residential.
    - students (bool): Whether the property is suitable for students.
    - featuredProperty (bool): Whether the property is featured on Rightmove.
    - displaySize (str): The display size of the property.
    - short_desc (str): A short description of the property type.
    """

    # Extract the JSON object containing property data
    json_obj = json.loads(re.search(r'window\.jsonModel = ({.+})', resp).group(1))['properties']

    # Initialize an empty list to store property data
    property_lists=[]
    
    # Iterate over each property in the JSON object and extract relevant data
    for row in json_obj:
        
        transactionType = row['transactionType']
        freq = row['price']['frequency'] if transactionType=='rent' else np.nan
            
        property_lists.append({
            "property_id" : row['id'],
            "transactionType" : transactionType,
            "bedrooms" : row['bedrooms'],
            'bathrooms' : row['bathrooms'],
            'description' : row['summary'],
            'propertySubType' : row['propertySubType'],
            'featuredProperty' : row['featuredProperty'],
            'price' : float(row['price']['amount']), 
            "price_currency" : row['price']['currencyCode'],
            'rent_freq' : freq,
            'displayAddress' : row['displayAddress'],
            'latitude' : row['location']['latitude'],
            'longitude' : row['location']['longitude'],
            'agent' : row['customer']['brandTradingName'],
            'listing_url' : f"https://www.rightmove.co.uk{row['propertyUrl']}",
            "listing_source" : "rightmove",
            "firstVisibleDate" : pd.to_datetime(row['firstVisibleDate']),
            "commercial" : row['commercial'],
            "development" : row['development'],
            "residential" : row['residential'],
            "students" : row['students'],
            "featuredProperty" : row['featuredProperty'],
            "displaySize" : row['displaySize'],
            "short_desc" : row['propertyTypeFullDescription']
        })
        
    return property_lists