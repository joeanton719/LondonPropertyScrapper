import asyncio
import datetime
import json
import os
import re
import sys

import tenacity

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from typing import Any, Union

import numpy as np
from aiohttp import ClientSession, ClientTimeout, TCPConnector

from onthemarket_scraper.onthemarket_utils import get_headers
from utils.add_utils import Logger, fetch


logger = Logger('otm_fetchdata_log', 'otm_fetchdata.log', currentdir).get_logger()



async def fetch_all_property_data(property_urls: list[str]) -> list[dict[str, Union[str, int, float, bool, None]]]:
    """
    Fetches property data for a list of property URLs asynchronously and returns the data as a Pandas DataFrame.

    Args:
    - property_urls (list[str]): A list of property URLs to fetch data for.

    Returns:
    - (list[dict]): A list of dictionaries containing property data for each URL in the input list.
    """    
    NUM_REQ=30
    async with ClientSession(
        timeout=ClientTimeout(total=60), 
        connector=TCPConnector(limit=NUM_REQ)
    ) as session:
        
        # create a semaphore to limit the number of concurrent requests
        sem = asyncio.Semaphore(NUM_REQ)
        
        async with sem:
            tasks = (asyncio.ensure_future(get_property_data(session=session, property_url=prop_url)) for prop_url in property_urls)

        property_data = await asyncio.gather(*tasks)
        return property_data



async def get_property_data(session: ClientSession, property_url: str) -> dict[str, Union[str, int, float, bool, None]]:
    """
    Retrieves the property data from a given property URL.

    Args:
    session (ClientSession): An aiohttp ClientSession object to make HTTP requests.
    property_url (str): The URL of the property to scrape.

    Returns:
    dict: A dictionary containing the scraped property data. If an error occurs while scraping, an empty dictionary is returned.
    """
    try:
        resp = await fetch(session=session, url=property_url, headers=get_headers())
        property_data_dict = parse_property_data(resp=resp)
        return property_data_dict
    
    except tenacity.RetryError as e:
        logger.error("RetryError", property_url)



def parse_property_data(resp: str) -> dict[str, Union[str, int, float, bool, None]]:
    """
    Parses the HTML response of a property page on OnTheMarket website and returns a dictionary of relevant information.

    Parameters:
    -----------
    resp : str
        The HTML response of a property page on OnTheMarket website.

    Returns:
    --------
    dict
        A dictionary containing the following keys and values:
        - 'property_id' : str
            The unique identifier of the property.
        - 'transactionType' : str
            Either 'buy' or 'rent', depending on whether the property is for sale or for rent.
        - 'bedrooms' : int
            The number of bedrooms in the property.
        - 'bathrooms' : int
            The number of bathrooms in the property.
        - 'description' : str
            A cleaned-up version of the HTML description of the property, with the list of features appended at the end.
        - 'propertySubType' : str
            The humanised property type (e.g. 'Detached house', 'Terraced house', 'Flat', etc.).
        - 'featuredProperty' : nan
            Always NaN, as this information is not available in the OnTheMarket website.
        - 'price' : float
            The asking price of the property, in pounds.
        - 'price_currency' : str
            The currency symbol used for the price (always '£' for OnTheMarket).
        - 'rent_freq' : str or nan
            If the property is for rent, the frequency of the rent payments (e.g. 'pcm' for 'per calendar month').
            Otherwise, NaN.
        - 'displayAddress' : str
            The human-readable address of the property.
        - 'latitude' : float
            The latitude coordinate of the property.
        - 'longitude' : float
            The longitude coordinate of the property.
        - 'agent' : str
            The name of the estate agent selling/renting the property.
        - 'listing_url' : str
            The URL of the property listing on the OnTheMarket website.
        - 'listing_source' : str
            Always 'OnTheMarket', as this function only works for this website.
        - 'firstVisibleDate' : str
            The current date, in YYYY-MM-DD format.
        - 'commercial' : bool
            True if the property is a commercial property, False otherwise.
        - 'development' : bool
            True if the property is a development property, False otherwise.
        - 'residential' : nan
            Always NaN, as this information is not available in the OnTheMarket website.
        - 'students' : bool
            True if the property is suitable for students, False otherwise.
        - 'displaySize' : str or nan
            If available, the minimum size of the property in square meters.
            Otherwise, NaN.
        - 'short_desc' : str
            A short version of the HTML title of the property.
    """
    json_data = json.loads(re.search('__OTM__\.jsonData = ({.+})', resp).group(1))
    property_data = {
        'property_id' : json_data['id'],
        'transactionType' : "buy" if json_data['for-sale?'] else "rent",
        'bedrooms' : json_data['bedrooms'],
        'bathrooms' : json_data['bathrooms'],
        'description' : parse_description(json_data=json_data),
        'propertySubType' : json_data['humanised-property-type'],
        'featuredProperty' : np.nan,
        'price' : parse_price(json_data)['price'],
        'price_currency' : parse_price(json_data)['price_currency'],
        'rent_freq' : parse_price(json_data)['rent_freq'],
        'displayAddress' : json_data['display_address'],
        'latitude' : json_data['location']['lat'],
        'longitude' : json_data['location']['lon'],
        'agent' : json_data['agent']['company_name'],
        'listing_url' : json_data['canonical-url'],
        'listing_source' : 'OnTheMarket',
        'firstVisibleDate' : datetime.datetime.today().strftime('%Y-%m-%d'),
        'commercial' :  json_data['commercial?'],
        'development' :  json_data['development-property?'],
        'residential' : np.nan,
        'students' : json_data['student?'],
        'displaySize' :  parse_displaySize(json_data),
        'short_desc' : json_data['property-title']
    }
    return property_data



def parse_displaySize(json_data: dict[str, Any]) -> Union[str, None]:
    """
    Parses the floor size of the property from a JSON object.

    Args:
        json_data (dict[str, Any]): The JSON object to parse.

    Returns:
        Union[str, None]: The floor size as a string if it is present in the JSON object, else None.
    """
    try: return json_data['minimum-area']
    except KeyError: return np.nan



def parse_description(json_data: dict[str, Any]) -> str:
    """
    Parses the description and features from the JSON data of a property.

    Args:
        json_data: The JSON data of a property.

    Returns:
        A string that contains the parsed description and features.

    """
    desc = re.sub(r'<.*?>', ' ', json_data['description'])
    features = ", ".join([d['feature'] for d in json_data['features']])
    full_desc = desc + " " + features
    clean_full_desc = re.sub(r'\s+', ' ', full_desc.strip())
    
    return clean_full_desc



def parse_price(json_data: dict[str, Any]) -> dict[str, Union[float, str, None]]:
    """
    Parses price data from a JSON dictionary.

    Args:
    - json_data: A JSON dictionary containing price data.

    Returns:
    - A dictionary containing the parsed price data, with keys 'price', 'price_currency' and 'rent_freq'.
    """
    text=json_data['price']
    price_text = text if json_data['for-sale?'] else text.split()[0]

    price_dict = {
            'price' : float(re.sub('[^\d\.]', '', price_text)),
            'price_currency' : text[0],
            'rent_freq' : np.nan if json_data['for-sale?'] else text.split()[1]
        }
    
    return price_dict