import json
import os
import re
import sys
from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from zoopla_scraper.zoopla_utils import get_url, initialize_driver

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from utils.add_utils import Logger

log_dict = {
    "for-sale" : Logger('zoopla_SaleLog', 'sale.log', currentdir, False).get_logger(),
    "to-rent" : Logger('zoopla_RentLog', 'rent.log', currentdir, False).get_logger()
    }


def get_data(for_sale: bool=True) -> list[dict[str, Union[str, int, float, datetime]]]:
    """
    Scrapes Zoopla for property data and returns a list of dictionaries containing property information.
    :param for_sale: A boolean indicating whether to scrape properties for sale (True) or for rent (False).
    :return: A list of dictionaries containing property information.
    """

    # Initialize variables for storing scraped data and the page number
    full_list = []
    page_num=1
    
    task="for-sale" if for_sale else "to-rent"
    
    # Loop through pages of search results until no more properties are found
    while True:

        # Construct the URL for the current page of search results and open it using a Selenium driver
        url=f"https://www.zoopla.co.uk/{task}/property/london/?q=London&search_source=home&chain_free"\
            f"=&added=24_hours&pn={page_num}"

        driver=get_url(driver=initialize_driver(), long_url=url)
        
        # Display the total number of results on the first page of search results
        if page_num==1: show_results(driver=driver, task=task)
        
        # Extract the list of properties from the current page of search results and close the driver
        try:
            property_lists = get_lists(driver=driver)
            driver.close()

            # If properties were found, parse the data and add it to the full list
            if len(property_lists)!=0:
                full_list.extend(parse_data(property_lists=property_lists, for_sale=for_sale))
                log_dict[task].info(f"Scraped {task} Page #{page_num}")
                page_num+=1
            
            # If no more properties were found, log the completion message and return the full list
            else:
                log_dict[task].info("Completed\n")
                driver.quit()
                return full_list

        except KeyError:
            log_dict[task].error(f"KeyError: {url}")
            log_dict[task].info("Completed with no results\n")
            driver.quit()
            return full_list
        
        except Exception as e:
            log_dict[task].error(f"{type(e).__name__}: {e} - {url}\n")
            print(f"{type(e).__name__}: {e} - {url}")
            driver.quit()
        



def show_results(driver: webdriver.Chrome, task: str) -> None:
    """
    Displays the total number of results found for a given task and logs the information using the appropriate logger.
    """

    # Find the total number of results using a CSS selector and extract the text
    results = driver.find_element(by="css selector", value="p[data-testid='total-results']").text

    # Log the total number of results using the appropriate logger
    log_dict[task].info(f"Total {task}: {results}")



def get_lists(driver: webdriver.Chrome) -> list:
    """
    Extracts a list of properties from a webpage using a Selenium driver and returns it.
    :param driver: A Selenium webdriver object for accessing the webpage.
    :return: A list of dictionaries containing property information.
    """

    # Extract the webpage source using BeautifulSoup and locate the script section containing property data
    soup = BeautifulSoup(driver.page_source, "lxml")
    script_section = str(soup.select("script#__NEXT_DATA__")[0])

    # Use regex to extract the JSON data containing property information
    pattern=r"^<script id=\"__NEXT_DATA__\" type=\"application/json\">({.+})</script>$"
    extracted_data=re.search(pattern, script_section).group(1)

    # Load the JSON data and extract the list of properties
    property_lists = json.loads(extracted_data)['props']['pageProps']['regularListingsFormatted']
    
    return property_lists



def parse_data(
    property_lists: list, 
    for_sale: bool=True
    ) -> list[dict[str, Union[str, int, float, datetime]]]:
    """
    Parses a list of property listings and returns a list of dictionaries containing relevant information about each property.

    Args:
        property_lists (list): List of property listings returned from the Zoopla API.
        for_sale (bool): Indicates whether the properties are for sale or for rent. Defaults to True.

    Returns:
        list: List of dictionaries containing relevant information about each property.
    """
    lists = []
    
    for row in property_lists:
        # Extract relevant information from the row and add it to the parsed_data list
        lists.append({
            "property_id" : row['listingId'],
            "transactionType" : "buy" if for_sale else "rent",
            "bedrooms" : total_bad_and_bath(dict_key=row['features'])["bed"],
            "bathrooms" : total_bad_and_bath(dict_key=row['features'])["bath"],
            "description" : row['summaryDescription'],
            "propertySubType" : row['propertyType'],
            "featuredProperty" : row['featuredType'] is not None,
            "price" : parse_price(row=row['price'], for_sale=for_sale)["price"],
            "price_currency" : parse_price(row=row['price'], for_sale=for_sale)["price_currency"],
            "rent_freq" : parse_price(row=row['price'], for_sale=for_sale)["rent_freq"],
            "displayAddress" : row['address'],
            "latitude" : row['location']['coordinates']['latitude'],
            "longitude" : row['location']['coordinates']['longitude'],
            'agent' : row['branch']['name'],
            "listing_url" : "https://www.zoopla.co.uk/" + row['listingUris']['detail'],
            'listing_source' : "zoopla",
            'firstVisibleDate' : pd.to_datetime(row['publishedOn']),
            'commercial': np.nan,
            'development': np.nan,
            'residential': np.nan,
            'students': np.nan,
            'displaySize' : np.nan,
            "short_desc" : row['title']
        })
        
    return lists


def total_bad_and_bath(dict_key: dict) -> dict[str, Union[int, float]]:
    """
    Extracts the total number of bedrooms and bathrooms from a dictionary of property data.

    Args:
        dict_key (Dict[str, Union[str, float]]): A dictionary containing property data.

    Returns:
        Dict[str, Union[int, float]]: A dictionary with keys "bed" and "bath" and the corresponding
        number of bedrooms and bathrooms as values. If the dictionary does not contain information
        about the number of bedrooms or bathrooms, the corresponding value in the returned dictionary
        is NaN.
    """
    try:
        bed = [d for d in dict_key if d['iconId'] == 'bed'][0]["content"]
        bath = [d for d in dict_key if d['iconId'] == 'bath'][0]["content"]
        return {"bed": bed, "bath": bath}
    
    except IndexError:
        return {"bed": np.nan, "bath": np.nan}
    

def parse_price(row: str, for_sale: bool) -> dict[str, Union[str, float]]:
    
    try:
        price=float(re.sub(r"[^\d\.]", "", row))
        price_currency=row[0]
        rent_freq=np.nan if for_sale else row['price'].split()[-1]

    except ValueError:
        price=np.nan
        price_currency=np.nan
        rent_freq=np.nan

    return {"price":price, "price_currency":price_currency, "rent_freq":rent_freq}