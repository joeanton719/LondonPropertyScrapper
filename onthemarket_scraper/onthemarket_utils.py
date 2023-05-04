
import pandas as pd
import sys
import os

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

file_name = r"utils\full_outcode_lists.xlsx"
file_path = os.path.join(parentdir, file_name)

def get_zipcodes() -> list[str]:
    """Reads a list of zipcodes from an Excel file and returns a filtered list.

    Returns:
    --------
    list[str]: A list of lowercase strings representing zipcodes.

    """
    # Read Excel file
    outcode_df = pd.read_excel(file_path)

     # Get all unique zipcodes
    all_zipcodes = outcode_df['pincode'].unique()

    # Exclude certain zipcodes - OnTheMarket website does not return any results for these zipcodes
    exclude_list=['EC1P', 'EC3P', 'EC2P', 'UB18', 'E77', 'E98', 'SW99', 'SW95', 'EC4P', 'W1A']
    zipcodes = [zc.lower() for zc in all_zipcodes if zc not in exclude_list]
    return zipcodes


def get_headers() -> dict[str, str]:
    """Returns a dictionary of headers to be used in an HTTP request.

    Returns:
    --------
    dict[str, str]: A dictionary containing headers.

    """
    headers = {
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="112", "Brave";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    return headers