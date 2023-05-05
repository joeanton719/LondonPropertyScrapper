import asyncio
import logging
import os
import time
from functools import wraps
from logging.handlers import RotatingFileHandler
from aiohttp.client_exceptions import ServerDisconnectedError

import tenacity
from aiohttp import ClientSession
from tenacity import wait_fixed, retry_if_exception_type, stop_after_attempt


class Logger:
    def __init__(
            self, 
            logger_name: str, 
            log_file: str, 
            log_directory: str = ".",
            need_console_log: bool=True            
            ) -> None:
        """
        Creates a new logger object with a specified name and log file.

        Args:
            logger_name (str): The name of the logger.
            log_file (str): The name of the log file.
            log_directory (str): The directory where the log file should be saved. Defaults to current directory.
        """
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        # create a console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        # create a file handler
        log_path = os.path.join(log_directory, "logs", log_file)
        fh = RotatingFileHandler(log_path, maxBytes=1024*1024, backupCount=5)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)

        # add the handlers to the logger
        if need_console_log: self.logger.addHandler(ch)
        else: pass
        
        self.logger.addHandler(fh)

    def get_logger(self) -> logging.Logger:
        """
        Returns the logger object created by this class.

        Returns:
            logging.Logger: The logger object.
        """
        return self.logger
    


# https://tenacity.readthedocs.io/en/latest/
@tenacity.retry(
        retry=retry_if_exception_type((asyncio.exceptions.TimeoutError, ServerDisconnectedError)),
        wait=wait_fixed(5), 
        stop=stop_after_attempt(5)
        )
async def fetch(session: ClientSession, url: str, headers: dict, params=None) -> str: 
    """
    Fetches data from the given URL using the given session.
    Returns the response text as a string.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        url (str): The URL to fetch data from.

    Returns:
        str: The response text as a string.
    """
    async with session.get(url=url, headers=headers, params=params) as response:
        resp = await response.text()
        return resp
    

def time_it(logger: logging.Logger):
    """
    A function to use as a decorator to measure time taken to execute a given function.
    """
    def decorator(func):
        
        @wraps(func)
        async def wrapper_async(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            
            if elapsed_time < 60: logger.info(f"{func.__name__} took {elapsed_time:.2f} seconds to run.")
            else: logger.info(f"{func.__name__} took {elapsed_time/60:.2f} minutes to run.")
            
            return result

        @wraps(func)
        def wrapper_sync(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            
            if elapsed_time < 60: logger.info(f"{func.__name__} took {elapsed_time:.2f} seconds to run.")
            else: logger.info(f"{func.__name__} took {elapsed_time/60:.2f} minutes to run.")
            
            return result

        return wrapper_async if asyncio.iscoroutinefunction(func) else wrapper_sync

    return decorator
