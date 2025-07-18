import requests
import os
import polars
import logging

logger = logging.getLogger(__name__)

class MockApi:
    def __init__(self):
        self.base_url = "https://my.api.mockaroo.com/"
        self.api_key = os.environ.get("MOCK_API_KEY")

    def build_url(self, endpoint, params: object = None) -> str:
        url = f"{self.base_url}{endpoint}?key={self.api_key}"
        if params:
            for key, value in params.items():
                url += f"&{key}={value}"
        return url
    
    def process_data(self, res: requests.Response) -> polars.DataFrame:
        data = res.json()
        df = polars.DataFrame(data)
        logger.info(df.head())
        return df

    def get(self, endpoint, params: object = None) -> polars.DataFrame:
        url = self.build_url(endpoint, params)
        try:
            logger.info(f"Getting data from {url}")
            res = requests.get(url)
            data = self.process_data(res)
            logger.info(f"Response status code: {res.status_code}")
            return data
        except:
            raise Exception(f"Error connecting to the mock API; it returned {res.status_code} status code with the following message: {res.text}")
        
    def write_to_csv(self, df: polars.DataFrame, file_name: str, folder_name: str = "downloaded_data") -> None:
        full_path = os.path.join(os.getcwd(), "include", folder_name, file_name)
        df.write_csv(full_path)