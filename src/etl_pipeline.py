from common_lib import *

    
class etl:
    def __init__(self) -> None:
        pass
    
    def extract_locally(self, type:str, year:str, month:str):
        """
        Extracts the data from a Parquet file from the URL and transforms it into a Pandas DataFrame.

        Args:
            type (str): Type of data to be extracted (e.g. ‘yellow_tripdata’).
            year (str): Year of the data.
            month (str): Month of the data.

        Returns:
            pd.DataFrame: DataFrame with the extracted data or None if the extraction fails.
        """
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_{year}-{month}.parquet'
        logging.info(f'Extraction Begging {url} {pandas.Timestamp.now()}')
        
        try:
            request = requests.get(url=url)
            
            if request.status_code == 200:   
                
                parquet_buffer = io.BytesIO(initial_bytes=request.content)
                table = pq.read_table(source=parquet_buffer)
                dataframe = pandas.DataFrame()      
                dataframe = table.to_pandas()
                dataframe_path = f'{current_dir}/data/raw/trip-data_{type}_{year}-{month}.parquet'
                 
                dataframe.iloc[:100000].to_excel(f"{dataframe_path}", index=False)
                logging.info(msg=f'Extraction well-done for {type} with time {year}-{month}')
                logging.info(msg=f'Data with {dataframe_path.info()} rows')              
                
                return dataframe
            else:
                print(f'Fail to obtain data: {request.status_code}')
                return None
        except Exception as e:
            logging.error(msg=f'Error during extraction: {str(object=e)}')
            return None

        
    def transform(self, df: pandas.DataFrame) -> None:
        pass
    
    def load(self, df: pandas.DataFrame, destination: str)-> None:
        pass
    


