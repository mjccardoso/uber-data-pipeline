from common_lib import *

    
class etl:
    def __init__(self) -> None:
        pass
    
    def extract(self, type:str, year:str, month:str):
        """_summary_

        Args:
            type (str): _description_
            year (str): _description_
            month (str): _description_

        Returns:
            _type_: _description_
        """
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_{year}-{month}.parquet'
        request = requests.get(url)
        
        if request.status_code == 200:    
            parquet_buffer = io.BytesIO(x.content)
            table = pq.read_table(parquet_buffer)    
            return table.to_pandas()
        else:
            print(f'Fail to obtain data: {request.status_code}')
            return None
    
        #ToDo
            #Add a Sample of the extrated Raw Data
            #Add 
        
    def transform(self, df: pandas.DataFrame):
        pass
    
    def load(self, df: pandas.DataFrame, destination: str):
        pass
    


