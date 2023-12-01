# import:
import pandas as pd
import numpy as np
import os
import env

import requests
import math




#----------------------Starwars API-------------------------#
# find out what is in the api: 
def fetch_api_data(base_url):
    '''
    This function will: 
    - get information from the starwars api 
    - give descriptions of each data frame
    '''
    # Fetch data for the provided base URL
    response = requests.get(base_url)
    data = response.json()

    # Print the entire data dictionary
    print("Data Dictionary:")
    print(data)
    print()
    
    # Extract information
    for key, url in data.items():
        # Skip keys that are not relevant for counting
        if not url.endswith('/'):
            continue

        # Fetch data for the specific key to get counts and results
        key_response = requests.get(url)
        key_data = key_response.json()

        # Calculate relevant information
        count_key = f'number_of_{key}'
        count_results = len(key_data['results'])
        max_page = math.ceil(key_data['count'] / count_results)

        print(f"{count_key}: {key_data['count']}")
        print(f"max_page: {max_page}")
        print(f"number_of_results: {count_results}")
        print()

    
#make the data frame: 
def acquire_starwars(base_url, num_pages):
    '''
    This function will: 
    - Create a dataframe for the url provided
    - The num_pages are the number of pages the API site has
   '''
    
    #create df
    df = pd.DataFrame()
    
    #Get the data: 
    for page in range(1, num_pages + 1):
        #get url for current page: 
        current_url = (f'{base_url}?page={page}')
        #get the data:
        response = requests.get(current_url)
        data = response.json()
        #extract results for the current page
        current_page = pd.DataFrame(data['results'])
        #concatenate the results to the main DataFrame
        df = pd.concat([df,current_page], ignore_index = True)
    return df


#save the dataframe as a csv: 
def csv(dataframe, file_name):
    '''
    This funciton will:
    - Take in a dataframe (df)
    - Take in the file_name
    - Save the data into the file name if it is not already a csv
    - call the dataframe: 
    '''
    
    if os.path.isfile(file_name):
        # If the CSV file exists, read in data from the CSV file.
        df = pd.read_csv(file_name, index_col=0)
    else:
        # Read fresh data from the DataFrame
        df = dataframe
        
        # Cache data to CSV file
        df.to_csv(file_name)

    return df


#----------------------Store data-----------------------#

def get_connection(db, user=env.user, host=env.host, password= env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'


def acquire_store():
    
    filename = 'store.csv'
    
    if os.path.exists(filename):
        
        return pd.read_csv(filename)
    
    else:
        
        query = '''
                SELECT sale_date, sale_amount,
                item_brand, item_name, item_price,
                store_address, store_zipcode
                FROM sales
                LEFT JOIN items USING(item_id)
                LEFT JOIN stores USING(store_id)
                '''
        
        url = get_connection(db='tsa_item_demand')
        
        df = pd.read_sql(query, url)
        
        df.to_csv(filename, index=False)
        
        return df
    
#------------ Germany Data -----------#
def get_germany_data():
    '''
    This function creates a csv of germany energy data if one does not exist
    if one already exists, it uses the existing csv 
    and brings it into pandas as dataframe
    '''
    if os.path.isfile('opsd_germany_daily.csv'):
        df = pd.read_csv('opsd_germany_daily.csv', index_col=0)
    
    else:
        url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
        df = pd.read_csv(url)
        df.to_csv('opsd_germany_daily.csv')

    return df