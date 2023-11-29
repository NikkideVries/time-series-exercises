# import:
import pandas as pd
import numpy as np
import os

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