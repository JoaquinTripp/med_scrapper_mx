"""
Script to process and merge 3 scrapps from eleonor.mx
"""

import pandas as pd
import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns


# auxiliary functions
def drop_duplicated_drs(data:pd.DataFrame)-> pd.DataFrame:
    """
    Function to drop duplicated by name and row size
    """
    try:
        data.loc[:,'row_size'] = data.apply(lambda x: x[2:].memory_usage(deep=True), axis=1)
        data.sort_values(by=['name','row_size'], ascending=True, inplace=True)
        data.drop_duplicates(subset=['name'], keep='last', inplace=True)
        return data

    except Exception as e:
        raise e


def predict_plan(x:pd.Series) -> str:
    """
    Function to predict which type of plan the Dr. pays for
    """
    result:str = 'Undefined'
    premium:int = x['premium']
    schedule:str = x['schedule']

    if premium == 1:
        result = 'Experto'
    elif premium == 0 and schedule == 'available':
        result = 'Intermedio'
    elif premium == 0 and schedule == 'not available':
        result = 'Básico'
    
    return result

_prices:dict = {
    'Experto':699,
    'Intermedio':499,
    'Básico':299
}


def main():
    
    states:pd.DataFrame = pd.read_csv('./data/states.csv')
    cities:pd.DataFrame = pd.read_csv('./data/cities.csv')
    #states_cities:pd.DataFrame = pd.read_csv('./data/state_cities.csv')
    
    return

    



if __name__=='__main__':
    main()