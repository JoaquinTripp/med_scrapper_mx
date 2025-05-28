import pandas as pd
import requests
import json
import time
import os
import re
from bs4 import BeautifulSoup as bs
from typing import Union

_request_headers:dict = {
    'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Accept-Language':'en-US,en;q=0.9,es;q=0.8'
}
_column_names:str = [
            'url',
            'state',
            'name', 
            'premium',
            'speciality',
            'schedule',
            'videocall',
            'videocall_price',
            'principal_caresite',
            'other_caresite',
            'appointment_price',
            'services',
            'dr_url',
            'phone_number'
]

def get_soup(main_url:str, headers:dict[str:str]) -> Union[object, None]: 
    
    try:
        response = requests.get(main_url, headers=headers, timeout=5)        
        if response.status_code == 200:
            soup = bs(response.text, "html.parser")
            return soup
    except:
        return None


def build_urls():
    if not os.path.exists('./urls_tree.json'):
        _base_url:str = 'https://directorio.eleonor.mx/directory'
        soup = get_soup(main_url=_base_url, headers=_request_headers)

        _states_tags:list = [opt for opt in soup.find_all('optgroup') if len(opt.get('label')) > 3]
        states_dict:dict = {}

        for state in _states_tags:
            # dict vars
            state_name:str = state.get('label')
            states_dict[state_name] = {}
            
            # new list to loop
            _cities_tags:list = [st for st in state.find_all('option') if len(st.get('data-estate'))>0]
            
            # mulitple state ids
            state_ids:list = list(set([ st.get('data-estate') for st in state.find_all('option')]))
            
            for state_id in state_ids:
                states_dict[state_name][state_id] = {}
                states_dict[state_name][state_id]['state_url'] = f'{_base_url}/e/{state_id}'
                states_dict[state_name][state_id]['cities'] = {}

                for city in [tag for tag in _cities_tags if tag.get('data-estate')==state_id]:
                    city_name:str = city.get('value')
                    #states_dict[state_name][state_id].append(city_name)
                    states_dict[state_name][state_id]['cities'][city_name] = {}
                    states_dict[state_name][state_id]['cities'][city_name]['city_url'] = f'{_base_url}/c/{city_name}'
                    states_dict[state_name][state_id]['cities'][city_name]['state_city_url'] = f'{_base_url}/e/{state_id}/c/{city_name}'

        with open('./urls_tree.json', 'w') as f:
            json.dump(states_dict, f, indent=4, ensure_ascii=False)
        print('>>> urls tree saved!')
    else:
        with open('./urls_tree.json','r') as f:
            states_dict = json.load(f)
    
    # build url frames
    _url_tree_cities:pd.DataFrame = pd.DataFrame(columns=['state','state_id','city','url'])
    _url_tree_states:pd.DataFrame = pd.DataFrame(columns=['state','state_id','city','url'])
    _url_tree_state_city:pd.DataFrame = pd.DataFrame(columns=['state','state_id','city','url'])

    for state, state_node in states_dict.items():

        for state_id, sid_node in state_node.items():

            _state_url:dict = {'state':state, 'url':sid_node['state_url']}
            _url_tree_states = pd.concat([_url_tree_states, pd.DataFrame([_state_url])])

            for city, city_node in sid_node['cities'].items():

                _city_url:dict = {'state':state,'state_id':state_id,'city':city,'url':city_node['city_url']}
                _state_city_url:dict = {'state':state,'state_id':state_id,'city':city,'url':city_node['state_city_url']}

                _url_tree_cities = pd.concat([_url_tree_cities, pd.DataFrame([_city_url])])
                _url_tree_state_city = pd.concat([_url_tree_state_city, pd.DataFrame([_state_city_url])])

    _url_tree_cities = (
        _url_tree_cities
            .reset_index(drop=True)
            .drop_duplicates(subset='url')
    )
    
    _url_tree_states = (
        _url_tree_states
            .reset_index(drop=True)
            .drop_duplicates(subset='url')
    )
    
    _url_tree_state_city = (
        _url_tree_state_city
            .reset_index(drop=True)
            .drop_duplicates(subset='url')
    )

    return _url_tree_states, _url_tree_cities, _url_tree_state_city


def scrap_urls(urls_dataframe:pd.DataFrame, file_name:str = None, format_urls:bool = True, **kwargs) -> pd.DataFrame:
    _individual_urls:bool = kwargs.get('_individual_urls', 'None')
    if file_name == None:
        file_name = 'data.csv'

    logfile:object = open(f"./data/logs_{file_name.replace('.csv','')}.txt",'w')

    if os.path.exists(f"./data/{file_name}"):
        outfile:object = open(f"./data/{file_name}", "a")
    else:
        outfile:object = open(f"./data/{file_name}", "w")
        outfile.write(f"{','.join(_column_names)}\n")
        outfile.flush()
    
    outfile_df:pd.DataFrame = pd.read_csv(f'./data/{file_name}')
    
    


    for index, row in urls_dataframe.iterrows():
        try:
            idx:int = 0 
            _url:str = row['url']
            _state_name:str = row['state']
            _total_drs:int = 0
            _url_list:list[str] = set(outfile_df['url'])

            print(f'... scrapping {_state_name}: ')

            while True:
                
                if format_urls: 
                    formatted_url:str = f"{row['url']}/i/{idx}"
                    if formatted_url in _url_list:
                        logfile.write(f'{formatted_url} is already scrapped\n')
                        logfile.flush()
                        idx += 1
                        continue
                else: 
                    formatted_url = row['url']
                    if formatted_url in _url_list:
                        logfile.write(f'{formatted_url} is already scrapped\n')
                        logfile.flush()
                        break

                print(f'> {formatted_url}', end=' ')
                for n in range(3):
                    
                    data_df = scrap_url(url=formatted_url, _logfile=logfile, _individual_url=_individual_urls)
                    data_df.insert(0,'state',_state_name)
                    data_df.insert(0,'url',formatted_url)
                    
                    if data_df.shape[0] > 0:
                        break
                    
                    time.sleep(2)

                if data_df.shape[0] == 0:
                    logfile.write(f'{formatted_url} 0 drs scrapped\n')
                    logfile.flush()
                    print('+ (0)')
                    break
                
                for _, dr in data_df.iterrows():
                    formatted_line:str = ','.join(
                            [
                                str(x) 
                                    if isinstance(x,(int,float)) 
                                    else f'"{x}"'
                                for x in list(dr)
                            ]
                        )
                    outfile.write(f'{formatted_line}\n')
                    
                    outfile.flush()
                
                idx += 1
                print(f'(+ {data_df.shape[0]})')
                _total_drs += data_df.shape[0]

                if _individual_urls:
                    break
                
            print(f'<<< {_state_name} >>> DONE: current {_total_drs} drs registers')
            continue
    
        except Exception as e:
            print(f'{" ERROR ":#^32}\n{e} with {_url}')
            logfile.write(f'{" ERROR ":#^32}\n{e} with {_url}\n')

            outfile.flush()
            logfile.flush()
            continue

    outfile.close()
    logfile.close()
    return print('>>>>>>>>>>>> Output and logs saved!')


def scrap_url(url:str, **kwargs) -> pd.DataFrame:
    """Function to scrap an url"""
    _logfile = kwargs.get('_logfile', None)
    _individual_url = kwargs.get('_individual_url', None)
    drs_data = pd.DataFrame(columns=['name','premium','speciality'])
    soup = get_soup(url, headers=_request_headers)
    new_url:str = None
    
    if soup == None:
        return drs_data
    
    drs_list:list = [dr for dr in soup.find_all('div',{'class':'result'}) if dr.get('class')==['result']]

    if len(drs_list) == 0 and _individual_url == False:
        try:
            new_url:str = soup.find('meta').get('content').split(';')[1].strip()
            new_url = f'https://directorio.eleonor.mx{new_url}'
            soup = get_soup(new_url, headers=_request_headers)
            drs_list:list = soup.find_all('article')
        except Exception as e:
            drs_list = []
            _logfile.write(f'{"#"*10} ERROR {"#"*10} with {new_url} - {e}')
            return drs_data
    
    elif _individual_url:
        drs_list = [soup]
    
    for dr in drs_list:
        try:
            dr_name:str = dr.find('h3',{'id':'medicName'}).get_text().strip()
            premium:str = 1 if dr.find('span',{'class':'insignia d-Premium'}) != None else 0
            schedule:str = 'avaialable' if dr.find('a',{'id':'scheduleButton'}) is not None else 'Not avaialable' 
            videocall:int = 0
            videocall_price:str = 0
            appoinment_price:str = 0
            principal_address:str = 'None'
            other_address:str = 'None'
            specilities:str = 'None'
            services:str = 'None'
            phone_number:str = 'None'
            dr_url:str = 'None'

            # videocall
            if dr.find('div',{'id':'viewVideoCallSchedule'}) is not None:
                videocall:int = 1
                pattern:str = r'^(.*?)\s([$]+[0-9]+)(.*)$'
            
                # videocall price
                if re.match(pattern,dr.find('div',{'id':'viewVideoCallSchedule'}).get_text()) is not None:
                    videocall_price = (
                        re.match(
                            pattern,
                            dr.find('div',{'id':'viewVideoCallSchedule'}).get_text()
                        ).group(2)
                    )


            # appointment price
            if dr.find('div', {'class':'col-lg-12 col-md-12 col-sm-12 col-xs-12 approximateCost'}) is not None:
                appoinment_price:str = (
                        dr
                            .find('div', {'class':'col-lg-12 col-md-12 col-sm-12 col-xs-12 approximateCost'})
                            .find('label',{'class':'t-orange2 text-sms font-popR'})
                            .get_text()
                    )

            # principal caresite
            if dr.find('input',{'class':'consultorySelector'}) is not None:
                principal_address_title:str = dr.find('input',{'class':'consultorySelector'}).get('data-title')
                principal_address_name:str = dr.find('input',{'class':'consultorySelector'}).get('data-resume')
                principal_address = f'{principal_address_title} ({principal_address_name})'
                
            
            # other caresite
            if dr.find('select',{'class':'consultorySelector'}) is not None:
                address_list:list[str] = dr.find('select',{'class':'consultorySelector'}).find_all('option')
                principal_address_title = address_list[0].get('data-title')
                principal_address_name = address_list[0].get('data-address')
                principal_address = f'{principal_address_title} ({principal_address_name})'

                for i, address in enumerate(address_list):
                    address_title:str = address.get('data-title')
                    address_name:str = address.get('data-address')
                    address_obj:str = f'{address_title} ({address_name})'
                    address_list[i] = address_obj
                
                other_address = '|'.join(address_list)
            
            # specialist
            if len(dr.find_all('span',{'class':'specialism t-black'}))>0:
                specilities:list = ''.join([spe.get_text() for spe in dr.find_all('span',{'class':'specialism t-black'})])

            # services
            if dr.find('div',{'class':'ui basic segment row servicesInfo'}) is not None:
                services_list = dr.find('div',{'class':'ui basic segment row servicesInfo'}).find_all('li',{'class':'t-blue2'})
                services_list = [str(s.get_text()).strip() for s in services_list]
                services = '|'.join(services_list)

            # dr url
            if dr.find('div',{'class':'photoHolder'}).find('a') is not None:
                dr_url = dr.find('div',{'class':'photoHolder'}).find('a').get('href')
                dr_url = f'https://directorio.eleonor.mx{dr_url}'
            elif new_url != None:
                dr_url = new_url
            elif _individual_url:
                dr_url = url

            # phone number
            if dr.find('div',{'class':'column text-left t-link'}) is not None:
                phone_number = dr.find('div',{'class':'column text-left t-link'}).find('a',{'data-category':'E_Directorio'}).get('href')
                
            dr_obj:dict = {
                'name':dr_name, 
                'premium':premium, 
                'speciality':specilities,
                'schedule':schedule, 
                'videocall':videocall,
                'videocall_price':videocall_price,
                'principal_caresite':principal_address,
                'other_caresite':other_address,
                'appointment_price':appoinment_price, 
                'services':services, 
                'dr_url':dr_url,
                'phone_number':phone_number
            }

            # clean weird characters
            replaces:dict = {
                '\n':' ',
                '"':'\''
            }
            for x,y in replaces.items():
                dr_obj['name'] = dr_obj['name'].replace(x,y)
                dr_obj['speciality'] = dr_obj['speciality'].replace(x,y)
                dr_obj['principal_caresite'] = dr_obj['principal_caresite'].replace(x,y)
                dr_obj['other_caresite'] = dr_obj['other_caresite'].replace(x,y)
                dr_obj['services'] = dr_obj['services'].replace(x,y)            

            drs_data = pd.concat([drs_data, pd.DataFrame([dr_obj])],axis=0)
        except Exception as e:
            _logfile.write(f'{"#"*10} ERROR {"#"*10} with {url} | {dr_name} {e}\n')
            continue

    return drs_data.reset_index(drop=True)
                    

def main() -> pd.DataFrame:
    
    states, cities, state_cities = build_urls()
    print(f'\n{" SCRAPPING BY STATES ":#^72}\n')
    scrap_urls(states, file_name='states.csv')

    print(f'\n{" SCRAPPING BY CITIES ":#^72}\n')
    time.sleep(10)
    scrap_urls(cities, file_name='cities.csv')
    
    print(f'\n{" SCRAPPING BY STATES-CITIES ":#^72}\n')
    time.sleep(10)
    scrap_urls(state_cities, file_name='state_cities.csv')
    return print('\n!!!!!!FINISHED MTHFR!!!!!!!!')
    
if __name__ == '__main__':
    
    main()
    #data = pd.read_csv('./missing_drs.csv')
    #scrap_urls(data, file_name='missing_drs.csv', format_urls=False, _individual_urls=True)