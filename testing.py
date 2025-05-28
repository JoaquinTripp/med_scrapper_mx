#from main import scrap_url
from main import get_soup
import pandas as pd
import re
import os

_request_headers:dict = {
    'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Accept-Language':'en-US,en;q=0.9,es;q=0.8'
}
logs:object = open(f'testing.txt', 'w')

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
            if dr.find('div', {'classs':'col-lg-12 col-md-12 col-sm-12 col-xs-12 approximateCost'}) is not None:
                appoinment_price:str = (
                        dr
                            .find('div', {'classs':'col-lg-12 col-md-12 col-sm-12 col-xs-12 approximateCost'})
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

if __name__ == '__main__':
    url = 'https://directorio.eleonor.mx/directory/ariel-garcia/salud-publica/cdmx'
    data = scrap_url(url=url, _logfile=logs, _individual_url=True)
    logs.flush()
    logs.close()