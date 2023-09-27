import requests
import selenium
from datetime import datetime
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
from threading import Thread
import threading
import os
import xlsxwriter
import pandas as pd
import re

from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_argument("--headless")

######################## flusso dati

#tutti i watt sono in kw
#tutti i g sono in kg(tranne carbon_intensity che è gco2/kwh)





#per ogni paese un file excel/csv
#ogni thread avrà accesso ai vari fogli excel a cui ha accesso per gli stati da cercare
#il singolo thread, apre il foglio excel, ne legge il contenuto, avvia la ricerca per una nuova riga, concatena la nuova ricerca ai vecchi risultati infine sovrascrive il file excel del paese specifico


dataframe = pd.DataFrame(
        columns=['timestamp', 'carbon_intensity', 'low_emissions', 'renewable_emissions', 'total_electricity',
                 'total_emissions'
            , 'nucleare_installed_capacity', 'nucleare_production', 'nucleare_emissions',
                 'geotermico_installed_capacity', 'geotermico_production', 'geotermico_emissions'
            , 'biomassa_installed_capacity', 'biomassa_production', 'biomassa_emissions', 'carbone_installed_capacity',
                 'carbone_production', 'carbone_emissions'
            , 'eolico_installed_capacity', 'eolico_production', 'eolico_emissions', 'fotovoltaico_installed_capacity',
                 'fotovoltaico_production', 'fotovoltaico_emissions'
            , 'idroelettrico_installed_capacity', 'idroelettrico_production', 'idroelettrico_emissions',
                 'accumuloidro_installed_capacity', 'accumuloidro_production', 'accumuloidro_emissions'
            , 'batterieaccu_installed_capacity', 'batterieaccu_production', 'batterieaccu_emissions',
                 'gas_installed_capacity', 'gas_production', 'gas_emissions'
            , 'petrolio_installed_capacity', 'petrolio_production', 'petrolio_emissions',
                 'sconosciuto_installed_capacity', 'sconosciuto_production', 'sconosciuto_emissions'
            , 'exchange_export', 'exchange_import'])

url_elettricity_map = "https://app.electricitymap.org/map"

STATES_DIR= "states"

arts=['dal/dalla','il/la']

def get_production_data(data):

    text=data.split('\n')#array 8 elementi
    type=None
    production=None
    emissions=None

    flag_deposit=1;
    if(re.search('accumulato', text[0])):
        flag_deposit=-1

    for art in arts:
        if (re.search(art, text[0])):
            tmp=text[0].split(art)
            type=tmp[1]

    if(type):
        type=type.split('.')
        type = type[0].replace(" ", "")


    total_eletricity=text[1].split("/ ")[1].replace(")","")

    tmp = re.search("[A-Z]+", total_eletricity)
    total_eletricity = re.search("[0-9]+['.'][0-9]*|[0-9]+", total_eletricity)

    if (total_eletricity):
        tmp = tmp.group(0)
        total_eletricity = float(total_eletricity.group(0))

        if(tmp == 'GW'):
            total_eletricity=total_eletricity * 1000000
        elif (tmp == 'MW'):
            total_eletricity = total_eletricity * 1000
        elif (tmp == 'W'):
            total_eletricity = total_eletricity / 1000

        percentage_on_total = re.search("[0-9]+['.'][0-9]*|[0-9]+", text[0])

        if(percentage_on_total):
            percentage_on_total = float(percentage_on_total.group(0))  # prendo il risultato trovato
            # print("                                                            ", percentage_on_total," % di questa fonte sul totale statale")
            production = total_eletricity * (percentage_on_total/100) * flag_deposit
            production=round(production,2)



    installed_capacity = text[4].split("/ ")[1].replace(")", "")
    tmp = re.search("[A-Z]+", installed_capacity)
    installed_capacity = re.search("[0-9]+['.'][0-9]*|[0-9]+", installed_capacity)

    if(installed_capacity):
        tmp = tmp.group(0)
        installed_capacity = float(installed_capacity.group(0))

        if (tmp == 'GW'):
            installed_capacity = installed_capacity * 1000000
        elif (tmp == 'MW'):
            installed_capacity = installed_capacity * 1000

        elif (tmp == 'W'):
            installed_capacity = installed_capacity / 1000

    total_emissions = text[7].split("/ ")[1]
    total_emissions = total_emissions.split(" ")[0]

    tmp = re.search("[a-z]+", total_emissions)
    total_emissions = re.search("[0-9]+['.'][0-9]*|[0-9]+", total_emissions)

    if (total_emissions):
        tmp = tmp.group(0)
        total_emissions = float(total_emissions.group(0))
        if(tmp == 't'):
            total_emissions = total_emissions * 1000
        percentage_on_total = re.search("[0-9]+['.'][0-9]*|[0-9]+", text[6])
        if(percentage_on_total):
            percentage_on_total = float(percentage_on_total.group(0))
            # print("                                                            ", percentage_on_total, " % di inquinamento di questa fonte sul totale statale")
            emissions= total_emissions * (percentage_on_total / 100)
            emissions=round(emissions,2)

    # print("                                                             Capacita' installata Totale", total_eletricity," KW")
    #
    # print("                                                             Emissioni totali", total_emissions, " kg CO2 equivalente al minuto")
    #
    # print("                                                             Tipo di fonte", type)
    #
    # print("                                                             Capacità installata",installed_capacity, "KW")
    #
    # print("                                                             Produzione", production, " KW")
    #
    # print("                                                             Emissioni per questa fonte", emissions, " kg CO2 equivalente al minuto")
    return total_eletricity,total_emissions,type,installed_capacity,production,emissions
    #TODO parser

def get_exchange_data(data):
    text = data.split('\n')
    res=""
    exchange_state=None
    flag_deposit=None
    emissions=None
    exchange = None

    if (re.search('esportata', text[0])):
        exchange_state = text[0].split("in")[1]
        flag_deposit = -1
    else:
        flag_deposit = 1;
        exchange_state = text[0].split("da")[1]


    total_eletricity = text[1].split("/ ")[1].replace(")", "")
    tmp = re.search("[A-Z]+", total_eletricity)
    total_eletricity = re.search("[0-9]+['.'][0-9]*|[0-9]+", total_eletricity)

    if (total_eletricity):
        tmp = tmp.group(0)
        total_eletricity = float(total_eletricity.group(0))

        if (tmp == 'GW'):
            total_eletricity = total_eletricity * 1000000
        elif (tmp == 'MW'):
            total_eletricity = total_eletricity * 1000
        elif (tmp == 'W'):
            total_eletricity = total_eletricity / 1000

        percentage_on_total = re.search("[0-9]+['.'][0-9]*|[0-9]+", text[0])

        if (percentage_on_total):
            percentage_on_total = float(percentage_on_total.group(0))  # prendo il risultato trovato
            # print("                                                            ", percentage_on_total,
            #       " % di questa fonte sul totale statale")
            exchange = total_eletricity * (percentage_on_total / 100) * flag_deposit
            exchange = round(exchange, 2)

    # print("stato scambio",exchange_state)

    installed_exchange_capacity = text[4].split("/ ")[1].replace(")", "")
    tmp = re.search("[A-Z]+", installed_exchange_capacity)
    installed_exchange_capacity = re.search("[0-9]+['.'][0-9]*|[0-9]+", installed_exchange_capacity)

    if (installed_exchange_capacity):
        tmp = tmp.group(0)
        installed_exchange_capacity = float(installed_exchange_capacity.group(0))

        if (tmp == 'GW'):
            installed_exchange_capacity = installed_exchange_capacity * 1000000
        elif (tmp == 'MW'):
            installed_exchange_capacity = installed_exchange_capacity * 1000

        elif (tmp == 'W'):
            installed_exchange_capacity = installed_exchange_capacity / 1000

    total_exchange_emissions = text[7].split("/ ")[1]
    total_exchange_emissions = total_exchange_emissions.split(" ")[0]

    tmp = re.search("[a-z]+", total_exchange_emissions)
    total_exchange_emissions = re.search("[0-9]+['.'][0-9]*|[0-9]+", total_exchange_emissions)

    if (total_exchange_emissions):
        tmp = tmp.group(0)
        total_exchange_emissions = float(total_exchange_emissions.group(0))
        if (tmp == 't'):
            total_exchange_emissions = total_exchange_emissions * 1000
        percentage_on_total = re.search("[0-9]+['.'][0-9]*|[0-9]+", text[6])
        if (percentage_on_total):
            percentage_on_total = float(percentage_on_total.group(0))
            # print("                                                            ", percentage_on_total,
            #       " % di inquinamento di questa fonte sul totale statale")
            emissions = total_exchange_emissions * (percentage_on_total / 100)
            emissions = round(emissions, 2)
    #
    # print("                                                             Capacita' installata Totale", total_eletricity,
    #       " KW")
    #
    # print("                                                             Emissioni  totali", total_exchange_emissions,
    #       " kg CO2 equivalente al minuto")
    #
    # print("                                                             Capacità installata", installed_exchange_capacity, "KW")
    #
    # print("                                                             Scambio", exchange, " KW")
    #
    # print("                                                             Emissioni per trasporto", emissions,
    #       " kg CO2 equivalente al minuto")
    if(exchange_state):
        res+=(str(exchange_state))+"_"
    else:
        res+="nan_"
    if(installed_exchange_capacity):
        res+=(str(installed_exchange_capacity))+"_"
    else:
        res+="nan_"
    if(exchange):
        res+=str((exchange))+"_"
    else:
        res+="nan_"
    if(emissions):
        res+=str((emissions))+";"
    else:
        res+="nan;"
    return res,flag_deposit
    #print("exchange \n", data)

    #TODO parser

def get_carbon_data(data):
    carbon_intensity=data[0].text.split("\n")[0].replace("g","")
    low_emissions=data[1].text.split("\n")[0].replace("%","")
    renewable_emissions=data[2].text.split("\n")[0].replace("%","")
    return carbon_intensity,low_emissions,renewable_emissions

    #TODO parser
# Press the green button in the gutter to run the script.

def run(timestamp, stati):
    '''
    stati=['Austria', 'Belgio', 'Bulgaria', 'Cipro', 'Croazia', 'Danimarca', 'Estonia', 'Finlandia', 'Francia',
              'Germania', 'Grecia', 'Irlanda', 'Italia', 'Lettonia', 'Lituania', 'Lussemburgo', 'Malta', 'Paesi Bassi',
              'Polonia', 'Portogallo', 'Repubblica Ceca', 'Romania', 'Slovacchia', 'Slovenia', 'Spagna', 'Svezia',
              'Ungheria']
    '''
    '''
    stati = ['Austria', 'Belgium', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Estonia', 'Finland', 'France',
            'Germany', 'Greece', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands',
             'Poland', 'Portugal', 'Czechia', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
             'Hungary']
    '''
    #stati = ['Francia']
    try:
        service = Service(executable_path=ChromeDriverManager().install())

        #s=Service("chromedriver.exe")
        #browser = webdriver.Chrome(service=service)
        browser = webdriver.Chrome(service=service,options=chrome_options)

        browser.get(url_elettricity_map)
        time.sleep(2)
        x_button=browser.find_elements(By.CLASS_NAME,"modal-close-button")[0]
        tent=0
        while tent<3:
            tent+=1
            try:
                time.sleep(2)
                x_button.click()
                break
            except:
                browser.get(url_elettricity_map)
                time.sleep(2)
                x_button = browser.find_elements(By.CLASS_NAME, "modal-close-button")[0]

        time.sleep(2)
        zone_list=browser.find_elements(By.CLASS_NAME,"zone-list")[0]
        zones=zone_list.find_elements(By.TAG_NAME,"a")
        for s in stati:
            try:
                for i in range(0, len(zones)):
                    zona = zones[i]
                    tag = zona.text.split("\n")  # 2 elementi [ numero , nome_stato ] o 3 elementi [ numero ,zona_specifica, nome_stato ]
                    stato = tag[len(tag) - 1]  # prendiamo nome_stato

                    state_name = ''
                    if (len(tag) == 3):
                        zona_nello_stato = tag[len(tag) - 2]
                        state_name += zona_nello_stato
                        state_name += " (" + stato + ")"
                    else:
                        state_name = stato
                    state_name = state_name.replace("/", " ")

                    if (s==state_name):
                        print(s)
                        tmp = {}
                        path = os.path.join(STATES_DIR, s+".xlsx")
                        exist=False
                        try:
                            dataframe_state=pd.read_excel(path)
                            exist=True
                        except:
                            pass

                        zona.click()
                        time.sleep(2)
                        body=browser.find_elements(By.TAG_NAME,"body")[0]
                        try:
                            carbon_data=body.find_elements(By.CLASS_NAME,"country-col")
                        except:
                            pass
                        if(carbon_data):
                            carbon_intensity,low_emissions,renewable_emissions=get_carbon_data(carbon_data)
                            tmp['carbon_intensity'] = carbon_intensity
                            tmp['low_emissions'] = low_emissions
                            tmp['renewable_emissions'] = renewable_emissions

                        rows = browser.find_elements(By.CLASS_NAME, "row") #prendiamo tutte le righe ognuna delle quali è una fonte energetica o scambio
                        time.sleep(2)
                        action = ActionChains(browser)
                        res_import=""
                        res_export=""
                        for r in rows:
                            action.move_to_element(r).perform()
                            time.sleep(2)
                            body = browser.find_elements(By.TAG_NAME, "body")[0]
                            production_popup = None
                            exchange_popup = None
                            try:
                                production_popup = body.find_element(By.ID,"countrypanel-production-tooltip")
                            except:
                                try:
                                    exchange_popup = body.find_element(By.ID,"countrypanel-exchange-tooltip")
                                except:
                                    pass
                            if (production_popup):
                                total_eletricity,total_emissions,type,installed_capacity,production,emissions=get_production_data(production_popup.text)
                                tmp['total_electricity'] = total_eletricity
                                tmp['total_emissions'] = total_emissions
                                tmp[type.lower() + '_installed_capacity'] = installed_capacity
                                tmp[type.lower() + '_production'] = production
                                tmp[type.lower() + '_emissions'] = emissions

                            elif (exchange_popup):
                                tmp_v,flag=get_exchange_data(exchange_popup.text)
                                if(flag>0):
                                    res_import+=(tmp_v)
                                else:
                                    res_export+=(tmp_v)
                        tmp['exchange_export'] = res_export
                        tmp['exchange_import'] = res_import
                        tmp['timestamp']=timestamp
                        df=pd.concat([dataframe.copy(),pd.DataFrame(tmp,index=[0])],ignore_index=True)
                        if (not exist):
                            dataframe_state = dataframe.copy()
                        dataframe_state = pd.concat([dataframe_state, df])
                        try:
                            dataframe_state.to_excel(path,sheet_name=s,index=False)
                        except:
                            dataframe_state.to_excel(path,sheet_name=s.split(" ")[0],index=False)

                        back = browser.find_elements(By.CLASS_NAME, "left-panel-back-button")[0]
                        back.click()
                        time.sleep(2)
                        zone_list = browser.find_elements(By.CLASS_NAME, "zone-list")[0]
                        zones = zone_list.find_elements(By.TAG_NAME, "a")
                        break
            except:
                path = os.path.join(STATES_DIR, s + ".xlsx")
                exist = False
                try:
                    dataframe_state = pd.read_excel(path)
                    exist = True
                except:
                    pass
                tmp={}
                tmp['timestamp'] = timestamp
                df = pd.concat([dataframe.copy(), pd.DataFrame(tmp, index=[0])], ignore_index=True)
                if (not exist):
                    dataframe_state = dataframe.copy()
                dataframe_state = pd.concat([dataframe_state, df])
                try:
                    dataframe_state.to_excel(path, sheet_name=s, index=False)
                except:
                    dataframe_state.to_excel(path, sheet_name=s.split(" ")[0], index=False)

                try:
                    back = browser.find_elements(By.CLASS_NAME, "left-panel-back-button")[0]
                    back.click()
                    time.sleep(2)
                finally:
                    zone_list = browser.find_elements(By.CLASS_NAME, "zone-list")[0]
                    zones = zone_list.find_elements(By.TAG_NAME, "a")

        timestamp = datetime.today().strftime('%H:%M %d-%m-%Y')
        print("sono il thread", threading.get_ident(), timestamp)
        browser.close()
    except:
        print("nessuna connessione o errore non previsto")



if __name__ == '__main__':

    '''
    stati = ['Austria', 'Belgio', 'Bulgaria', 'Cipro', 'Croazia', 'Danimarca', 'Estonia', 'Finlandia', 'Francia',
             'Germania', 'Grecia', 'Irlanda', 'Lettonia', 'Lituania', 'Lussemburgo', 'Malta', 'Paesi Bassi',
             'Polonia', 'Portogallo', 'Repubblica Ceca', 'Romania', 'Slovacchia', 'Slovenia', 'Svezia',
             'Ungheria','Italia'] # italia 6  danimarca 2 sezioni

    stato = ['Spagna'] #11 sezioni
    '''

    stati = ['El Hierro (Spagna)','Svezia','Francia','Danimarca orientale (Danimarca)','Romania','Spagna','Belgio','Lettonia','Portogallo','Gran Canaria (Spagna)',
           'Danimarca occidentale (Danimarca)','Ungheria','Formentera (Spagna)','Lussemburgo','Finlandia','Austria','Slovenia','Maiorca (Spagna)','Paesi Bassi','Lituania',
           'Slovacchia','Croazia','Ibiza (Spagna)','Polonia','Irlanda','Sicilia (Italia)','Bulgaria','Tenerife (Spagna)','Germania','Centronord (Italia)','Cipro',
           'Minorca (Spagna)','Settentrione (Italia)','Estonia','Fuerteventura Lanzarote (Spagna)','La Palma (Spagna)','Meridione (Italia)','La Gomera (Spagna)',
           'Centrosud (Italia)','Sardegna (Italia)']

    nThread=8

    x = int(len(stati)/(nThread))

    final_list = lambda stati, x: [stati[i:i + x] for i in range(0, len(stati), x)]

    output = final_list(stati, x)
    # print()
    for out in output:

        print(len(out),"  -->",out)

    # timestamp = datetime.today().strftime('%H:%M %d-%m-%Y')
    # print(timestamp)
    #
    # for i in range(nThread):
    #     t = Thread(target=run, args=(timestamp, output[i],))
    #     t.start()
    # time.sleep(300)
    # timestamp = datetime.today().strftime('%H:%M %d-%m-%Y')
    # print("sono passati 5 minuti",timestamp)
    # time.sleep(60)
    # timestamp = datetime.today().strftime('%H:%M %d-%m-%Y')
    # print("sono passati 6 minuti", timestamp)
    # time.sleep(60)
    # timestamp = datetime.today().strftime('%H:%M %d-%m-%Y')
    # print("sono passati 7 minuti", timestamp)


    c=1
    while (c <= 10):
        timestamp = datetime.today().strftime('%H:%M %d-%m-%Y')
        print(timestamp)

        for i in range(nThread):
            t = Thread(target=run, args=(timestamp,output[i],))
            t.start()
        time.sleep(600)
        print("fine c = ",c)
        c += 1

    # c=1
    # while(c<=10):
    #     timestamp = datetime.today().strftime('%H:%M %d-%m-%Y')
    #     print(timestamp)
    #     x(timestamp)
    #     c+=1

