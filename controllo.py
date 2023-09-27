import requests
import selenium
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import xlsxwriter
import pandas as pd
import re
from selenium.webdriver.chrome.options import Options
# This is a sample Python script.

# Press Maiusc+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
chrome_options = Options()
chrome_options.add_argument("--headless")

# Press the green button in the gutter to run the script.


if __name__ == '__main__':
    url_elettricity_map = "https://app.electricitymap.org/map"

    statiold=['Austria', 'Belgio', 'Bulgaria', 'Cipro', 'Croazia', 'Danimarca', 'Estonia', 'Finlandia', 'Francia',
              'Germania', 'Grecia', 'Irlanda', 'Italia', 'Lettonia', 'Lituania', 'Lussemburgo', 'Malta', 'Paesi Bassi',
              'Polonia', 'Portogallo', 'Repubblica Ceca', 'Romania', 'Slovacchia', 'Slovenia', 'Spagna', 'Svezia',
              'Ungheria']
    '''
    stati = ['Austria', 'Belgium', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Estonia', 'Finland', 'France',
              'Germany', 'Greece', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands',
              'Poland', 'Portugal', 'Czechia', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
              'Hungary']
    '''
    stati = ['Austria', 'Belgium', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Estonia', 'France',
              'Germany', 'Ireland', 'Latvia', 'Lithuania', 'Luxembourg', 'Netherlands',
              'Poland', 'Portugal', 'Czechia', 'Romania', 'Slovenia', 'Sweden',
              'Hungary']

    n=500
    secondi=0;
    for j in range(0,n):



        s=Service("chromedriver.exe")
        #browser = webdriver.Chrome(service=s)
        browser = webdriver.Chrome(service=s, options=chrome_options)

        browser.get(url_elettricity_map)
        time.sleep(0.5)
        x_button=browser.find_elements(By.CLASS_NAME,"modal-close-button")[0]
        time.sleep(0.5)
        x_button.click()


        zone_list=browser.find_elements(By.CLASS_NAME,"zone-list")[0]
        zones=zone_list.find_elements(By.TAG_NAME,"a")

        for i in range (0,len(zones)):
             zona = zones[i]
             tag = zona.text.split("\n")  #2 elementi [ numero , nome_stato ]
             stato = tag[len(tag) - 1] # prendiamo nome_stato

             if (stato in stati):
                 l = []
                 l.append(secondi)
                 l.append(stato)
                 zona.click()
                 time.sleep(0.5)
                 left_panel = browser.find_elements(By.CLASS_NAME, "left-panel-zone-details")[0]  # cliccato il paese prendiamo il pannello a sinistra
                 time.sleep(0.5)
                 body = browser.find_elements(By.TAG_NAME, "body")[0]
                 time.sleep(0.5)
                 carbon_data = body.find_elements(By.CLASS_NAME, "country-col")

                 for cd in carbon_data:
                     l.append(cd.text)
                 print(l)

                 back = browser.find_elements(By.CLASS_NAME, "left-panel-back-button")[0]
                 back.click()
                 zone_list = browser.find_elements(By.CLASS_NAME, "zone-list")[0]
                 zones = zone_list.find_elements(By.TAG_NAME, "a")

        time.sleep(27.5)
        secondi= secondi + 27.5 + 1 + 31,5





