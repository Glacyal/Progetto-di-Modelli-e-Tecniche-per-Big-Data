import requests
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import xlsxwriter
import pandas as pd
import re
# This is a sample Python script.

# Press Maiusc+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    url_elettricity_map = "https://app.electricitymap.org/map"

    statiold=['Austria', 'Belgio', 'Bulgaria', 'Cipro', 'Croazia', 'Danimarca', 'Estonia', 'Finlandia', 'Francia',
              'Germania', 'Grecia', 'Irlanda', 'Italia', 'Lettonia', 'Lituania', 'Lussemburgo', 'Malta', 'Paesi Bassi',
              'Polonia', 'Portogallo', 'Repubblica Ceca', 'Romania', 'Slovacchia', 'Slovenia', 'Spagna', 'Svezia',
              'Ungheria']

    stati = ['Austria', 'Belgium', 'Bulgaria', 'Cyprus', 'Croatia', 'Denmark', 'Estonia', 'Finland', 'France',
              'Germany', 'Greece', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands',
              'Poland', 'Portugal', 'Czechia', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
              'Hungary']



    s=Service("chromedriver.exe")
    browser = webdriver.Chrome(service=s)

    browser.get(url_elettricity_map)
    time.sleep(3)
    x_button=browser.find_elements(By.CLASS_NAME,"modal-close-button")[0]
    time.sleep(3)
    x_button.click()
    l=[]


    zone_list=browser.find_elements(By.CLASS_NAME,"zone-list")[0]
    zones=zone_list.find_elements(By.TAG_NAME,"a")

    print(len(zones))
    print(len(stati))
    for i in range (0,len(zones)):
         zona = zones[i]
         tag = zona.text.split("\n")  #2 elementi [ numero , nome_stato ]
         stato = tag[len(tag) - 1] # prendiamo nome_stato

         if (stato in stati):

               if(stato not in l):
                 l.append((stato))


    print(len(l))
    l1=[]
    for i in range(0,len(stati)):
          if (stati[i] not in l):
              l1.append(stati[i])

    print(l1)


browser.close()

