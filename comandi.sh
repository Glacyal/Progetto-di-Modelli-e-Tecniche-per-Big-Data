#!/bin/bash

sudo apt-get update
sudo apt-get upgrade

loadkeys it
sudo apt install python3-pip

pip install selenium
pip install pandas
pip install webdriver-manager
pip install openpyxl
pip install pydrive


wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb

sudo apt-get install -y xvfb
sudo apt-get -y install xorg xvfb gtk2-engines-pixbuf
sudo apt-get -y install dbus-x11 xfonts-base xfonts-100dpi xfonts-75dpi xfonts-cyrillic xfonts-scalable
Xvfb -ac :99 -screen 0 1280x1024x16 & export DISPLAY=:99

