"""
Sequenctially fill out the form for air quality data for a location and download historical data

The list is prepared offline
"""

import sys
import os
from selenium import webdriver
import csv
import time
from selenium.webdriver.chrome.options import Options


download_dir = "/home/craft/web/analytics/analytics/stocks/pollution/"
preferences = {"download.default_directory" : download_dir}

base_url = "https://app.cpcbccr.com/ccr/#/caaqm-dashboard-all/caaqm-landing/data"

stations = ['Alipur, Delhi - DPCC', 'Anand Vihar, Delhi - DPCC', 'Ashok Vihar, Delhi - DPCC', 'Aya Nagar, New Delhi - IMD', 'Bawana, Delhi - DPCC', 'Burari Crossing, New Delhi - IMD', 'CRRI Mathura Road, New Delhi - IMD', 'DTU, New Delhi - CPCB', 'Dr. Karni Singh Shooting Range, Delhi - DPCC', 'Dwarka-Sector 8, Delhi - DPCC', 'East Arjun Nagar, Delhi - CPCB', 'IGI Airport (T3), New Delhi - IMD', 'IHBAS, Dilshad Garden,New Delhi - CPCB', 'ITO, New Delhi - CPCB', 'Jahangirpuri, Delhi - DPCC', 'Jawaharlal Nehru Stadium, Delhi - DPCC', 'Lodhi Road, New Delhi - IMD', 'Major Dhyan Chand National Stadium, Delhi - DPCC', 'Mandir Marg, New Delhi - DPCC', 'Mundka, Delhi - DPCC', 'NSIT Dwarka, New Delhi - CPCB', 'Najafgarh, Delhi - DPCC', 'Narela, Delhi - DPCC', 'Nehru Nagar, Delhi - DPCC', 'North Campus, DU, New Delhi - IMD', 'Okhla Phase-2, Delhi - DPCC', 'Patparganj, Delhi - DPCC', 'Punjabi Bagh, Delhi - DPCC', 'Pusa, Delhi - DPCC', 'Pusa, New Delhi - IMD', 'R K Puram, New Delhi - DPCC', 'Rohini, Delhi - DPCC', 'Shadipur, New Delhi - CPCB', 'Sirifort, New Delhi - CPCB', 'Sonia Vihar, Delhi - DPCC', 'Sri Aurobindo Marg, Delhi - DPCC', 'Vivek Vihar, Delhi - DPCC', 'Wazirpur, Delhi - DPCC']

def download_data(driver, url, station_to_get):
  try:
    driver.get(url)
    # Get the container to select state by text
    state_name = driver.find_elements_by_xpath("//*[contains(text(), 'State Name :')]")[0]
    parent = state_name.find_elements_by_xpath('..')[0]
    select = parent.find_element_by_tag_name('ng-select')
    select.click()
    dropdown = select.find_elements_by_tag_name("select-dropdown")[0]
    delhi = dropdown.find_elements_by_xpath("//*[contains(text(), 'Delhi')]")[0].click()
    
    # Now, select city in state
    city_name = driver.find_elements_by_xpath("//*[contains(text(), 'City Name :')]")[0]
    parent = city_name.find_elements_by_xpath('..')[0]
    select = parent.find_element_by_tag_name('ng-select')
    select.click()
    dropdown = select.find_elements_by_tag_name("select-dropdown")[0]
    delhi = select.find_elements_by_xpath("//*[contains(text(), 'Delhi')]")[1].click() #Hacky. It finds first one also.
    
    # Get all station names and select one by one
    station_name = driver.find_elements_by_xpath("//*[contains(text(), 'Station Name :')]")[0]
    parent = station_name.find_elements_by_xpath('..')[0]
    select = parent.find_element_by_tag_name('ng-select')
    select.click()
    dropdown = select.find_elements_by_tag_name("select-dropdown")[0]
    selected_station = dropdown.find_elements_by_xpath("//*[contains(text(), '{station}')]".format(station=station_to_get))[0]
    selected_station.click()
      # Select all data points to download
    parameters = driver.find_elements_by_xpath("//*[contains(text(), 'Parameters :')]")[0]
    parent = parameters.find_elements_by_xpath('..')[0]
    select = parent.find_element_by_tag_name('angular2-multiselect')
    select.click()
    select_all = select.find_elements_by_xpath("//*[contains(text(), 'Select All')]")[0]
    select_all.click()
    
    #Click anywhere else
    parameters.click()
    #Set criteria
    criteria = driver.find_elements_by_xpath("//*[contains(text(), 'Criteria :')]")[0]
    parent = criteria.find_elements_by_xpath('..')[0]
    select = parent.find_element_by_tag_name('ng-select')
    select.click()
    dropdown = select.find_elements_by_tag_name("select-dropdown")[0]
    granularity = dropdown.find_elements_by_xpath("//*[contains(text(), '15 Minute')]")[0]
    granularity.click()
    
    #Find submit button
    submit = driver.find_elements_by_xpath("//*[contains(text(), 'Submit')]")[0]
    submit.click()
    
    #New page has opened. Wait for a while
    time.sleep(5)
    download_btn = driver.find_elements_by_class_name('fa-file-excel-o')[0].click()
    
  except:
    print('Exception Geting URL')
    return False

def work_loop():
  options = webdriver.ChromeOptions() 
  
  options.add_experimental_option("prefs", preferences)
  driver = webdriver.Chrome('/home/craft/web/analytics/analytics/stocks/chromedriver',chrome_options=options)
  driver.implicitly_wait(10) #seconds: After page load, some classes may load up by JS. So, wait if not available

  for station in stations:
    ret_val = download_data(driver, base_url, station)
  driver.close()
  driver.quit()


work_loop()
