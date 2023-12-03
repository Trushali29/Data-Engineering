import pandas as pd
import datetime
from datetime import *
import glob
import xml.etree.ElementTree as ET

tmpfile    = "dealership_temp.tmp"               # store all extracted data

logfile    = "dealership_logfile.txt"            # all event logs will be stored

targetfile = "dealership_transformed_data.csv"   # transformed data is stored
# extract from CSV
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# extract from JSON
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe


# XML extract information
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        car_model = person.find("car_model").text
        year_of_manufacture = int(person.find("year_of_manufacture").text)
        price = float(person.find("price").text)
        fuel = person.find("fuel").text

        dataframe = dataframe._append({"car_model":car_model,"year_of_manufacture":year_of_manufacture,"price":price,"fuel":fuel},ignore_index = True)

        return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    # for csv files
    for csvfile in glob.glob("D:/Data Engineering/datasource/*.csv"):
        extracted_data = extracted_data._append(extract_from_csv(csvfile),ignore_index = True)
    # for json files
    for jsonfile in glob.glob("D:/Data Engineering/datasource/*.json"):
        extracted_data = extracted_data._append(extract_from_json(jsonfile),ignore_index= True)

    # for xml files
    for xmlfile in glob.glob("D:/Data Engineering/datasource/*.xml"):
        extracted_data = extracted_data._append(extract_from_xml(xmlfile),ignore_index=True).drop_duplicates()
        
    return extracted_data


def transform(data):
    data['price'] = round(data.price,2)
    return data


def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    # hour- minute-second-monthname-day-year
    now =  datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_lof_file.txt","a") as f : f.write(timestamp + ","+message+"\n")
    

log("ETL job started")
log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")

log("Transform phase Started")
transform_data = transform(extracted_data)

log("Transfor phase ended")

log("Load phase Started")
load(targetfile,transform_data)
log("Load phase Ended")

log("ETL job Ended")

    
