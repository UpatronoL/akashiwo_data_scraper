import requests
import json
import pandas as pd

# msalah.29.10@gmail.com

###
# id - species
# sdate - starting date
# fdate - final date
# output_path - name of the csv output
###
def pull_data(id, sdate, fdate, output_path):
    # Requesting data
    data = requests.get(f"https://akashiwo.jp/public/json/jsonfileKikan.php?kaiku_id=&species_id={str(id)}&gather_ymd_s={sdate}&gather_ymd_e={fdate}&now=263&dispid=1&saisui_value=-1&saisui_value2=-1")

    # Loading the data as a jason and traversing the matrix
    data_json = json.loads(data.text)
    markers = data_json['markers']

    # Fraiming the data with pandas and uploading to a csv
    df = pd.DataFrame(markers)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')


###
# colum_name - array of colum names you would like to remove from a csv
# csv_name - name of a csv file
###
def remove_colum(colum_name, csv_name):
    # transferes all the data from a csv file into a data variable
    data = pd.read_csv(csv_name)

    # removes collumes that where indicated and insures that the empty place gets filled in
    data.drop(columns=colum_name, inplace=True)

    # uploads the manipulated data to the same csv file
    data.to_csv(csv_name, index=False)

###
# id_array - any array you would like to populate with point ids
# date_arrat - any array you would like to populate with dates
# file_name - name of the file you would like to read from
###
def get_id_date(id_array, date_array, file_name):
    data = pd.read_csv(file_name)
    id_array.append(data['pointId'])
    date_array.append(data['gatherYMD'])

###
# data_array - any array that you would like to populate with data
# html_source - html that you have gathered
###
def data_parser(data_array, html_source, ):
    # Finds all existing tables in the html
    tables = html_source.find_all('table')

    # Finds all the wanted data in a table
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            element = row.find_all(['th', 'td'])
            data = [i.text for i in element]
            data_array.append(data)