import requests
import json
import pandas as pd

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
# table - for this function we need to send the table where the main data is conteined
# headers_to_skip - is an array of headers and data that we would like to skip
# data - dictionary that will be filled in by the function
###
def parse_main_table(table, headers_to_skip, data):
    headers = []

    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all(['th', 'td'])

        if len(cells) >= 2:
            header = cells[0].text.strip()
            if header not in headers_to_skip:
                headers.append(header)
                data_column = [cell.text.strip() for cell in cells[1:]]
                if header in data:
                    data[header].extend(data_column)
                else:
                    data[header] = data_column
    return len(data_column)


###
# table - for this function we will be sending the table whih contains lat long and other information
# data_in - is the final dictionaru after the data has been scraped and duplicated
# times_to_diplicate - the amount of times we need to duplicate the information
###
def parse_coordinate_table(table, data_in, times_to_duplicate):
    headers = []
    data = {}
    
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all(['th', 'td'])

        if len(cells) >= 2:
            header = cells[0].text.strip()
            headers.append(header)
            data_column = cells[2].text.strip()
            data[header] = data_column
            if header in data:
                    data[header].extend(data_column)
            else:
                data[header] = data_column
                
    for key, value in data.items():
        duplicated_values = [value] * times_to_duplicate
        data_in[key] = duplicated_values


            