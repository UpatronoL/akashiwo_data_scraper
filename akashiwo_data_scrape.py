import requests
import json
import pandas as pd
from collections import OrderedDict
from bs4 import BeautifulSoup
from tqdm import tqdm

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
def parse_main_table(table, headers_to_skip, df):
    headers = []
    data = {}
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
    data_df = pd.DataFrame(data)
    df = pd.concat([df, data_df], ignore_index=True).fillna(0)
    return len(data_column), df


###
# table - for this function we will be sending the table whih contains lat long and other information
# data_in - is the final dictionaru after the data has been scraped and duplicated
# times_to_diplicate - the amount of times we need to duplicate the information
###
def parse_coordinate_table(table, df, times_to_duplicate):
    headers = []
    data = {}
    data_in = {}
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all(['th', 'td'])

        if len(cells) >= 2:
            header = cells[0].text.strip()
            headers.append(header)
            data_column = cells[2].text.strip()
            if header in data:
                    data[header].extend(data_column)
            else:
                data[header] = data_column
                
    for key, value in data.items():
        duplicated_values = [value] * times_to_duplicate
        data_in[key] = duplicated_values
    data_df = pd.DataFrame(data_in)
    df = pd.concat([df, data_df], ignore_index=True).fillna(0)
    return df


###
# list1 - in our case would be pointIDs
# list2 - in our case would be gateredYMDs
###
def remove_duplicates(list1, list2):
    zipped_list = list(zip(list1, list2))
    filtered_list = list(OrderedDict.fromkeys(zipped_list))
    return filtered_list

###
# csv - is the path name to the csv that will be used to scrape the data
###
def scraper_for_tables(csv):
    main_data = {}
    coordinate_data = {}
    main_df = pd.DataFrame(main_data)
    coordinate_df = pd.DataFrame(coordinate_data)
    headers_to_skip_main = ["確定値／速報値", "事業・調査名"]
    filtered_data = pd.read_csv(csv).to_dict(orient='records')
    j = 0
    for i, (pointId, gatherYMD) in enumerate(tqdm(filtered_data, total=len(filtered_data), dynamic_ncols=True)):
        html_data = requests.get(f"https://akashiwo.jp/private/akashiwoListInit.php?qpoint_id={str(pointId)}&qspecies_id=3&qgather_ymd_s=&qgather_ymd_e={str(gatherYMD)}")
        html_data.encoding = 'utf-8'
        soup = BeautifulSoup(html_data.text, 'html.parser')
        tables = soup.find_all('table')
        if len(tables) >= 2:
            times_to_duplicate, main_df = parse_main_table(tables[1], headers_to_skip_main, main_df)
            coordinate_df = parse_coordinate_table(tables[0], coordinate_df, times_to_duplicate)
        if j == 500:
            combined_data = coordinate_df.merge(main_df, left_index=True, right_index=True)
            combined_data.to_csv("shatonela.csv", index=False)
            j=0
        j += 1
        if j == 10: break