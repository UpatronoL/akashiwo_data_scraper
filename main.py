from akashiwo_data_scrape import *

def run_scraper(csv_file, file_name):
    scraper_for_tables(csv_file, file_name)

if __name__ == "__main__":
    file_names = ["shatonela_1.csv", "shatonela_2.csv", "shatonela_3.csv", "shatonela_4.csv", "shatonela_5.csv"]
    csv_files = ["shatonela_data_1.csv", "shatonela_data_2.csv", "shatonela_data_3.csv", "shatonela_data_4.csv", "shatonela_data_5.csv"]
    run_scraper(csv_files[4], file_names[4])
