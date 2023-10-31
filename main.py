from akashiwo_data_scrape import *
from multiprocessing import Process

def run_scraper(csv_file, file_name):
    scraper_for_tables(csv_file, file_name)

if __name__ == "__main__":
    file_names = ["shatonela_1.csv", "shatonela_2.csv", "shatonela_3.csv", "shatonela_4.csv", "shatonela_5.csv"]
    csv_files = ["shatonela_data_1.csv", "shatonela_data_2.csv", "shatonela_data_3.csv", "shatonela_data_4.csv", "shatonela_data_5.csv"]

    processes = []

    for i in range(len(file_names)):
        process = Process(target=run_scraper, args=(csv_files[i], file_names[i]))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    concatenated_df = pd.DataFrame()
    for file in file_names:
        df = pd.read_csv(file)
        concatenated_df = pd.concat([concatenated_df, df], ignore_index=True)
    concatenated_df.to_csv("concatenated_data.csv", index=False)
    print("All processes have finished.")