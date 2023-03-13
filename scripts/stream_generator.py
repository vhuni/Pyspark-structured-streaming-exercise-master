import os
import pandas as pd
from time import sleep, gmtime, strftime
from random import randint
from itertools import cycle
import glob

NUM_FILES = 20
DELAY_SECONDS = 1
COUNTS_PER_FILE = 2

data_path = os.path.join(os.getcwd(), "..", "data")
streamtest_path = os.path.join(data_path, "streamtest")

loading_spinner_symbols = ['/','-','\\','|']
spinner = cycle(loading_spinner_symbols)

print("generating...")

for i in range(NUM_FILES):
    randomness_scaler = 1 - abs((i/NUM_FILES) - 0.5)
    df = pd.DataFrame({
        "data_value" : [randint(500, 1000) * randomness_scaler for j in range(COUNTS_PER_FILE)],
        "timestamp" : [strftime("%Y-%m-%d %H:%M:%S", gmtime()) for j in range(COUNTS_PER_FILE)]
    })

    df.to_csv(os.path.join(data_path, f"streamfile{i}.csv"), index=False)

    # join data inside streamfile csv with new data from new streamfile csv
    os.replace(os.path.join(data_path, f"streamfile{i}.csv"), os.path.join(streamtest_path,f"streamfile{i}.csv"))
    print(f"{next(spinner)}", end='\r')
    sleep(DELAY_SECONDS)

print("done!")

# path = 'C:/Users/BoneyLabinghisa/Desktop/xander_work/Pyspark-structured-streaming-exercise-master/data/streamtest/*.csv'
# csv_files = glob.glob(path)

# df_append = pd.DataFrame()
# #append all files together
# for file in csv_files:
#             df_temp = pd.read_csv(file)
#             df_append = df_append.append(df_temp, ignore_index=True)
# print('df_append: ', df_append)

# #save to csv
# df_append.to_csv('C:/Users/BoneyLabinghisa/Desktop/xander_work/Pyspark-structured-streaming-exercise-master/data/streamtest/streamfile.csv', index=False)