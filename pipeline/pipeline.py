import sys
import pandas as pd 
print("Printing sys.argv")
print(sys.argv)

# Remember, whatever received from terminal is alway in the form of text
# So, we have to convert it intopip integer type explicitly

day = int(sys.argv[1])
print(f"Running pipeline for day {day}")


df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")