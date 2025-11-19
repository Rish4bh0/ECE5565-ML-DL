#this is where I plan to implement a program that should allow for the conversion and use of the CTU-13 dataset
import pandas as pd

def parquet_to_csv(filepath):
    df = pd.read_parquet(filepath)
    df.to_csv("data/ctu13/ctu-9.csv")
    print("All Done")
    return
#parquet_to_csv("data/ctu13/9-Neris-20110817.binetflow.parquet")

def ctu13_to_csv(filepath):
