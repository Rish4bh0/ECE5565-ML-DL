import csv
import pandas as pd

#this program should take unreadable cells in a csv and replace them will NULL and then write that back to the original csv
def csvNullReplacement(filepath):
    out_data = []
    with open(filepath, 'r', newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            out_data.append(row)
            for cell in row:
                if cell == '-' or cell == '' or cell == ' ':
                    out_data[csv_reader.line_num-1][row.index(cell)] = 'NULL'
    with open('data/ton_iot/test1.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(out_data)
    print("All Done")
    
    return

#this program receives a csv filepath as well as some feature/column name, then it goes through it in order and replaces the values with integers
#staying consistent on which number represents which value for that feature
#this is useful for taking string values and encoding them to integers
def encode_csv_column(filename, column_name):
    column_data = []
    saved_items = {}
    used_nums = 0
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        if column_name not in reader.fieldnames:
            print(f"Error: Column '{column_name}' not found in the CSV.")
            return

        for row in reader:
            column_data.append(row[column_name])
    
    for item in column_data:
        if item not in saved_items and item != column_name:
            saved_items[item] = used_nums
            used_nums += 1
    for i in range(len(column_data)):
        column_data[i] = saved_items[column_data[i]]
    return column_data

#replaces a specific value in a specific column with another specified value
def replace_specific(filename, column_name, target, replacement):
    column_data = []
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        if column_name not in reader.fieldnames:
            print(f"Error: Column '{column_name}' not found in the CSV.")
            return

        for row in reader:
            column_data.append(row[column_name])
    for i, item in enumerate(column_data):
        if item == target:
            column_data[i] = replacement
    data = pd.read_csv(filename)
    data[column_name] = column_data
    data.to_csv("data/CICIDS2017/cicids2017_combined_relabled2.csv")
    print("All Done")
    return

#calls the encode_csv_column code on all the features you wish to select and then saves a new copy of the modified file
def csvEncodeReplacement(filepath, encoded_features):
    #encoded features should be list such as ["attack", "flags", "category"]
    data = pd.read_csv(filepath)
    for feat in encoded_features:
        column_data = encode_csv_column(filepath, feat)
        data[feat] = column_data
    data.to_csv("data/CICIDS2017/cicids2017_combined_relabled.csv")
    print("All Done")
    
    return
#csvEncodeReplacement("data/CICIDS2017/cicids2017_combined.csv", [" Label"])