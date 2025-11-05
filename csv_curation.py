#this program should take unreadable cells in a csv and replace them will NULL and then write that back to the original csv
import csv

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

#this one is still in development and not meant to be operable yet
def csvEncodeReplacement(filepath):
    out_data = []
    with open(filepath, 'r', newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)
        data = list(csv_reader)
        columns = list(zip(*data))
        '''for i, col_data in enumerate(columns):
            column_name = header[i]
            has_int = False
            for cell in col_data:
                if has_int == False and i'''
    with open('data/ton_iot/test1.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(out_data)
    print("All Done")
    
    return
csvNullReplacement("data/ton_iot/Train_Test_Windows_10.csv")

