import pandas as pd
import json
import datetime
sql_to_python = {
    "INT": "int64",
    "FLOAT": "float",
    "DECIMAL": "float",
    "VARCHAR": "str",
    "CHAR": "str",
    "TEXT": "str",
    "DATE": "datetime.date",
    "DATETIME": "datetime.datetime",
    "TIMESTAMP": "datetime.datetime",
    "BOOLEAN": "bool",
    "BLOB": "bytes"
}

def transform_data(folder,row) :
    data_list = json.loads(row[-1])
    df = pd.read_csv(folder+'/'+''.join(row[:2]), sep='|', header=0)
    df = df.drop(columns=['H'])
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    date_format = row[2]
    date_format = date_format.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
    time_format = row[3]
    time_format = date_format.replace("HH", "%H").replace("MM", "%M").replace("SS", "%S").replace("TT", "%f")
    for i in range(len(df.columns)) :
        
        if data_list[i]["Data Type"] == 'DATE' :
            df.iloc[:, i] = pd.to_datetime(df.iloc[:, i], format=date_format)
        elif data_list[i]["Data Type"] == 'TIME' :
            df.iloc[:, i] = pd.to_datetime(df.iloc[:, i], format=time_format)
        else :
            df.iloc[:, i] = df.iloc[:, i].astype(sql_to_python[data_list[i]["Data Type"]])

    
    return df
def extract_data(master_file) :
    files = pd.read_csv(master_file)
    file_data=[]
    for i, row in files.iterrows() :
        data = transform_data(master_file.split('/')[0],row)
        file_data.append([data,row[-1]])
    return file_data
        
        
        
