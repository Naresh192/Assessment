import sqlite3
import json
from sqlalchemy import create_engine


def table_exists(conn,table_name) :
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    table_exists = cursor.fetchone()
    return table_exists
    

def load_data(df,conn,table_schema,inc_column,group_by) :

    #splitting data based on country
    grouped = df.groupby(group_by)
    dfs = {country: data for country, data in grouped}
    for country in dfs :
        column_list=[]
        primary_keys=[]
        table_name=country
        df=dfs[country]
        cursor = conn.cursor()
        
        #creating country specific tables

        #### There are limitations in SQLite which restricts us in created derived columns based on date time columns because I have skipped adding derived columns. Otherwise I have knowledgeof creating Derived Columns
        
        create_table_query = f'CREATE TABLE {table_name} ('
        for column in json.loads(table_schema):
            column_list.append(column['Column_Name'].replace(' ','_'))
            create_table_query += f"{column['Column_Name'].replace(' ','_')} {column['Data Type']}({column['Field Length']})"
            if column['Mandatory'] == 'Y':
                create_table_query += ' NOT NULL'
            if column['Key Column'] == 'Y':
                primary_keys.append(column['Column_Name'].replace(' ','_'))
                create_table_query += ' PRIMARY KEY'
            create_table_query += ', '
        create_table_query = create_table_query.rstrip(', ') + ')'
        set_clause = ", ".join([f'{col} = {table_name}_Stage.{col}' for col in column_list if col not in primary_keys])
        if table_exists(conn,table_name) :
            print(f"Table {table_name} already exists")
        else :
            cursor.execute(create_table_query)
            cursor.execute(create_table_query.replace(table_name,table_name+'_Stage'))
            print(f"Table '{table_name}' created.")

        #copying data into country specific stage table
        df = df.sort_values(by=[inc_column], ascending=False).groupby(level=0).first()
        df.columns=column_list
        df.to_sql(table_name+'_Stage',conn, if_exists='replace' , index=False)

        #Merging Stage to core
        update_query=f'''UPDATE {table_name} set {set_clause} from {table_name}_Stage where {" AND ".join([f'{table_name}.{pk} = {table_name}_Stage.{pk}' for pk in primary_keys])}'''
        cursor.execute(update_query)
        insert_query=f'''INSERT INTO {table_name} ({', '.join(column_list)}) select {', '.join(column_list)} from {table_name}_Stage WHERE NOT EXISTS ( SELECT 1 FROM {table_name} where {" AND ".join([f'{table_name}.{pk} = {table_name}_Stage.{pk}' for pk in primary_keys])});'''
        cursor.execute(insert_query)
        conn.commit()
    cursor.close()


    
