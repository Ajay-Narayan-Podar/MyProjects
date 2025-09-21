#loading excel file into the postgres.
import pandas as pd,openpyxl,re
import psycopg2
from psycopg2.extras import execute_values

df=pd.read_excel("~/CoffeeOrdersData_.xlsx")
# print(df.columns)
def cleaned_col(col_name):
    col_name=col_name.lower()
    col_name=re.sub(r'[\s\-]+','_',col_name) #used regular expression module to substitute 
    col_name=re.sub(r'[^\w]','',col_name)
    return col_name

df.columns=[cleaned_col(c) for c in df.columns] #it changes the df column labels in the memory permanently but doesn't affect the file.
print(df)

df.duplicated().sum() #gives count of duplicated rows.
df.drop_duplicates
print(len(df)) #gives the count of df including the labels just as in excel.

conn=psycopg2.connect(host="localhost",port=5433,user="admin",password="admin",dbname="testdb")

cur=conn.cursor()

query=f"Insert into coffeeorders ({",".join(df.columns)}) values %s"

values=[tuple(x) for x in df.to_numpy()] #it removes the header of the dataset & gives 2d-ndarray.
execute_values(cur,query,values)

conn.commit()
cur.close()
conn.close()
print("Data is loaded successfully into the database")