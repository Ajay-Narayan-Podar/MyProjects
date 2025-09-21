import requests
import json
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.exc import SQLAlchemyError

url = 'https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true'
res = requests.get(url)

if res.status_code == 200:
    data = res.json()
    with open('weather_data.json', 'w') as f:
        json.dump(data, f, indent=4)

    df = json_normalize(data, sep='_') #gives separate columns
    print(df.head())

    # Connect to PostgreSQL
    try:
        engine = create_engine('postgresql+psycopg2://admin:admin@localhost:5433/testdb')
         
        # Test the connection
        with engine.connect() as connection:
            print("Database connection successful!")
        
        # Write DataFrame to PostgreSQL
        df.to_sql('myflow_table', engine, if_exists='replace', index=False)
        print('Database loaded successfully')
        
    except SQLAlchemyError as e:
        print('SQLAlchemy Error:', e)
    except Exception as e:
        print('General Error:', e)

else:
    print(f'Failed to access the data: {res.status_code}')