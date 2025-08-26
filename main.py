import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


DB_USER = 'concourse_user'
DB_PASSWORD = 'concourse_pass'
DB = 'concourse'
engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@172.23.0.2:5432/{DB}')
code = 'RELIANCE.NS'
comp_data = yf.Ticker(code)
df = pd.DataFrame(comp_data.history(start = '2020-08-01', end = '2025-08-01'))
df['stock_code'] = code
df['company'] = 'Reliance Industries'
df.reset_index(inplace=True)
print('Data fetched successfully from yfinance')
connection = engine.connect()
print(df)
df['load_dttm'] = datetime.now()
df.to_sql(f'con_raw_data', engine, if_exists='replace',index=False)
connection.commit()
connection.close()
print(f'Table:con_raw_data pushed to postgres')