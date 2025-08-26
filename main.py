import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


# DB_USER = 'DB_USER'
# DB_PASSWORD = 'DB_PASSWORD'
# DB = 'DB_1'
# engine = create_engine(
#         f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@172.23.0.2:5432/{DB}')
code = 'RELIANCE.NS'
comp_data = yf.Ticker(code)
df = pd.DataFrame(comp_data.history(start = '2020-08-01', end = '2025-08-01'))
df['stock_code'] = code
df['company'] = 'Reliance Industries'
df.reset_index(inplace=True)
print(df)
# connection = engine.connect()
# print(df)
# df.to_sql(f'{table_name}', engine, if_exists='append',index=False)
# connection.execute(text(f'ALTER TABLE {table_name} ADD FOREIGN KEY (company_id) REFERENCES comp_data(company_id);'))
# connection.commit()
# connection.close()
# print(f'Table:{table_name} pushed to postgres')