from sqlalchemy import create_engine, text
import pandas as pd
DB_USER = 'concourse_user'
DB_PASSWORD = 'concourse_pass'
DB = 'concourse'
engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@172.23.0.2:5432/{DB}')
df = pd.read_sql_table('con_raw_data',engine)
print(df.info())
