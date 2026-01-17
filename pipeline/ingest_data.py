#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm




prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + '/yellow_tripdata_2021-01.csv.gz')




df.head()


df.dtypes


df.shape

len(df)



dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + '/yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates
)


# In[52]:


df.head()
len(df)


# In[53]:


df.dtypes


# In[12]:


get_ipython().system('uv add sqlalchemy')


# In[13]:


get_ipython().system('uv add psycopg2-binary')


# In[54]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[55]:


df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists = 'replace')


# In[56]:



pg_user = 'root'
pg_pass = 'root'
pg_host = 'localhost'
pg_port = '5432'
pg_db = 'ny_taxi'

engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'

url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'


df_iter = pd.read_csv(
    url,
    dtype = dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


if __name__ == '__main__':
    main()

# In[59]:


for df_chunk in df_iter:
    print(len(df_chunk))
    df_chunk.to_sql(
    name="yellow_taxi_data",
    con=engine,
    if_exists="append"
    )


# In[ ]:


# Everything after this is optional. It is not really needed to implement the following lines of codes


# In[36]:


# get_ipython().system('uv add tqdm')


# # In[ ]:





# # In[45]:


# from tqdm.auto import tqdm


# # In[ ]:





# # In[48]:


# # for df_chunk in tqdm(df_iter):
# #     df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists = 'append')

# # To add and injest those chunk of data, we could have used the following code as well:

# first = True

# for df_chunk in df_iter:

#     if first:
#         # Create table schema (no data)
#         df_chunk.head(0).to_sql(
#             name="yellow_taxi_data",
#             con=engine,
#             if_exists="replace"
#         )
#         first = False
#         print("Table created")

#     # Insert chunk
#     df_chunk.to_sql(
#         name="yellow_taxi_data",
#         con=engine,
#         if_exists="append"
#     )

#     print("Inserted:", len(df_chunk))    


# # In[ ]:





# In[ ]:





# In[ ]:




