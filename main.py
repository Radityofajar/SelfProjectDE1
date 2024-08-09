import connection
import os
import sqlparse
import pandas as pd

if __name__ == '__main__':
    #Connection data source
    conf = connection.config('marketplace_prod')
    conn, engine = connection.get_conn(conf, 'DataSource')
    cursor = conn.cursor()

    #Connection dwh
    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh, 'DWH')
    cursor_dwh = conn_dwh.cursor()

    #Get query string
    path_query = os.getcwd() + '/query/'
    query = sqlparse.format(
        open(path_query + 'query.sql', 'r').read(), strip_comments=True
    ).strip()
    dwh_design = sqlparse.format(
        open(path_query + 'dwh_design.sql', 'r').read(), strip_comments=True
    ).strip()

    try:
        #Get data
        print('[INFO] service ETL is running...')
        df = pd.read_sql(query, engine)

        #Create schema
        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()

        #Ingest data to dwh
        df.to_sql(
            'dim_orders_radit',
            engine_dwh,
            schema='public',
            if_exists='append', #replace or append
            index=False
        )

        print('[INFO] Service ETL success')

    except Exception as e:
        print('[INFO] Service ETL Error')
        print(str(e))