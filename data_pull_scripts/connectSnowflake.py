import pandas as pd
import snowflake.connector
from db_creds_config import snow_config



def connect_snowflake():
    engine = create_engine(URL(user = snow_config['user'],
                        password = snow_config['password'],
                        role = snow_config['role'],
                        account=snow_config['account'],
                        region=snow_config['region'],
                        warehouse=snow_config['warehouse']))

    print("Connected to Snowflake!")
    return engine


def pull_data_snowflake(sql):
    try:
        conn = snowflake.connector.connect(**snow_config)
        with conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=column_names)
        conn.close()
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame


def read_db_snow(sql):
    # Create SQLAlchemy engine to connect to database
    engine = connect_snowflake()
    # read from db
    df = pd.read_sql(sql, engine)
    engine.dispose()
    return df
