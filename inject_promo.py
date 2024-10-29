import oracledb
import pandas as pd
import time
import sys

def timer(func):
    """A decorator that prints how long a function took to run.
    """
    # Define the wrapper function to return.
    def wrapper(*args, **kwargs):
        # When wrapper() is called, get the current time.
        t_start = time.time()
        # Call the decorated function and store the result.
        result = func(*args, **kwargs)
        # Get the total time it took to run, and print it.
        t_total = time.time() - t_start

        # Convert to hours, minutes, and seconds
        hours, remainder = divmod(t_total, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Format as hh:mm:ss.fff
        formatted_time = f"{int(hours):02}:{int(minutes):02}:{seconds:06.3f}"

        print('{} process took {}'.format(func.__name__,formatted_time))
        return result
    return wrapper


def get_connection(user, password, dsn):#, encoding, nencoding, timezone):
    try:
        return oracledb.connect(user=user, password=password, dsn=dsn)#, encoding=encoding, nencoding=nencoding,timezone=timezone)
    except oracledb.Error as e:
        print(f"Error connecting to Oracle: {e}")
        return None

@timer
def get_arguments():
    # Access the arguments
    args = []
    for i, arg in enumerate(sys.argv[1:], start=1):
        args.append(arg)
    table = args[0]
    df = pd.read_csv(args[1])
    return table, df

@timer
def get_data(df):
    try:
        datas = []
        for index, row in df.iterrows():
            data = []
            for column in df.columns:
                data.append(row[column] if pd.notnull(row[column]) else None)
            datas.append(data)
        return datas
    except Exception as e:
        print('Error when getting the data')
        print(f'An Error occured: {e}')

def insert_to_userproperties(table):
    # FREEPDB1.JABAR.
    sql_insert = f"""
    INSERT INTO {table}
    (ID, USER_ID, GENDER, RELIGION, SEGMENTATION, TOTAL_ASSETS, ACCOUNT_AGE, TRANSACTION_TIME, UPDATE_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, :7, TO_TIMESTAMP_TZ(:8, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :9, TO_TIMESTAMP_TZ(:10, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :11, TO_TIMESTAMP_TZ(:12, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))
    """
    return sql_insert



def insert_to_promoseg():
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = """
    INSERT INTO PROMO_SEGMENTATION
    (ID, SEGMENTATION, CATEGORY, SCORE, UPDATE_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME)
    VALUES (:1, :2, :3, :4, :5, TO_TIMESTAMP_TZ(:6, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :7, TO_TIMESTAMP_TZ(:8, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))
    """
    print(sql_insert)
    return sql_insert

def insert_to_promobrand():
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = """
    INSERT INTO PROMO_BRANDING
    (ID, USER_ID, BRAND, CATEGORY, SCORE, UPDATE_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, TO_TIMESTAMP_TZ(:7, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :8, TO_TIMESTAMP_TZ(:9, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))
    """
    return sql_insert

def insert_to_masterpromo_2():
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = """
    INSERT INTO MASTER_PROMO
    (ID, PROMO_ID, BRAND, CATEGORY, RELIGION, GENDER, SEGMENTATION, PROMO_LOCATION, TOTAL_ASSETS, ACCOUNT_AGE, TRANSACTION_TIME, START_DATE, END_DATE, UPDATE_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, TO_TIMESTAMP_TZ(:11, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :12, :13, :14, TO_TIMESTAMP_TZ(:15, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :16, TO_TIMESTAMP_TZ(:17, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))
    """
    return sql_insert

def insert_to_transactiondma():
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = """
    INSERT INTO TRANSACTION_DMA
    (ID, USER_ID, MCC_GROUP, MERCHANT_CATEGORY, RANKING, UPDATE_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME, PROMO_LOCATION)
    VALUES (:1, :2, :3, :4, :5, :6, TO_TIMESTAMP_TZ(:7, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :8, TO_TIMESTAMP_TZ(:9, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :10)
    """
    return sql_insert

@timer
def main():
    db_username = "JABAR"
    db_password = "oracleApri"
    host = "103.127.137.244"
    port = "1521"
    service_name = "FREEPDB1"

    # Construct the DSN
    dsn = oracledb.makedsn(host=host, port=port, service_name=service_name)

    try:
        connection = get_connection(db_username, db_password, dsn)
        print("Connection Ready")
    
        # Create a cursor
        cursor = connection.cursor()
        cursor.setinputsizes(int, int)     # or cur.setinputsizes(oracledb.DB_TYPE_NUMBER, oracledb.DB_TYPE_NUMBER)
    
        table, df = get_arguments()
    
        # if len(df) <= 10000:
        #     datas = get_data(df)
        # else:
        #     datas = list(df.values)
        datas = list(df.values)

        if table.upper()=='USER_PROPERTIES':
            sql_insert= insert_to_userproperties(table)
        elif table.upper()=='PROMO_SEGMENTATION':
            sql_insert= insert_to_promoseg()
        elif table.upper()=='PROMO_BRANDING':
            sql_insert= insert_to_promobrand()
        elif table.upper()=='MASTER_PROMO':
            sql_insert= insert_to_masterpromo_2()
        # elif table.upper()=='TRANSACTION_DMA':
        #     sql_insert= insert_to_transactiondma()
        else :
            print(f"There's no {table} table")
            exit()
        
        
        print(f'Inserting {len(df)} data to {table} ... ')
        count = 1
        batch_size = 10000
        batches = int(len(datas)/batch_size) if int(len(datas)/batch_size) > 0 else 1
        print(f"Making {batches} batch(s) before inserting ... ")

        for i in range(0, len(datas), batch_size):
            # Create a cursor and execute the query
            cursor = connection.cursor()
            batch = datas[i:i + batch_size]
            cursor.executemany(sql_insert, batch)
            connection.commit()
            print(f'Success inject data at batch: {count}')
            count += 1

        cursor.close()
        connection.close()
    except Exception as e:
        print(f'An Error occured: {e}')
        if connection: connection.close()

## MAIN ##
if __name__ == "__main__":
    main()