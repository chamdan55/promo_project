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
    df = pd.read_csv(f'{table}.csv')
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
    (ID, GENDER_ID, RELIGION_ID, SEGMENTATION_ID, AUM_ID, ACCOUNT_AGE_ID, TRANSACTION_TIME_ID, LOCATION_ID, UPDATED_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, TO_TIMESTAMP_TZ(:10, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :11, TO_TIMESTAMP_TZ(:12, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))
    """
    return sql_insert



def insert_to_promoseg(table):
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = f"""
    INSERT INTO {table}
    (ID, SEGMENTATION_ID, CATEGORY_ID, MCC_CATEGORY_ID, SCORE, UPDATE_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, TO_TIMESTAMP_TZ(:7, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :8, TO_TIMESTAMP_TZ(:9, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))
    """
    print(sql_insert)
    return sql_insert

def insert_to_promobrand(table):
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = f"""
    INSERT INTO {table}
    (ID, USER_ID, BRAND, CATEGORY_ID, MCC_CATEGORY_ID, RANKING, UPDATED_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, :7, TO_TIMESTAMP_TZ(:8, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :9, TO_TIMESTAMP_TZ(:10, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))
    """
    return sql_insert

def insert_to_masterpromo_2(table):
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = f"""
    INSERT INTO {table}
    (ID, TITLE, DESCRIPTION, DESCRIPTION_DETAIL, TERM_CONDITION, IMAGE, IS_PRIORITY, DAY_OF_WEEK, TYPE, BRAND, CATEGORY_ID, MCC_CATEGORY_ID, RELIGION_ID, GENDER_ID, SEGMENTATION_ID, LOCATION_ID, AUM_ID, ACCOUNT_AGE_ID, TRANSACTION_TIME_ID, START_DATE, END_DATE, IS_DELETED, IS_OTHER, UPDATED_BY, UPDATED_TIME, CREATED_BY, CREATED_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, TO_TIMESTAMP_TZ(:20, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), TO_TIMESTAMP_TZ(:21, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :22, :23, :24, TO_TIMESTAMP_TZ(:25, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), :26, TO_TIMESTAMP_TZ(:27, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))
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

def update_master_promo(df):
    # SQL command to insert data
    # FREEPDB1.JABAR.
    df = df[['ID', 'TYPE']].values
    
    sql_insert = """
        UPDATE MASTER_PROMO
        SET TYPE = :2
        WHERE id = :1
    """
    return sql_insert, df

@timer
def main():
    db_username = "N_SURR_PROMO"
    db_password = "N_SURR_PROMO"
    host = "192.168.142.214"
    port = "1530"
    service_name = "PROMODEV"

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

        if table.upper()=='MASTER_USER':
            sql_insert= insert_to_userproperties(table.upper())
        elif table.upper()=='SEGMENTATION_CATEGORY_METRICS':
            sql_insert= insert_to_promoseg(table.upper())
        elif table.upper()=='USER_BRAND_METRICS':
            sql_insert= insert_to_promobrand(table.upper())
        elif table.upper()=='MASTER_PROMO':
            sql_insert= insert_to_masterpromo_2(table.upper())
        elif table.upper()=='UPDATE_PROMO':
            sql_insert, datas = update_master_promo(df)
        else :
            print(f"There's no {table} table")
            exit()
        
        
        print(f'Inserting {len(df)} data to {table} ... ')
        count = 1
        batch_size = 10000
        batches = int(len(datas)/batch_size) if int(len(datas)/batch_size) > 0 else 1
        print(f"Making {batches} batch(s) before inserting ... ")

        for i in range(0, len(datas), batch_size):
            # if i > 483:
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