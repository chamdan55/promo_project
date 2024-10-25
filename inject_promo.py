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
        print('{} process took {}s'.format(func.__name__,t_total))
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

def insert_to_masteruser():
    # FREEPDB1.JABAR.
    sql_insert = """
    INSERT INTO MASTERUSER
    (USER_ID, GENDER, RELIGION, SEGMENTATION, TOTAL_ASSETS, ACCOUNT_AGE, TRANSACTION_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, :7)
    """
    return sql_insert

def insert_to_mastermerchant():
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = """
    INSERT INTO MASTERMERCHANT
    (mcc_group, MERCHANT_CATEGORY)
    VALUES (:1, :2)
    """
    return sql_insert

def insert_to_masterpromo():
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = """
    INSERT INTO MASTERPROMO
    (PROMO_ID, MCC_GROUP, MERCHANT_CATEGORY, GENDER, RELIGION, PROMO_LOCATION, START_DATE, END_DATE, SEGMENTATION, TOTAL_ASSETS, ACCOUNT_AGE, TRANSACTION_TIME)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)
    """
    return sql_insert

def insert_to_transactiondma():
    # SQL command to insert data
    # FREEPDB1.JABAR.
    sql_insert = """
    INSERT INTO TRANSACTIONDMA
    (USER_ID, MCC_GROUP, MERCHANT_CATEGORY, RANGKING, PROMO_LOCATION)
    VALUES (:1, :2, :3, :4, :5)
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
    
        if len(df) <= 10000:
            datas = get_data(df)
        else:
            datas = list(df.values)
        
        if table.upper()=='MASTERUSER':
            sql_insert= insert_to_masteruser()
        elif table.upper()=='MASTERMERCHANT':
            sql_insert= insert_to_mastermerchant()
        elif table.upper()=='MASTERPROMO':
            sql_insert= insert_to_masterpromo()
        elif table.upper()=='TRANSACTIONDMA':
            sql_insert= insert_to_transactiondma()
        
    
        print(f'Inserting {len(df)} data to {table} ... ')
        count = 1
        batch_size = 10000
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