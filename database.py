import psycopg2 as pg
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor, RealDictRow, execute_values 
from typing import Any
from config import settings


# Connection parameters.
connection_params: dict[str, Any] = {
    # 'database': 'souglobal',
    'database': 'setn',
    'password': 'arju@123',
    'user': 'postgres', 
    'cursor_factory': RealDictCursor   
}

connection_params_: dict = {
    'database': settings.db_name,
    'password': settings.db_password,
    'user': settings.db_username, 
    'cursor_factory': RealDictCursor,
    'host': settings.db_host,
    'port': settings.db_port

}

# Create a database pool to create and store the connections.
db: SimpleConnectionPool = SimpleConnectionPool(
    minconn = 4, maxconn = 12, **connection_params_
)



def execute_sql_select_statement(
        sql: str, vars: dict | list | None = None,
        fetch_all: bool = True
) -> list[RealDictRow] | RealDictRow | None:
    
    # Get a connection from the pool. 
    conn: pg.extensions.connection = db.getconn()
    # Get a cursor object.
    cur: pg.extensions.cursor = conn.cursor()
    cur.execute(sql, vars = vars) # Execute the select statement.

    if fetch_all: 
        result: list[RealDictRow] | None = cur.fetchall() # Fetch all records.
    else:
        result: RealDictRow | None = cur.fetchone() # Fetch single record. 
    
    db.putconn(conn) # Release the connection back to the pool. 
    cur.close()

    return result




def execute_sql_commands(
        sql: str, vars: list | dict | None = None,  
        fetch: bool = False
) -> (RealDictRow | None):
    
    # Get the database connection. 
    conn: pg.extensions.connection = db.getconn()
    # Get the cursor object.
    cur = conn.cursor()
    # Execute the sql command/statemnt.
    cur.execute(sql, vars)
    # Commit the changes.
    conn.commit()
    # Release the connection back to the pool.
    db.putconn(conn)
    # if fetch = True, Need to fetch the record from the cursor.
    record = None
    if fetch:
        record: RealDictRow = cur.fetchone()
    cur.close()
    
    return record



table_schema: str = """

        create table if not exists beneficiaries(
            full_name varchar not null,
            aadhar_num varchar(12) not null,
            email_id varchar(100) not null unique,
            phone_num varchar,
            application_num integer,
            list int
        );


        create table if not exists bank_details(
            email_id varchar references beneficiaries(email_id) on delete cascade,  
            bank_name varchar not null,
            account_holder_name varchar not null,
            account_num varchar not null unique,
            ifsc_code varchar not null,
            created_at timestamp default current_timestamp  
        )
"""

def create_tables():
    for sql in table_schema.split(';'):
        if sql.strip():
            execute_sql_commands(sql.strip())
    
            print(f"Executed: {sql.strip()}")
    print("Database tables created successfully.")



create_tables()

def drop_tables():
    drop_sql: str = """
        drop table if exists beneficiaries cascade;
        drop table if exists bank_details cascade;
    """
    for sql in drop_sql.split(';'):
        if sql.strip():
            execute_sql_commands(sql.strip())
            print(f"Executed: {sql.strip()}")
    print("Database tables dropped successfully.")



if __name__ == "__main__":
    drop_tables()
    # create_tables()