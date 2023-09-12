from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from config import dbconfig

def db_connection():
    url = URL.create(
        drivername=dbconfig['drivername'],
        host=dbconfig['host'],
        port=dbconfig['port'],
        database=dbconfig['database'],
        username=dbconfig['username'],
        password=dbconfig['password']
    )

    engine = create_engine(url)
    return engine