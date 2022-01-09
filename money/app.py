from money.framework.application import GraphqlApplication
from money.framework.storage import PostgreSQLStorage

STORAGE = PostgreSQLStorage(
    host="localhost",
    port="5432",
    user="postgres",
    password="unsafe",
    database="postgres",
)

APP = GraphqlApplication(storage=STORAGE)
