from money.framework.application import GraphqlApplication
from money.framework.storage import SqliteStorage

APP = GraphqlApplication(storage=SqliteStorage(db_name="app.db"))
