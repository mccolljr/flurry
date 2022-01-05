from money.framework.application import GraphQLApplication
from money.framework.storage import SqliteStorage

APP = GraphQLApplication(storage=SqliteStorage(db_name="app.db"))
