from money.framework.application import GraphQLApplication
from money.framework.storage import MemoryStorage

APP = GraphQLApplication(storage=MemoryStorage())
