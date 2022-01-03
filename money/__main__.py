import os
import logging

from .app import APP

APP.register_modules(
    "money.user",
)


def main(*args):
    schema = APP.gql_schema
    fd = os.open("./schema.gql", os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
    os.write(fd, bytes(str(schema), "utf-8"))
    os.close(fd)
    APP.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
