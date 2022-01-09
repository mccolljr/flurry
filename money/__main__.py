import os
import sys
import atexit
import logging

from .app import APP

APP.register_modules(
    "money.user",
)


def main():
    import sys

    if "--write-schema" in sys.argv or "--write-schema-only" in sys.argv:
        write_schema()
    if "--write-schema-only" in sys.argv:
        return
    APP.run(
        host=os.environ.get("HOST", "localhost"),
        port=int(os.environ.get("PORT", "80")),
    )


def write_schema():
    with open("./schema.gql", "w", encoding="utf-8") as schema_file:
        schema_file.write(str(APP.gql_schema))


if __name__ == "__main__":
    atexit.register(lambda: print("===== EXITING ====="))
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, force=True)
    main()
