import logging
import json
import sqlalchemy
from sqlalchemy.engine.url import URL

log = logging.getLogger(__name__)


class RSClient:

    def __init__(self, creds_json: str) -> None:
        with open(creds_json) as f:
            creds = json.load(f)
            f.close()
        # Create connection object
        self.conn_url = URL(
            drivername="postgresql"
            , username=creds["username"]
            , password=creds["password"]
            , host=creds["host"]
            , port=creds["port"]
            , database=creds["database"]
        )
        self.conn = None

    def connect(self) -> None:
        if self.conn is None:
            log.info("Creating Redshift connection object")
            self.conn = sqlalchemy.create_engine(self.conn_url).connect()
            log.info("Redshift connection object created")
        else:
            log.info("Already connected")

    def close(self) -> None:

        if self.conn is None:
            raise ConnectionError("not connected. Connect First")
        else:
            self.conn.close()
            log.info("Closed Redshift Connection")
