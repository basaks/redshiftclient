import pandas as pd
import client

# initialize
rsclient = client.RSClient('/path/to/creds.json')

# connect to redshift
rsclient.connect()


df = pd.read_sql("select * from table_name limit 10", rsclient.conn)
