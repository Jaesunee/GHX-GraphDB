from connectSnowflake import pull_data_snowflake
import pandas as pd
import csv

qry = "select MASTER_ID as ID, PRIMARY_EID, SOURCE_ID, SRC_ORGANIZATION_NAME from CDP_PRD.ataccama.party_instance where ID = 59083786;"

df = pull_data_snowflake(qry)

print(df.head())
print(df.size)