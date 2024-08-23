from aws_funcs import get_ssm_param 
import urllib.parse

# URL(user = snow_config['user'],
#                         password = snow_config['password'],
#                         role = snow_config['role'],
#                         account=snow_config['account'],
#                         region=snow_config['region'],
#                         warehouse=snow_config['warehouse'],
#                         database=snow_config['database'],
#                         schema = snow_config['schema'])
user = get_ssm_param('/datascience/sf/user', encrypted=True)
# user = 'SLANGE'
password = get_ssm_param('/datascience/sf/p',encrypted=True)

snow_config = {
    'user':user ,
    'password':password,
    'role' : 'AI_READONLY_USERS',
    'account':'ghx',
    'region':'us-east-1',
    'warehouse':'CONSUMER_DWH_DS'}

