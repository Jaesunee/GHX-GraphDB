import sys

# print the original sys.path
print('Original sys.path:', sys.path)

# append a new directory to sys.path
sys.path.append('/home/ubuntu/data_pull_scripts')

# print the updated sys.path
print('Updated sys.path:', sys.path)

from pull_data import return_dfs
result = []
result = return_dfs()
node_dfs = result[0]
edge_dfs = result[1]

print(node_dfs[0].head())
edge_dfs[0].head()
