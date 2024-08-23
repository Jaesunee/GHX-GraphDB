from connectSnowflake import pull_data_snowflake
import pandas as pd
import csv
import math

# Initialize lists to store the dataframes, names, and relevant ids
node_dfs = []
node_names = []
edge_dfs = []
edge_names = []
edge_labels = []
relevant_ids1 = []
relevant_ids2 = []

# Define the parameters for pulling data from Snowflake
parameters = [
    ("PARTY_MASTER_ID, PRODUCT_MASTER_ID", "party_id, man_item_id", "ataccama.party2manprod", "party2man_item.csv", "edges", "manufactures"),
    ("PARTY_MASTER_ID, PRODUCT_MASTER_ID", "party_id, sup_item_id", "ataccama.party2supprod", "party2sup_item.csv", "edges", "supplies"),
    ("PARENT_MASTER_ID, CHILD_MASTER_ID", "man_item_id, sup_item_id", "ataccama.rel_prod2prod", "man_item2sup_item.csv", "edges", "is"),
    ("pi.master_id, p.id", "party_instance_id, party_id", "ataccama.party_instance AS pi JOIN CDP_PRD.ataccama.party AS p ON pi.master_id = p.id", "party_instance2party.csv", "edges", "isSameParty"),
    ("distinct hash(GSIK,poh.CDP__PRODUCER_NAME,EXTRACT(year from PO_DATE),EXTRACT(month from PO_DATE)) as transaction_id, poh.CDP__PRODUCER_NAME",
     "transaction_id, primary_eid",
     "GHX_DATA.PURCHASE_ORDER_HISTORY poh join CDP_PRD.GHX_DATA.item_catalog_api_post_match_results cat on cat.cdp__cat_match_key= poh.cdp__cat_match_key where extract(year from PO_DATE) >= 2023 and extract(month from PO_DATE) >= 6 and GSIK is not null",
     "transaction2party_instance.csv",
     "edges",
     "purchaser"),
    ("distinct hash(GSIK, poh.CDP__PRODUCER_NAME, EXTRACT(year from PO_DATE), EXTRACT(month from PO_DATE)) as transaction_id, GSIK",
     "transaction_id, sup_item_id",
     "GHX_DATA.PURCHASE_ORDER_HISTORY poh join CDP_PRD.GHX_DATA.item_catalog_api_post_match_results cat on cat.cdp__cat_match_key= poh.cdp__cat_match_key where extract(year from PO_DATE) >= 2023 and extract(month from PO_DATE) >= 6 and GSIK is not null",
     "transaction2sup_item.csv",
     "edges",
     "item_purchased"),
    ("PARTY_MASTER_ID, PRODUCT_MASTER_ID", "party_id, sup_item_id", "ataccama.party2supprod", "party2sup_item.csv", "informed_edges", [1, 5]),
    ("pi.master_id, p.id", "party_instance_id, party_id", "ataccama.party_instance AS pi JOIN CDP_PRD.ataccama.party AS p ON pi.master_id = p.id", "party_instance2party.csv", "informed_edges", [3, 6]),
    ("ID, CMO_SOURCE, CMO_ORGANIZATION_NAME, CMO_SUB_TYPE", "party_id, source, organization, subtype", "ataccama.party", "party.csv", "nodes", [0, 1, 3, 6, 7]),
    ("MASTER_ID, PRIMARY_EID, SOURCE_ID, SRC_ORGANIZATION_NAME", "party_instance_id, primary_eid, source, source_organization", "ataccama.party_instance", "party_instance.csv", "nodes", [[3,7], [4]]),
    ("ID, CMO_SOURCE, CMO_PART_NUMBER, CMO_DESCRIPTION_VALUE, CMO_ORG_NAME", "man_item_id, source, part number, description, organization", "ataccama.man_item", "man_item.csv", "nodes", [0, 2]),
    ("ID, CMO_SOURCE, CMO_PART_NUMBER, CMO_DESCRIPTION_VALUE", "sup_item_id, source, part_number, description","ataccama.sup_item","sup_item.csv", "nodes", [1,2,5,6]),
    ("distinct hash(GSIK, poh.CDP__PRODUCER_NAME,EXTRACT(year from PO_DATE),EXTRACT(month from PO_DATE)) as transaction_id, count(*) as transaction_cnt, SUM(EXTENDED_PRICE)",
     "transaction_id, transaction_count, sum_price",
     "GHX_DATA.PURCHASE_ORDER_HISTORY poh join CDP_PRD.GHX_DATA.item_catalog_api_post_match_results cat on cat.cdp__cat_match_key= poh.cdp__cat_match_key where extract(year from PO_DATE) >= 2023 and extract(month from PO_DATE) >= 6 and GSIK is not null",
     "transaction.csv",
     "nodes",
     [4, 5]),
]

# Function to check if a string is a number
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
    
# Function to return the dataframes
def return_dfs():
    return node_dfs, edge_dfs, node_names, edge_names

# Pull data from Snowflake for each parameter set
# TODO: make a batch processing function for readability

for i, (selection, name, table_name, output_file, type, relationship) in enumerate(parameters):
    print("pulling data for " + name)
    qry = f"select {selection} from CDP_PRD.{table_name}"
    df = pd.DataFrame()
    parts = name.split(", ")
    batch_size = 900
    relevant_ids1, relevant_ids2 = [], []
    if type == "nodes" and relationship != "":
        if name.split(",")[0] == "party_instance_id":
            for i in relationship[0]:
                relevant_ids1 += edge_dfs[i][parts[0]].tolist()
            for i in relationship[1]:
                relevant_ids2 += edge_dfs[i][parts[1].strip()].tolist()

            num_batches1 = math.ceil(len(relevant_ids1) / batch_size)
            num_batches2 = math.ceil(len(relevant_ids2) / batch_size)

            for i in range(num_batches1):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(relevant_ids1))
                batch_ids1 = relevant_ids1[start_idx:end_idx]
                batch_ids1 = [id for id in batch_ids1 if str(id).isdigit() or is_number(str(id))]
                in_clause = ','.join(str(id) for id in batch_ids1)
                batch_qry = qry + f" WHERE MASTER_ID IN ({in_clause}) AND TRY_TO_NUMBER(PRIMARY_EID) IS NOT NULL;"
                batch_df = pull_data_snowflake(batch_qry)
                
                if name != "":
                    new_column_names = dict(zip(batch_df.columns, name.split(", ")))
                    batch_df = batch_df.rename(columns=new_column_names)
                batch_df = batch_df.drop_duplicates()
                batch_df.drop_duplicates(subset=[parts[0]], keep='first', inplace=True)
                print("batch #" + str(i) + " for " + parts[0])
                df = pd.concat([df, batch_df], ignore_index=True)
            for j in range(num_batches2):
                # Calculate the start and end index for the current batch
                start_idx = j * batch_size
                end_idx = min((j + 1) * batch_size, len(relevant_ids2))

                # Extract the relevant IDs for the current batch
                batch_ids2 = relevant_ids2[start_idx:end_idx]

                # Filter out non-numeric IDs
                batch_ids2 = [id for id in batch_ids2 if str(id).isdigit() or is_number(str(id))]

                in_clause = ','.join(str(id) for id in batch_ids2)

                # Construct the query with the IN clause for the current batch
                batch_qry = qry + f" WHERE TRY_CAST(PRIMARY_EID AS INT) IN ({in_clause}) AND TRY_TO_NUMBER(PRIMARY_EID) IS NOT NULL;"
                
                # Execute the query for the current batch
                batch_df = pull_data_snowflake(batch_qry)
                
                if name != "":
                    new_column_names = dict(zip(batch_df.columns, name.split(", ")))
                    batch_df = batch_df.rename(columns=new_column_names)
                batch_df = batch_df.drop_duplicates()
                batch_df.drop_duplicates(subset=[parts[1]], keep='first', inplace=True)

                # Process the results for the current batch (e.g., append to node_dfs)
                print("batch #" + str(j) + " for " + parts[1])
                df = pd.concat([df, batch_df], ignore_index=True)
        else:
            for i in relationship:
                relevant_ids1 += edge_dfs[i][parts[0]].tolist()
                #relevant_ids.update(edge_dfs[i][parts[0]].tolist()) 
                relevant_ids1 = list(set(relevant_ids1))

            # Calculate the number of batches needed
            num_batches = math.ceil(len(relevant_ids1) / batch_size)
            
            # Iterate over the relevant IDs in batches
            for i in range(num_batches):
                print("processing batch #" + str(i) + " for " + name)
                start_idx = i * batch_size
                end_idx = (i + 1) * batch_size
                batch_ids = list(relevant_ids1)[start_idx:end_idx]
                batch_ids = [id for id in batch_ids if str(id).isdigit() or is_number(str(id))]
                in_clause = ','.join(str(id) for id in batch_ids)

                if output_file == "transaction.csv":
                    batch_qry = qry + f" AND transaction_id IN ({in_clause}) group by 1 LIMIT 100000;"
                else:
                    batch_qry = qry + f" WHERE ID IN ({in_clause}) limit 100000;"
                
                batch_df = pull_data_snowflake(batch_qry)
                if name != "":
                    new_column_names = dict(zip(batch_df.columns, name.split(", ")))
                    batch_df = batch_df.rename(columns=new_column_names)
                batch_df = batch_df.drop_duplicates()
                batch_df.drop_duplicates(subset=[parts[0]], keep='first', inplace=True)
                df = pd.concat([df, batch_df], ignore_index=True)
    elif type == "informed_edges":
        relevant_ids1 += edge_dfs[relationship[1]][parts[1]].tolist() 
        relevant_ids1 = list(set(relevant_ids1))
        num_batches = math.ceil(len(relevant_ids1) / batch_size)
        
        # Iterate over the relevant IDs in batches
        for i in range(num_batches):
            print("batch #" + str(i) + " for " + name)

            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            batch_ids = list(relevant_ids1)[start_idx:end_idx]
            batch_ids = [id for id in batch_ids if str(id).isdigit() or is_number(str(id))]
            in_clause = ','.join(str(id) for id in batch_ids)

            # Construct the query with the IN clause for the current batch
            batch_qry = qry + f" WHERE {selection.split(', ')[1]} IN ({in_clause}) LIMIT 100000;"
            
            # Execute the query for the current batch
            batch_df = pull_data_snowflake(batch_qry)

            # process results and add to the main df
            if name != "":
                new_column_names = dict(zip(batch_df.columns, name.split(", ")))
                batch_df = batch_df.rename(columns=new_column_names)
            batch_df = batch_df.drop_duplicates()
            batch_df.drop_duplicates(subset=[parts[1]], keep='first', inplace=True)
            df = pd.concat([df, batch_df], ignore_index=True)
    else:
        qry += " LIMIT 100000;"
        df = pull_data_snowflake(qry)
            
    if name != "":
        new_column_names = dict(zip(df.columns, name.split(", ")))
        df = df.rename(columns=new_column_names)
    
    print("preview:", df.head())

    # replace null values with None string
    df = df.fillna(value="None")  

    # remove duplicates 
    df = df.drop_duplicates()
    df.drop_duplicates(subset=[parts[0]], keep='first', inplace=True)
    
    dataCSV = df.to_csv(f"/home/ubuntu/GHX-Capstone-Code/data/pulled-CSV-Data/{output_file}")

    # populate dataframes with appropriate data
    if type == "nodes":
        node_dfs.append(df)
        if '.' in table_name or ',' in table_name or '_' in table_name:
            node_name = output_file[:-4]  # Remove the last 4 characters from output_file
        else:
            node_name = table_name
        node_names.append(str(node_name))
    elif type == "informed_edges":
        edge_names.append(parameters[relationship[0]][-1])
        edge_dfs.append(df)
    else:
        edge_names.append(relationship)
        edge_dfs.append(df)