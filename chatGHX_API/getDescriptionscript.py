from queryScript import *
import pandas as pd

# Requirements, csv file needs to have id and company column name
# Format (csv file path, id column, name column, output csv name)
parameters = [
    ("../../data/pulled-CSV-Data/sup_item.csv", "ID", "CMO_ORG_NAME", "supplier_name_descriptions.csv"),
    ("../../data/pulled-CSV-Data/man_item.csv", "ID", "CMO_ORG_NAME", "manufacturer_name_descriptions.csv"),
    ("../../data/hospital.csv", "HOSPITALID", "HOSPITALNAME", "hospitals_name_descriptions.csv")
]

for path, id_col, name_col, output in parameters:
    df = pd.read_csv(path)

    ids = df[id_col][:5]
    names = df[name_col][:5]
    descriptions = []

    for name in names:
        descriptions.append(getQueryDescriptionFromName(name))
    data = {
        'ids': ids,
        'names': names,
        'descriptions': descriptions
    }
    final = pd.DataFrame(data)
    final.to_csv(output)