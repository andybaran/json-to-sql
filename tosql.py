import pandas as pd
import json
import os
import glob
import gc
from sqlalchemy import engine
# Create SQL connection
dbengine = engine.create_engine('postgresql://localhost:5432/postgres')
myconnection = dbengine.connect()

# Get a list of JSON files in the current directory
os.chdir('./')
filelist = glob.glob('*.{}'.format('json'))
print("Found for processing\n",filelist, "\n")

# Import files to a dict
# Flatten that dict to a dataframe
# Push the dataframe to SQL
# Delete the DF and do some garbage collection just in case it's giant
for file in filelist:
    with open(file) as rawjson:
        print("Opening ", file)
        rawjson = json.load(rawjson)
    print("Flattening ", file)    
    flattened  = pd.json_normalize(rawjson)
    print("Appending ", file)
    flattened.to_sql('nomad_logs',myconnection,if_exists='append')
    del rawjson
    del flattened
    gc.collect()
    print("Finished processing ", file, "\n")