import os

def parse_name(table_name):
    name = table_name.strip()
    name = name.replace('#','')
    name = name.replace('_','')
    name = name.replace(' ','')
    name = name.strip('[')
    name = name.strip(']')
    name = name.replace('.csv', '')
    return name

def clean_directory(directory):
    for file in os.listdir(directory):
        filepath = os.path.join(directory, file)
        os.remove(filepath)
        
use_constraints = False
use_schema = False
use_fks = False
use_data = True