import owlready2 as owl
import csv
import os
from helpers import parse_name

def load_base_ontology():
    owl.onto_path.append(f"{os.getcwd()}/ontology")
    ontology = owl.get_ontology(f'file://{os.getcwd().replace('\\','/')}/ontology/put-dataset-data-lineage-individuals-base.rdf')
    ontology.load(format='rdfxml')
    return ontology

def load_ontology(ttl_file):
    ontology = owl.get_ontology(ttl_file)
    ontology.load(format='rdfxml')
    return ontology

def create_graph(ontology, source_directory):
    print('Creating knowledge graph...')
    total = len(os.listdir(source_directory))
    print()
    for i,directory in enumerate(os.listdir(source_directory)):
        print(f'Processing {directory} file {i}/{total}                                            ', end='\r')
        if directory.startswith('DataLineage'):
            continue
        with open(source_directory + '/' + directory, newline='') as csvfile:
            tableName = parse_name(directory)
            onto_table = ontology.Table(tableName)
            onto_table.label = [tableName]
            onto_table.title = [tableName]
            
            columns = [column.strip() for column in csvfile.readline().split(';')]
            data_types = [column.strip() for column in csvfile.readline().split(';')]
            onto_columns = []
            for coli, column in enumerate(columns):
                dtype = data_types[coli]
                dtype = dtype.split()[1][1:-2]
                dtype_ind = ontology.search_one(iri="*" + dtype)
                if dtype_ind is None:
                    dtype_ind = ontology.DataType(dtype)
                    dtype_ind.datatypeName = [dtype,]
                onto_column = ontology.Column(column)
                onto_column.label = [column,]
                onto_column.hasDatatype = [dtype_ind,]
                onto_table.hasColumn.append(onto_column)
                onto_column.isColumnOf.append(onto_table)
                onto_columns.append(onto_column)
            reader = csv.reader(csvfile, delimiter=';')
            rownum = 1
            for row in reader:
                onto_row = ontology.Row()
                onto_row.rownum.append(rownum)
                rownum += 1
                for colno,value in enumerate(row):
                    onto_value = ontology.CellValue()
                    onto_value.exactValue = [value]
                    onto_row.hasCellValue.append(onto_value)
                    onto_value.belongsToColumn.append(onto_columns[colno])
                    onto_columns[colno].hasInstance.append(onto_value)
                onto_table.hasRow.append(onto_row)
    
    print('Base graph created, number of individuals: ', len(list(ontology.individuals())))
def create_lineage_structures_in_graph(ontology, source_directory):
    print('Creating lineage structures.')
    for directory in os.listdir(source_directory):
        if not directory.startswith('DataLineage.csv'):
            continue
        with open(source_directory + '/' + directory, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            csvfile.readline()
            csvfile.readline()
            for row in reader:
                table1 = parse_name(row[0])
                column1 = row[1]
                value1 = row[2]
                table2 = parse_name(row[3])
                column2 = row[4]
                value2 = row[5]
                if "'" in value1 or "'" in value2: continue
                query1 = f"""
                            PREFIX : <http://www.semanticweb.org/put/database-data-lineage/>
                            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                            SELECT ?cellValue 
                            WHERE {{
                                ?x :hasCellValue ?cellValue .
                                ?cellValue :exactValue "{value1}" .
                                ?cellValue :belongsToColumn ?column .
                                ?column rdfs:label "{column1}" .
                                ?table :hasColumn ?column .
                                ?table :title "{table1}" .
                                                            }}
                            """
                query2 = f"""
                            PREFIX : <http://www.semanticweb.org/put/database-data-lineage/>
                            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                            SELECT ?cellValue 
                            WHERE {{
                                ?x :hasCellValue ?cellValue .
                                ?cellValue :exactValue "{value2}" .
                                ?cellValue :belongsToColumn ?column .
                                ?column rdfs:label "{column2}" .
                                ?table :hasColumn ?column .
                                ?table :title "{table2}" .
                                                            }}
                            """
                result1 = owl.default_world.sparql(query1)
                row1 = result1
                result2 = owl.default_world.sparql(query2)
                row2 = result2
                for r1 in row1:
                    for r2 in row2:
                        r1[0].rowDerivedFrom = [r2[0]]
    print('Complete graph created.')