import owlready2 as owl
import csv
import os
from helpers import parse_name

def load_base_ontology():
    owl.onto_path.append(f"{os.getcwd()}/ontology")
    ontology = owl.get_ontology(f'file://{os.getcwd().replace('\\','/')}/ontology/put-dataset-data-lineage.3.1.rdf')
    ontology.load(format='rdfxml')
    return ontology

def load_ontology(ttl_file):
    ontology = owl.get_ontology(ttl_file)
    ontology.load(format='rdfxml')
    return ontology

def create_graph(ontology, source_directory, use_constraints = False, use_schema = False, use_fks = False, use_data = True):
    print('Creating knowledge graph...')
    total = len(os.listdir(source_directory))
    print()
    tables = set()
    views = set()
    for i,directory in enumerate(os.listdir(source_directory)):
        print(f'Adding {directory} file {i}/{total}                                            ', end='\r')
        if directory.startswith('DataLineage'):
            continue
        if '.data.csv' in directory:
            tables.add(directory.split('.')[0])
        if '.vw.csv' in directory:
            views.add(directory.split('.')[0])
    print()
    total = len(views)
    for i,directory in enumerate(views):
        print(f'Processing {directory} view {i}/{total}                                            ', end='\r')
        with open(source_directory + '/' + directory+'.vw.csv', newline='', encoding='utf8') as csvfile:  
            viewName = parse_name(directory)
            onto_table = ontology.View(viewName)
            onto_table.label = [viewName]
            onto_table.title = [viewName]
            columns = [column.strip() for column in csvfile.readline().split(';')]
            data_types = [column.strip() for column in csvfile.readline().split(';')]
            onto_columns = []
            for coli, column in enumerate(columns):
                # dtype = data_types[coli]
                # dtype = dtype.split()[1][1:-2]
                # dtype_ind = ontology.search_one(iri="*" + dtype)
                # if dtype_ind is None:
                #     dtype_ind = ontology.DataType(dtype)
                #     dtype_ind.datatypeName = [dtype,]
                onto_column = ontology.Column(column)
                onto_column.label = [column,]
                # onto_column.hasDatatype = [dtype_ind,]
                onto_table.hasColumn.append(onto_column)
                onto_column.isColumnOf.append(onto_table)
                onto_columns.append(onto_column)
            reader = csv.reader(csvfile, delimiter=';')
            if use_data:
                    data_reader = csv.reader(csvfile, delimiter=';')      
                    rownum = 1
                    for row__ in data_reader:
                        onto_row = ontology.Row()
                        onto_row.rownum.append(rownum)
                        rownum += 1

                        for colno,value in enumerate(row__):
                            
                            onto_value = ontology.CellValue()
                            onto_value.exactValue.append(value)
                            onto_row.hasCellValue.append(onto_value)
                            onto_value.belongsToColumn.append(onto_columns[colno])
                            onto_columns[colno].hasInstance.append(onto_value)
                            # print(onto_columns[colno].hasInstance)
                        onto_table.hasRow.append(onto_row)
    print()
    total = len(tables)
    for i, directory in enumerate(tables):
        print(f'Processing {directory} table {i}/{total}                                            ', end='\r')
        tableName = parse_name(directory)
        onto_table = ontology.Table(tableName)
        onto_table.label = [tableName]
        onto_table.title = [tableName]
        onto_columns = []
        
        with open(source_directory + '/' + directory+'.schema.csv', newline='', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            col_names = {}
            for row in reader:
                [column, dtype, length, nullable, default] = row
                onto_column = ontology.Column(directory + '_' + column)
                onto_column.label.append(column)
                col_names[column] = onto_column 
                onto_table.hasColumn.append(onto_column)
                if  use_schema:
                    onto_column.isColumnOf.append(onto_table)
                    if nullable == 'NO':
                        onto_column.isNullable.append(False)
                    else:
                        onto_column.isNullable.append(True)
                    if length == 'None':
                        length = '0'
                    onto_dtype = ontology.search_one(iri="*" + dtype +f'_{length}_')
                    if onto_dtype is None:
                        if dtype == 'nvarchar':
                            onto_dtype = ontology.StringType(dtype+f'_{length}_')
                        if dtype == 'varchar':
                            onto_dtype = ontology.StringType(dtype+f'_{length}_')
                        if dtype == 'nchar':
                            onto_dtype = ontology.StringType(dtype+f'_{length}_')
                        if dtype == 'ntext':
                            onto_dtype = ontology.StringType(dtype+f'_{length}_')
                        if dtype == 'int':
                            onto_dtype = ontology.NumericType(dtype+f'_{length}_')
                        if dtype == 'real':
                            onto_dtype = ontology.NumericType(dtype+f'_{length}_')
                        if dtype == 'money':
                            onto_dtype = ontology.NumericType(dtype+f'_{length}_')
                        if dtype == 'decimal':
                            onto_dtype = ontology.NumericType(dtype+f'_{length}_')
                        if dtype == 'smallint':
                            onto_dtype = ontology.NumericType(dtype+f'_{length}_')
                        if dtype == 'datetime':
                            onto_dtype = ontology.DataTimeType(dtype+f'_{length}_')
                        if dtype == 'image':
                            onto_dtype = ontology.StringType(dtype+f'_{length}_')
                        if dtype == 'bit':
                            onto_dtype = ontology.BooleanType(dtype+f'_{length}_')
                    onto_dtype.datatypeName = [dtype+f'_{length}_',]
                    onto_dtype.typeLength = [int(length)]
                    onto_column.hasDatatype = [onto_dtype]
                onto_columns.append(onto_column)
            if use_data:
               
                with open(source_directory + '/' + directory+'.data.csv', newline='', encoding='utf8') as csvfile:  
                    # columns = [column.strip() for column in csvfile.readline().split(';')]
                    csvfile.readline()
                    data_reader = csv.reader(csvfile, delimiter=';')      
                    rownum = 1
                    for row__ in data_reader:
                        onto_row = ontology.Row()
                        onto_row.rownum.append(rownum)
                        rownum += 1

                        for colno,value in enumerate(row__):
                            
                            onto_value = ontology.CellValue()
                            onto_value.exactValue.append(value)
                            onto_row.hasCellValue.append(onto_value)
                            onto_value.belongsToColumn.append(onto_columns[colno])
                            onto_columns[colno].hasInstance.append(onto_value)
                            # print(onto_columns[colno].hasInstance)
                        onto_table.hasRow.append(onto_row)
            if use_constraints:
                with open(source_directory + '/' + directory+'.constraints.csv', newline='', encoding='utf8') as csvfile:  
                    cstr_reader = csv.reader(csvfile, delimiter=';')   
                    for row__ in cstr_reader:
                        [type, name, col_name] = row__
                        column = col_names[col_name]
                        if 'PRIMARY KEY' in type:
                            constraint = ontology.PrimaryKey(directory+'_'+name)
                        if 'FOREIGN KEY' in type:
                            constraint = ontology.ForeignKey(directory+'_'+name)                    
                        if constraint:
                            column.hasConstraint.append(constraint)
                        else:
                            print(type)
            if use_fks:
                with open(source_directory + '/' + directory+'.fks.csv', newline='', encoding='utf8') as csvfile:  
                    cstr_reader = csv.reader(csvfile, delimiter=';')   
                    for row__ in cstr_reader:
                        [name, col_name, table, col_name2] = row__
                        column = col_names[col_name]
                        table_name = name.split('_')[-1]
                        onto_table = ontology.search_one(iri="*" + table_name )
                        constraint = ontology.search_one(iri="*" + directory + '_' + name )                 
                        if constraint:
                            constraint.referencesTable.append(onto_table)
                        else:
                            print(name)
                # print(col_names)
            # columns = [column.strip() for column in csvfile.readline().split(';')]
            # data_types = [column.strip() for column in csvfile.readline().split(';')]
            # onto_columns = []
            # for coli, column in enumerate(columns):
            #     dtype = data_types[coli]
            #     dtype = dtype.split()[1][1:-2]
            #     dtype_ind = ontology.search_one(iri="*" + dtype)
            #     if dtype_ind is None:
            #         dtype_ind = ontology.DataType(dtype)
            #         dtype_ind.datatypeName = [dtype,]
            #     onto_column = ontology.Column(column)
            #     onto_column.label = [column,]
            #     onto_column.hasDatatype = [dtype_ind,]
            #     onto_table.hasColumn.append(onto_column)
            #     onto_column.isColumnOf.append(onto_table)
            #     onto_columns.append(onto_column)
            # reader = csv.reader(csvfile, delimiter=';')
            # rownum = 1
            # for row in reader:
            #     onto_row = ontology.Row()
            #     onto_row.rownum.append(rownum)
            #     rownum += 1
            #     for colno,value in enumerate(row):
            #         onto_value = ontology.CellValue()
            #         onto_value.exactValue = [value]
            #         onto_row.hasCellValue.append(onto_value)
            #         onto_value.belongsToColumn.append(onto_columns[colno])
            #         onto_columns[colno].hasInstance.append(onto_value)
            #     onto_table.hasRow.append(onto_row)
    
    print('Base graph created, number of individuals: ', len(list(ontology.individuals())))
def create_lineage_structures_in_graph(ontology, source_directory):
    print('Creating lineage structures.')
    for directory in os.listdir(source_directory):
        if not directory.startswith('DataLineage.data.csv'):
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
                            PREFIX : <http://www.semanticweb.org/put/database-data-lineage#>
                            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                            SELECT ?x ?column
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
                            PREFIX : <http://www.semanticweb.org/put/database-data-lineage#>
                            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                            SELECT ?x
                            WHERE {{
                                ?x :hasCellValue ?cellValue .
                                ?cellValue :exactValue "{value2}" .
                                ?cellValue :belongsToColumn ?column .
                                ?column rdfs:label "{column2}" .
                                ?table :hasColumn ?column .
                                ?table :title "{table2}" .
                                                            }}
                            """
                print('---------------------------------')
                print(query1)
                print(query2)
                result1 = owl.default_world.sparql(query1)
                row1 = result1
                result2 = owl.default_world.sparql(query2)
                row2 = result2

                
                for r1 in row1:
                    for r2 in row2:
                        r2[0].rowDerivedFrom.append(r1[0])

    print('Complete graph created.')