import database
import helpers
import knowledge_graph
from config import csv_test_directory, csv_train_directory, csv_nn_directory
import os
import random
scenario_types = {
    1 : 'select',
    2 : 'transformation',
    3 : 'join'
}

def print_menu():
    print('1. Clean database.')
    print('2. Add scenario.')
    print('3. Create CSVs and save them in train directory.')
    print('4. Create CSVs and save them in test directory.')
    print('5. Create training knowledge graph.')
    print('6. Create test knowledge graph.')
    print('7. Create training file.')
    print('8. Create test file.')
    print('9. Create validation file.')
    print('10. Train model')
    print('11. Test model')
    print('12. Exit.')
def scenario_type_menu():
    print('1. Select based scenarios')
    print('2. Transformation based scenarios')
    print('3. Join based scenarios')

def create_csv_data(target_directory, test_directory = None):
    if test_directory:
        test_lineage = [line.strip() for line in open(os.path.join(test_directory,'DataLineage.csv')).readlines()]
    else:
        test_lineage = []
    helpers.clean_directory(target_directory)
    connection = database.create_connection()
    table_data = []
    skipped_lineage_entries = 0
    for table_info in database.get_all_tables_info(connection):
        table_name = table_info[2]
        db_name = '['+table_name+']' if ' ' in table_name else table_name
        onto_name = helpers.parse_name(db_name)
        file_name = onto_name + ".csv"
        table_data.append((db_name, onto_name, file_name))
    for table in table_data:
        (db_name, onto_name, file_name) = table
        table_file = open(os.path.join(target_directory,file_name), 'w+', encoding='utf8')
        for irow, entry in enumerate(database.get_all_data(connection, db_name)):
            parsed_entry = [str(cell).replace(';',',') for cell in entry]
            if db_name == 'DataLineage':
                if ";".join(parsed_entry) in test_lineage and irow>2: 
                    skipped_lineage_entries+=1
                    continue
            table_file.write(";".join(parsed_entry) + os.linesep[0])
        table_file.close()
    view_data = [] 
    print('Skipped lineage entries (has to be greater than 0 in the training set for the system to be evaluated properly): ', skipped_lineage_entries)
    for view_info in database.get_all_views_info(connection):
        view_name = view_info[2]
        if not view_name.startswith('vw'): continue
        db_name = '['+view_name+']' if ' ' in view_name else view_name
        onto_name = helpers.parse_name(db_name)
        file_name = onto_name + ".csv"
        view_data.append((db_name, onto_name, file_name))
    for view in view_data:
        (db_name, onto_name, file_name) = view
        view_file = open(os.path.join(target_directory,file_name), 'w+', encoding='utf8')
        for entry in database.get_all_data(connection, db_name):
            parsed_entry = [str(cell).replace(';','.') for cell in entry]
            view_file.write(";".join(parsed_entry) + os.linesep[0])
        view_file.close()
    connection.close()

def create_training_files(ontology, file):
    data_file = open(f'SiaILP/data/lineage/{file}.txt','w+')
    for individual in ontology.individuals():
        for prop in individual.get_properties():
            for value in prop[individual]:
                value_str = str(value)
                value_str = value_str.replace(' ','_')
                value_str = value_str.replace('put-dataset-data-lineage-individuals-base.','')
                value_str = value_str.replace('training_files\\train_kg.','')
                value_str = value_str.replace('training_files\\test_kg.','')
                rng = random.random()
                if file == 'train' and rng < 0.1:
                    data_file.write(f"{individual.name}\t{prop.python_name}\t{value_str}\n")
                elif file == 'test':# and rng < 0.6 and 'derived' in prop.python_name.lower():
                    data_file.write(f"{individual.name}\t{prop.python_name}\t{value_str}\n")
                elif file == 'valid' and 'derived' in prop.python_name.lower():
                    data_file.write(f"{individual.name}\t{prop.python_name}\t{value_str}\n")
    data_file.close()


def main():
    running = True
    while(running):
        print_menu()
        option = input("Choose an option: ")
        if option == '1':
            connection = database.create_connection()
            database.grant_permissions_to_database_directory()
            database.clean_database(connection)
            database.clear_lineage_structures(connection)
            connection.close()
        elif option == '2':
            scenario_type_menu()
            scenario_type = int(input("Choose scenario type: "))
            scenario_number = int(input("Choose scenario(1-5): "))
            connection = database.create_connection()
            database.create_scenario(connection, scenario_types[scenario_type], scenario_number)
            connection.close()
        elif option == '3':
            create_csv_data(csv_train_directory, test_directory=csv_test_directory)
        elif option == '4':
            create_csv_data(csv_test_directory)
        elif option == '5':
            ontology = knowledge_graph.load_base_ontology()
            knowledge_graph.create_graph(ontology,csv_train_directory)
            knowledge_graph.create_lineage_structures_in_graph(ontology,csv_train_directory)
            ontology.save(os.path.join(csv_nn_directory, 'train_kg.rdf'))
        elif option == '6':
            ontology = knowledge_graph.load_base_ontology()
            knowledge_graph.create_graph(ontology,csv_test_directory)
            knowledge_graph.create_lineage_structures_in_graph(ontology,csv_test_directory)
            ontology.save(os.path.join(csv_nn_directory, 'test_kg.rdf'))
        elif option == '7':
            ontology = knowledge_graph.load_ontology(os.path.join(csv_nn_directory, 'train_kg.rdf'))
            create_training_files(ontology,'train')
        elif option == '8':
            ontology = knowledge_graph.load_ontology(os.path.join(csv_nn_directory, 'test_kg.rdf'))
            create_training_files(ontology,'test')
        elif option == '9':
            ontology = knowledge_graph.load_ontology(os.path.join(csv_nn_directory, 'train_kg.rdf'))
            create_training_files(ontology,'valid')
            pass
        elif option == '10':
            pass
        elif option == '11':
            pass
        elif option == '12':
            running = False
        else:
            print('invalid option')
        if(running): input('Continue...')
if __name__ == "__main__":
    main()