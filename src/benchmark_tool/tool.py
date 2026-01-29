import database
import helpers
import knowledge_graph
from config import csv_test_directory, csv_train_directory, csv_nn_directory
import os
import random
scenario_types = {
    1 : 'select',
    2 : 'transformation',
    3 : 'join',
    4 : 'deduplication',
    5 : 'derived',
    6 : 'materialized',
    7 : 'recursive',
    8 : 'tabular',
    9 : 'partitioning',
    10 : 'temporary'
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
    print('9. Create training part of benchmark. (All scenario types)')
    print('10. Create test part of benchmark. (All scenario types)')
    print('11. Test model')
    print('12. Exit.')
def scenario_type_menu():
    print('1. Select based scenarios')
    print('2. Transformation based scenarios')
    print('3. Join based scenarios')
    print('4. Data deduplication')
    print('5. Derived views')
    print('6. Materialized summary tables')
    print('7. Recursive queries and hierarchical data')
    print('8. Tabular operation based objects')
    print('9. Partitioned data processing')
    print('10. Temporary table utilization')

def create_csv_data(target_directory):

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
        for entry in database.get_all_data(connection, db_name):
            parsed_entry = [str(cell).replace(';',',') for cell in entry]
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

def create_training_files(ontology, inductive = False):
    train_file = open(f'SiaILP/data/lineage_full{"_ind" if inductive else ""}/train.txt','w+', encoding='utf8')
    test_file = open(f'SiaILP/data/lineage_full{"_ind" if inductive else ""}/test.txt','w+', encoding='utf8')
    validation_file = open(f'SiaILP/data/lineage_full{"_ind" if inductive else ""}/valid.txt','w+', encoding='utf8')
    for individual in ontology.individuals():
        for prop in individual.get_properties():
            for value in prop[individual]:
                if(hasattr(value,'name')):
                    value_str = value.name
                else:
                    value_str = str(value)
                value_str = value_str.replace(' ','_')
                value_str = value_str.replace('put-dataset-data-lineage-individuals-base.','')
                value_str = value_str.replace('training_files\\train_kg.','')
                value_str = value_str.replace('training_files\\test_kg.','')
                individual_name = 'ind_' + individual.name if inductive else 'orig_' + individual.name
                value_name = 'ind_' + value_str if inductive else 'orig_' + value_str
                rng = random.random() 
                # if rng < 0.9 : continue
                rng = random.random() 

                # train_file.write(f"{value_name}\t{prop.python_name+'_inverse'}\t{individual_name}\n")
                train_file.write(f"{individual_name}\t{prop.python_name}\t{value_name}\n")

                # test_file.write(f"{value_name}\t{prop.python_name+'_inverse'}\t{individual_name}\n")
                test_file.write(f"{individual_name}\t{prop.python_name}\t{value_name}\n")

                # validation_file.write(f"{value_name}\t{prop.python_name+'_inverse'}\t{individual_name}\n")
                validation_file.write(f"{individual_name}\t{prop.python_name}\t{value_name}\n")
    train_file.close()
    test_file.close()
    validation_file.close()


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
            create_csv_data(csv_train_directory)
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
            create_training_files(ontology,False)
        elif option == '8':
            ontology = knowledge_graph.load_ontology(os.path.join(csv_nn_directory, 'test_kg.rdf'))
            create_training_files(ontology,True)
        elif option == '9':
            connection = database.create_connection()
            exclude = int(input('Which scenario should be excluded? (And later added to the test set) [1-5]'))
            chosen_scenarios = []
            for i in range(5):
                if i+1 != exclude:
                    chosen_scenarios.append(i+1)
            print('Adding scenarios...')
            for scenario_type in range(10):
                for scenario_number in chosen_scenarios:
                    try:
                        database.create_scenario(connection, scenario_types[scenario_type+1], scenario_number, verbose=True)
                    except Exception as ex:
                        print(scenario_types[scenario_type+1], scenario_number, 'failed')
            print('Generating CSVs...')
            create_csv_data(csv_train_directory)
            print('Creating graph...')
            ontology = knowledge_graph.load_base_ontology()
            knowledge_graph.create_graph(ontology,csv_train_directory)
            knowledge_graph.create_lineage_structures_in_graph(ontology,csv_train_directory)
            ontology.save(os.path.join(csv_nn_directory, 'train_kg.rdf'))
            print('Creating training files...')
            create_training_files(ontology,False)
            connection.close()
        elif option == '10':
            connection = database.create_connection()
            include = int(input('Which scenario should be included in testing? (It should not be in the training part) [1-5]'))
            chosen_scenarios = [include]
            print('Adding scenarios...')
            for scenario_type in range(10):
                for scenario_number in chosen_scenarios:
                    try:
                        database.create_scenario(connection, scenario_types[scenario_type+1], scenario_number, verbose=True)
                    except Exception as ex:
                        print(scenario_types[scenario_type+1], scenario_number, 'failed')
            print('Generating CSVs...')
            create_csv_data(csv_test_directory)
            print('Creating graph...')
            ontology = knowledge_graph.load_base_ontology()
            knowledge_graph.create_graph(ontology,csv_test_directory)
            knowledge_graph.create_lineage_structures_in_graph(ontology,csv_test_directory)
            ontology.save(os.path.join(csv_nn_directory, 'test_kg.rdf'))
            print('Creating training files...')
            create_training_files(ontology,True)
            connection.close()
        elif option == '11':
            pass
        elif option == '12':
            running = False
        else:
            print('invalid option')
        if(running): input('Continue...')
if __name__ == "__main__":
    main()