import os
import sys
import numpy as np
import random
import pickle
import argparse
import tensorflow as tf

from collections import defaultdict
from copy import deepcopy
from sklearn.utils import shuffle
from tensorflow import keras
from tensorflow.keras import layers
from tqdm import tqdm
from tensorflow.keras.optimizers.schedules import ExponentialDecay

#surpass all warning message
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
tf.get_logger().setLevel('ERROR')

# Redirect stderr to /dev/null
#sys.stderr = open(os.devnull, 'w')

def get_max(t):
    import tensorflow as tf
    return tf.reduce_max(t, axis=1)
def get_sum(t):
    import tensorflow as tf
    return tf.reduce_sum(t, axis=1)
def l2norm(t):
    import tensorflow as tf
    return tf.math.l2_normalize(t, axis=1)

##################################
####functions/classes#############
class LoadKG:
    
    def __init__(self):
        
        self.x = 'Hello'
        
    def load_train_data(self, data_path, one_hop, data, s_t_r, entity2id, id2entity,
                     relation2id, id2relation):
        
        data_ = set()
    
        ####load the train, valid and test set##########
        with open (data_path, 'r') as f:
            
            data_ini = f.readlines()
                        
            for i in range(len(data_ini)):
            
                x = data_ini[i].split()
                
                x_ = tuple(x)
                
                data_.add(x_)
        
        ####relation dict#################
        index = len(relation2id)
     
        for key in data_:
            
            if key[1] not in relation2id:
                
                relation = key[1]
                
                relation2id[relation] = index
                
                id2relation[index] = relation
                
                index += 1
                
                #the inverse relation
                iv_r = '_inverse_' + relation
                
                relation2id[iv_r] = index
                
                id2relation[index] = iv_r
                
                index += 1
        
        #get the id of the inverse relation, by above definition, initial relation has 
        #always even id, while inverse relation has always odd id.
        def inverse_r(r):
            
            if r % 2 == 0: #initial relation
                
                iv_r = r + 1
            
            else: #inverse relation
                
                iv_r = r - 1
            
            return(iv_r)
        
        ####entity dict###################
        index = len(entity2id)
        
        for key in data_:
            
            source, target = key[0], key[2]
            
            if source not in entity2id:
                                
                entity2id[source] = index
                
                id2entity[index] = source
                
                index += 1
            
            if target not in entity2id:
                
                entity2id[target] = index
                
                id2entity[index] = target
                
                index += 1
                
        #create the set of triples using id instead of string        
        for ele in data_:
            
            s = entity2id[ele[0]]
            
            r = relation2id[ele[1]]
            
            t = entity2id[ele[2]]
            
            if (s,r,t) not in data:
                
                data.add((s,r,t))
            
            s_t_r[(s,t)].add(r)
            
            if s not in one_hop:
                
                one_hop[s] = set()
            
            one_hop[s].add((r,t))
            
            if t not in one_hop:
                
                one_hop[t] = set()
            
            r_inv = inverse_r(r)
            
            s_t_r[(t,s)].add(r_inv)
            
            one_hop[t].add((r_inv,s))
            
        #change each set in one_hop to list
        for e in one_hop:
            
            one_hop[e] = list(one_hop[e])


class ObtainPathsByDynamicProgramming:

    def __init__(self, amount_bd=50, size_bd=50, threshold=20000):
        
        self.amount_bd = amount_bd #how many Tuples we choose in one_hop[node] for next recursion
                        
        self.size_bd = size_bd #size bound limit the number of paths to a target entity t
        
        #number of times paths with specific length been performed for recursion
        self.threshold = threshold
        
    '''
    Given an entity s, the function will find the paths from s to other entities, using recursion.
    
    One may refer to LeetCode Problem 797 for details:
        https://leetcode.com/problems/all-paths-from-source-to-target/
    '''
    def obtain_paths(self, mode, s, t_input, lower_bd, upper_bd, one_hop):

        if type(lower_bd) != type(1) or lower_bd < 1:
            
            raise TypeError("!!! invalid lower bound setting, must >= 1 !!!")
            
        if type(upper_bd) != type(1) or upper_bd < 1:
            
            raise TypeError("!!! invalid upper bound setting, must >= 1 !!!")
            
        if lower_bd > upper_bd:
            
            raise TypeError("!!! lower bound must not exced upper bound !!!")
            
        if s not in one_hop:
            
            raise ValueError('!!! entity not in one_hop. Please work on existing entities')

        #here is the result dict. Its key is each entity t sharing paths from s
        #The value of each t is a set containing the paths from s to t
        #These paths can be either the direct connection r, or a multi-hop path
        res = defaultdict(set)
        
        #qualified_t contains the types of t we want to consider,
        #that is, what t will be added to the result set.
        qualified_t = set()

        #under this mode, we will only consider the direct neighbour of s
        if mode == 'direct_neighbour':
        
            for Tuple in one_hop[s]:
            
                t = Tuple[1]
                
                qualified_t.add(t)
        
        #under this mode, we will only consider one specified entity t
        elif mode == 'target_specified':
            
            qualified_t.add(t_input)
        
        #under this mode, we will consider any entity
        elif mode == 'any_target':
            
            for s_any in one_hop:
                
                qualified_t.add(s_any)
                
        else:
            
            raise ValueError('not a valid mode')
        
        '''
        We use recursion to find the paths
        On current node with the path [r1, ..., rk] and on-path entities {s, e1, ..., ek-1, node}
        from s to this node, we will further find the direct neighbor t' of this node. 
        If t' is not an on-path entity (not among s, e1,...ek-1, node), we recursively proceed to t' 
        '''
        def helper(node, path, on_path_en, res, qualified_t, lower_bd, upper_bd, one_hop, count_dict):

            #when the current path is within lower_bd and upper_bd, 
            #and the node is among the qualified t, and it has not been fill of paths w.r.t size_limit,
            #we will add this path to the node
            if (len(path) >= lower_bd) and (len(path) <= upper_bd) and (
                node in qualified_t) and (len(res[node]) < self.size_bd):
                
                res[node].add(tuple(path))
                    
            #won't start new recursions if the current path length already reaches upper limit
            #or the number of recursions performed on this length has reached the limit
            if (len(path) < upper_bd) and (count_dict[len(path)] <= self.threshold):
                                
                #temp list is the id list for us to go-over one_hop[node]
                temp_list = [i for i in range(len(one_hop[node]))]
                random.shuffle(temp_list) #so we random-shuffle the list
                
                #only take 20 recursions if there are too many (r,t)
                for i in temp_list[:self.amount_bd]:
                    
                    #obtain tuple of (r,t)
                    Tuple = one_hop[node][i]
                    r, t = Tuple[0], Tuple[1]
                    
                    #add to count_dict even if eventually this step not proceed
                    count_dict[len(path)] += 1
                    
                    #if t not on the path and we not exceed the computation threshold, 
                    #then finally proceed to next recursion
                    if (t not in on_path_en) and (count_dict[len(path)] <= self.threshold):

                        helper(t, path + [r], on_path_en.union({t}), res, qualified_t, 
                               lower_bd, upper_bd, one_hop, count_dict)

        length_dict = defaultdict(int)
        count_dict = defaultdict(int)
        
        helper(s, [], {s}, res, qualified_t, lower_bd, upper_bd, one_hop, count_dict)
        
        return(res, count_dict)
    

#function to build the big batche for connection-based training
def build_big_batches_path(lower_bd, upper_bd, data, one_hop, s_t_r,
                      x_p_list, x_r_list, y_list,
                      relation2id, entity2id, id2relation, id2entity):
    
    #the set of all relation IDs
    relation_id_set = set()
    
    #the set of all initial relations
    ini_r_id_set = set()
    
    for i in range(len(id2relation)):
        
        if i not in id2relation:
            raise ValueError('error when generaing id2relation')
        
        relation_id_set.add(i)
        
        if i % 2 == 0: #initial relation id is always an even number
            ini_r_id_set.add(i)
    
    num_r = len(id2relation)
    num_ini_r = len(ini_r_id_set)
    
    if num_ini_r != int(num_r/2):
        raise ValueError('error when generating id2relation')
    
    #in case not all entities in entity2id are in one_hop, 
    #so we need to find out who are indeed in
    existing_ids = set()
    
    for s_1 in one_hop:
        existing_ids.add(s_1)
        
    existing_ids = list(existing_ids)
    random.shuffle(existing_ids)
    
    count = 0
    for s in tqdm(existing_ids, desc='generating big-batches for connection-based model'):
        
        #impliment the path finding algorithm to find paths between s and t
        result, length_dict = Class_2.obtain_paths('direct_neighbour', s, 'nb', lower_bd, upper_bd, one_hop)
        
        for iteration in range(10):

            #proceed only if at least three paths are between s and t
            for t in result:

                if len(s_t_r[(s,t)]) == 0:

                    raise ValueError(s,t,id2entity[s], id2entity[t])

                #we are only interested in forward link in relation prediciton
                ini_r_list = list()

                #obtain initial relations between s and t
                for r in s_t_r[(s,t)]:
                    if r % 2 == 0:#initial relation id is always an even number
                        ini_r_list.append(r)

                #if there exist more than three paths between s and t, 
                #and inital connection between s and t exists,
                #and not every r in the relation dictionary exists between s and t (although this is rare)
                #we then proceed
                if len(result[t]) >= 3 and len(ini_r_list) > 0 and len(ini_r_list) < int(num_ini_r):

                    #obtain the list form of all the paths from s to t
                    temp_path_list = list(result[t])

                    temp_pair = random.sample(temp_path_list, 3)

                    path_1, path_2, path_3 = temp_pair[0], temp_pair[1], temp_pair[2]

                    #####positive#####################
                    #append the paths: note that we add the space holder id at the end of the shorter path
                    x_p_list['1'].append(list(path_1) + [num_r]*abs(len(path_1)-upper_bd))
                    x_p_list['2'].append(list(path_2) + [num_r]*abs(len(path_2)-upper_bd))
                    x_p_list['3'].append(list(path_3) + [num_r]*abs(len(path_3)-upper_bd))

                    #append relation
                    r = random.choice(ini_r_list)
                    x_r_list.append([r])
                    y_list.append(1.)

                    #####negative#####################
                    #append the paths: note that we add the space holder id at the end
                    #of the shorter path
                    x_p_list['1'].append(list(path_1) + [num_r]*abs(len(path_1)-upper_bd))
                    x_p_list['2'].append(list(path_2) + [num_r]*abs(len(path_2)-upper_bd))
                    x_p_list['3'].append(list(path_3) + [num_r]*abs(len(path_3)-upper_bd))

                    #append relation
                    neg_r_list = list(ini_r_id_set.difference(set(ini_r_list)))
                    r_ran = random.choice(neg_r_list)
                    x_r_list.append([r_ran])
                    y_list.append(0.)
        
        count += 1
        #if count % 100 == 0:
        #    print('generating big-batches for connection-based model', count, len(existing_ids))
            
            
#Again, it is too slow to run the path-finding algorithm again and again on the complete FB15K-237
#Instead, we will find the subgraph for each entity once.
#then in the subgraph based training, the subgraphs are stored and used for multiple times
def store_subgraph_dicts(lower_bd, upper_bd, data, one_hop, s_t_r,
                         relation2id, entity2id, id2relation, id2entity):
    
    #the set of all relation IDs
    relation_id_set = set()
    
    for i in range(len(id2relation)):
        
        if i not in id2relation:
            raise ValueError('error when generaing id2relation')
        
        relation_id_set.add(i)
    
    num_r = len(id2relation)
    
    #in case not all entities in entity2id are in one_hop, 
    #so we need to find out who are indeed in
    existing_ids = set()
    
    for s_1 in one_hop:
        existing_ids.add(s_1)
    
    #the ids to start path finding
    existing_ids = list(existing_ids)
    random.shuffle(existing_ids)
    
    #Dict stores the subgraph for each entity
    Dict_1 = dict()
    
    count = 0
    for s in tqdm(existing_ids, desc='generating and storing paths for the connection-based model'):
        
        path_set = set()
            
        result, length_dict = Class_2.obtain_paths('any_target', s, 'any', lower_bd, upper_bd, one_hop)

        for t_ in result:
            for path in result[t_]:
                path_set.add(path)

        del(result, length_dict)
        
        path_list = list(path_set)
        
        path_select = random.sample(path_list, min(len(path_list), 100))
            
        Dict_1[s] = deepcopy(path_select)
        
        count += 1
        #if count % 100 == 0:
        #    print('generating and storing paths for the connection-based model', count, len(existing_ids))
        
    return(Dict_1)


#function to build the big-batch for one-hope neighbor training
def build_big_batches_subgraph(lower_bd, upper_bd, data, one_hop, s_t_r,
                      x_s_list, x_t_list, x_r_list, y_list, Dict,
                      relation2id, entity2id, id2relation, id2entity):
    
    #the set of all relation IDs
    relation_id_set = set()
    
    #the set of all initial relations
    ini_r_id_set = set()
    
    for i in range(len(id2relation)):
        
        if i not in id2relation:
            raise ValueError('error when generaing id2relation')
        
        relation_id_set.add(i)
        
        if i % 2 == 0: #initial relation id is always an even number
            ini_r_id_set.add(i)
    
    num_r = len(id2relation)
    num_ini_r = len(ini_r_id_set)
    
    if num_ini_r != int(num_r/2):
        raise ValueError('error when generating id2relation')
        
    #if an entity has at least three out-stretching paths, it is a qualified one
    qualified = set()
    for e in Dict:
        if len(Dict[e]) >= 3:
            qualified.add(e)
    qualified = list(qualified)
    
    data = list(data)
    
    for iteration in range(10):

        data = shuffle(data)

        for i_0 in tqdm(range(len(data)), desc='generating big-batches for subgraph-based model'):

            triple = data[i_0]

            s, r, t = triple[0], triple[1], triple[2] #obtain entities and relation IDs

            if s in qualified and t in qualified:

                #obtain the path list for true entities
                path_s, path_t = list(Dict[s]), list(Dict[t])

                #####positive step###########
                #randomly obtain three paths for true entities
                temp_s = random.sample(path_s, 3)
                temp_t = random.sample(path_t, 3)
                s_p_1, s_p_2, s_p_3 = temp_s[0], temp_s[1], temp_s[2]
                t_p_1, t_p_2, t_p_3 = temp_t[0], temp_t[1], temp_t[2]

                #append the paths: note that we add the space holder id at the end of the shorter path
                x_s_list['1'].append(list(s_p_1) + [num_r]*abs(len(s_p_1)-upper_bd))
                x_s_list['2'].append(list(s_p_2) + [num_r]*abs(len(s_p_2)-upper_bd))
                x_s_list['3'].append(list(s_p_3) + [num_r]*abs(len(s_p_3)-upper_bd))

                x_t_list['1'].append(list(t_p_1) + [num_r]*abs(len(t_p_1)-upper_bd))
                x_t_list['2'].append(list(t_p_2) + [num_r]*abs(len(t_p_2)-upper_bd))
                x_t_list['3'].append(list(t_p_3) + [num_r]*abs(len(t_p_3)-upper_bd))

                #append relation
                x_r_list.append([r])
                y_list.append(1.)

                #####negative step for relation###########
                #append the paths: note that we add the space holder id at the end of the shorter path
                x_s_list['1'].append(list(s_p_1) + [num_r]*abs(len(s_p_1)-upper_bd))
                x_s_list['2'].append(list(s_p_2) + [num_r]*abs(len(s_p_2)-upper_bd))
                x_s_list['3'].append(list(s_p_3) + [num_r]*abs(len(s_p_3)-upper_bd))

                x_t_list['1'].append(list(t_p_1) + [num_r]*abs(len(t_p_1)-upper_bd))
                x_t_list['2'].append(list(t_p_2) + [num_r]*abs(len(t_p_2)-upper_bd))
                x_t_list['3'].append(list(t_p_3) + [num_r]*abs(len(t_p_3)-upper_bd))

                #append relation
                neg_r_list = list(ini_r_id_set.difference({r}))
                r_ran = random.choice(neg_r_list)
                x_r_list.append([r_ran])
                y_list.append(0.)
                
                ##############################################
                ##############################################
                #randomly choose two negative sampled entities
                s_ran = random.choice(qualified)
                t_ran = random.choice(qualified)

                #obtain the path list for random entities
                path_s_ran, path_t_ran = list(Dict[s_ran]), list(Dict[t_ran])
                
                #####positive step#################
                #Again: randomly obtain three paths
                temp_s = random.sample(path_s, 3)
                temp_t = random.sample(path_t, 3)
                s_p_1, s_p_2, s_p_3 = temp_s[0], temp_s[1], temp_s[2]
                t_p_1, t_p_2, t_p_3 = temp_t[0], temp_t[1], temp_t[2]

                #append the paths: note that we add the space holder id at the end of the shorter path
                x_s_list['1'].append(list(s_p_1) + [num_r]*abs(len(s_p_1)-upper_bd))
                x_s_list['2'].append(list(s_p_2) + [num_r]*abs(len(s_p_2)-upper_bd))
                x_s_list['3'].append(list(s_p_3) + [num_r]*abs(len(s_p_3)-upper_bd))

                x_t_list['1'].append(list(t_p_1) + [num_r]*abs(len(t_p_1)-upper_bd))
                x_t_list['2'].append(list(t_p_2) + [num_r]*abs(len(t_p_2)-upper_bd))
                x_t_list['3'].append(list(t_p_3) + [num_r]*abs(len(t_p_3)-upper_bd))

                #append relation
                x_r_list.append([r])
                y_list.append(1.)

                #####negative for source entity###########
                #randomly obtain three paths
                temp_s = random.sample(path_s_ran, 3)
                s_p_1, s_p_2, s_p_3 = temp_s[0], temp_s[1], temp_s[2]

                #append the paths: note that we add the space holder id at the end of the shorter path
                x_s_list['1'].append(list(s_p_1) + [num_r]*abs(len(s_p_1)-upper_bd))
                x_s_list['2'].append(list(s_p_2) + [num_r]*abs(len(s_p_2)-upper_bd))
                x_s_list['3'].append(list(s_p_3) + [num_r]*abs(len(s_p_3)-upper_bd))

                x_t_list['1'].append(list(t_p_1) + [num_r]*abs(len(t_p_1)-upper_bd))
                x_t_list['2'].append(list(t_p_2) + [num_r]*abs(len(t_p_2)-upper_bd))
                x_t_list['3'].append(list(t_p_3) + [num_r]*abs(len(t_p_3)-upper_bd))

                #append relation
                x_r_list.append([r])
                y_list.append(0.)

                #####positive step###########
                #Again: randomly obtain three paths
                temp_s = random.sample(path_s, 3)
                temp_t = random.sample(path_t, 3)
                s_p_1, s_p_2, s_p_3 = temp_s[0], temp_s[1], temp_s[2]
                t_p_1, t_p_2, t_p_3 = temp_t[0], temp_t[1], temp_t[2]

                #append the paths: note that we add the space holder id at the end of the shorter path
                x_s_list['1'].append(list(s_p_1) + [num_r]*abs(len(s_p_1)-upper_bd))
                x_s_list['2'].append(list(s_p_2) + [num_r]*abs(len(s_p_2)-upper_bd))
                x_s_list['3'].append(list(s_p_3) + [num_r]*abs(len(s_p_3)-upper_bd))

                x_t_list['1'].append(list(t_p_1) + [num_r]*abs(len(t_p_1)-upper_bd))
                x_t_list['2'].append(list(t_p_2) + [num_r]*abs(len(t_p_2)-upper_bd))
                x_t_list['3'].append(list(t_p_3) + [num_r]*abs(len(t_p_3)-upper_bd))

                #append relation
                x_r_list.append([r])
                y_list.append(1.)

                #####negative for target entity###########
                #randomly obtain three paths
                temp_t = random.sample(path_t_ran, 3)
                t_p_1, t_p_2, t_p_3 = temp_t[0], temp_t[1], temp_t[2]

                #append the paths: note that we add the space holder id at the end of the shorter path
                x_s_list['1'].append(list(s_p_1) + [num_r]*abs(len(s_p_1)-upper_bd))
                x_s_list['2'].append(list(s_p_2) + [num_r]*abs(len(s_p_2)-upper_bd))
                x_s_list['3'].append(list(s_p_3) + [num_r]*abs(len(s_p_3)-upper_bd))

                x_t_list['1'].append(list(t_p_1) + [num_r]*abs(len(t_p_1)-upper_bd))
                x_t_list['2'].append(list(t_p_2) + [num_r]*abs(len(t_p_2)-upper_bd))
                x_t_list['3'].append(list(t_p_3) + [num_r]*abs(len(t_p_3)-upper_bd))

                #append relation
                x_r_list.append([r])
                y_list.append(0.)

            #if i_0 % 200 == 0:
            #    print('generating big-batches for subgraph-based model', i_0, len(data), iteration)


###################################
####implementation#################

if __name__ == "__main__":
    
    
    ##################################
    ####original parameters###########
    parser = argparse.ArgumentParser(description="Run the training model with specified parameters.")
    parser.add_argument('--data_name', type=str, required=True, help='The name of the dataset')
    parser.add_argument('--model_name', type=str, required=True, help='The name of the model')
    parser.add_argument('--num_epoch', type=int, required=True, help='The number of epochs')

    args = parser.parse_args()
    
    data_name = args.data_name 
    model_name = args.model_name
    num_epoch = args.num_epoch
    
    lower_bound = 1 #the lower bound on path length, shared by both the connection-based model and sub-graph based model
    upper_bound_path = 10 #the upper bound on path length, for connection-based model
    upper_bound_subg = 3 #the upper bound on path length, for subgraph-based model
    
    batch_size = 32 #batch size
    
    #difine the names for checkpoint saving
    model_name_ = 'Model_' + model_name + '_' + data_name
    one_hop_model_name = 'One_hop_model_' + model_name + '_' + data_name
    ids_name = 'IDs_' + model_name + '_' + data_name
    #################################
    
    
    train_path = './data/' + data_name + '/train.txt'
    valid_path = './data/' + data_name + '/valid.txt'
    test_path = './data/' + data_name + '/test.txt'
    
    
    #load the classes
    Class_1 = LoadKG()
    Class_2 = ObtainPathsByDynamicProgramming()

    #define the dictionaries and sets for load KG
    one_hop = dict() 
    data = set()
    s_t_r = defaultdict(set)
    
    #define the dictionaries, which is shared by initail and inductive train/valid/test
    entity2id = dict()
    id2entity = dict()
    relation2id = dict()
    id2relation = dict()
    
    #fill in the sets and dicts
    Class_1.load_train_data(train_path, one_hop, data, s_t_r,
                            entity2id, id2entity, relation2id, id2relation)

    #define the dictionaries and sets for load KG
    one_hop_valid = dict() 
    data_valid = set()
    s_t_r_valid = defaultdict(set)
    
    #fill in the sets and dicts
    Class_1.load_train_data(valid_path, one_hop_valid, data_valid, s_t_r_valid,
                            entity2id, id2entity, relation2id, id2relation)


    #define the dictionaries and sets for load KG
    one_hop_test = dict() 
    data_test = set()
    s_t_r_test = defaultdict(set)
    
    #fill in the sets and dicts
    Class_1.load_train_data(test_path, one_hop_test, data_test, s_t_r_test,
                            entity2id, id2entity, relation2id, id2relation)

    
    
    #############################################################
    ####build siamese NN for connection-based model##############
    #############################################################
    
    # Input layer, using integer to represent each relation type
    #note that inputs_path is the path inputs, while inputs_out_re is the output relation inputs
    fst_path = keras.Input(shape=(None,), dtype="int32")
    scd_path = keras.Input(shape=(None,), dtype="int32")
    thd_path = keras.Input(shape=(None,), dtype="int32")
    
    #the relation input layer (for output embedding)
    id_rela = keras.Input(shape=(None,), dtype="int32")
    
    # Embed each integer in a 300-dimensional vector as input,
    # note that we add another "space holder" embedding, 
    # which hold the spaces if the initial length of paths are not the same
    in_embd_var = layers.Embedding(len(relation2id)+1, 300)
    
    # Obtain the embedding
    fst_p_embd = in_embd_var(fst_path)
    scd_p_embd = in_embd_var(scd_path)
    thd_p_embd = in_embd_var(thd_path)
    
    # Embed each integer in a 300-dimensional vector as output
    rela_embd = layers.Embedding(len(relation2id)+1, 300)(id_rela)
    
    #add 2 layer bi-directional LSTM
    lstm_layer_1 = layers.Bidirectional(layers.LSTM(150, return_sequences=True))
    lstm_layer_2 = layers.Bidirectional(layers.LSTM(150, return_sequences=True))
    
    #first LSTM layer
    fst_lstm_mid = lstm_layer_1(fst_p_embd)
    scd_lstm_mid = lstm_layer_1(scd_p_embd)
    thd_lstm_mid = lstm_layer_1(thd_p_embd)
    
    #second LSTM layer
    fst_lstm_out = lstm_layer_2(fst_lstm_mid)
    scd_lstm_out = lstm_layer_2(scd_lstm_mid)
    thd_lstm_out = lstm_layer_2(thd_lstm_mid)
    
    #reduce max
    fst_reduce_max = layers.Lambda(get_max,output_shape = (300,))(fst_lstm_out)
    scd_reduce_max = layers.Lambda(get_max,output_shape = (300,))(scd_lstm_out)
    thd_reduce_max = layers.Lambda(get_max,output_shape = (300,))(thd_lstm_out)
    
    #concatenate the output vector from both siamese tunnel: (Batch, 900)
    path_concat = layers.concatenate([fst_reduce_max, scd_reduce_max, thd_reduce_max], axis=-1)
    
    #add dropout on top of the concatenation from all channels
    dropout = layers.Dropout(0.25)(path_concat)
    
    #multiply into output embd size by dense layer: (Batch, 300)
    path_out_vect = layers.Dense(300, activation='tanh')(dropout)
    
    #remove the time dimension from the output embd since there is only one step
    rela_out_embd = layers.Lambda(lambda t: get_sum,output_shape = (300,))(rela_embd)
    
    # Normalize the vectors to have unit length
    path_out_vect_norm = layers.Lambda(lambda t: l2norm,output_shape = (300,))(path_out_vect)
    rela_out_embd_norm = layers.Lambda(lambda t: l2norm,output_shape = (300,))(rela_out_embd)
    
    # Calculate the dot product
    dot_product = layers.Dot(axes=-1)([path_out_vect_norm, rela_out_embd_norm])
    
    #put together the model
    model = keras.Model([fst_path, scd_path, thd_path, id_rela], dot_product)
    
    #config the Adam optimizer 
    lr_schedule = ExponentialDecay(
        initial_learning_rate=0.0005,
        decay_steps=10000,
        decay_rate=0.96,
        staircase=True)
    
    opt = keras.optimizers.Adam(learning_rate=lr_schedule)
    
    #compile the model
    model.compile(loss='binary_crossentropy', optimizer=opt, metrics=['binary_accuracy'])



    #############################################################
    ####build siamese NN for subgraph-based model################
    #############################################################

    #each input is an vector with number of relations to be dim:
    #each dim represent the existence (1) or not (0) of an out-going relation from the entity
    source_path_1 = keras.Input(shape=(None,), dtype="int32")
    source_path_2 = keras.Input(shape=(None,), dtype="int32")
    source_path_3 = keras.Input(shape=(None,), dtype="int32")
    
    target_path_1 = keras.Input(shape=(None,), dtype="int32")
    target_path_2 = keras.Input(shape=(None,), dtype="int32")
    target_path_3 = keras.Input(shape=(None,), dtype="int32")
    
    #the relation input layer (for output embedding)
    id_rela_ = keras.Input(shape=(None,), dtype="int32")
    
    # Embed each integer in a 300-dimensional vector as input,
    # note that we add another "space holder" embedding, 
    # which hold the spaces if the initial length of paths are not the same
    in_embd_var_ = layers.Embedding(len(relation2id)+1, 300)
    
    # Obtain the source embeddings
    source_embd_1 = in_embd_var_(source_path_1)
    source_embd_2 = in_embd_var_(source_path_2)
    source_embd_3 = in_embd_var_(source_path_3)
    
    #Obtain the target embeddings
    target_embd_1 = in_embd_var_(target_path_1)
    target_embd_2 = in_embd_var_(target_path_2)
    target_embd_3 = in_embd_var_(target_path_3)
    
    # Embed each integer in a 300-dimensional vector as output
    rela_embd_ = layers.Embedding(len(relation2id)+1, 300)(id_rela_)
    
    #add 2 layer bi-directional LSTM network
    lstm_1 = layers.Bidirectional(layers.LSTM(150, return_sequences=True))
    lstm_2 = layers.Bidirectional(layers.LSTM(150, return_sequences=True))
    
    ###source lstm implimentation########
    #first LSTM layer
    source_mid_1 = lstm_1(source_embd_1)
    source_mid_2 = lstm_1(source_embd_2)
    source_mid_3 = lstm_1(source_embd_3)
    
    #second LSTM layer
    source_out_1 = lstm_2(source_mid_1)
    source_out_2 = lstm_2(source_mid_2)
    source_out_3 = lstm_2(source_mid_3)
    
    #reduce max
    source_max_1 = layers.Lambda(get_max,output_shape = (300,))(source_out_1)
    source_max_2 = layers.Lambda(get_max,output_shape = (300,))(source_out_2)
    source_max_3 = layers.Lambda(get_max,output_shape = (300,))(source_out_3)
    
    #concatenate the output vector from both siamese tunnel: (Batch, 900)
    source_concat = layers.concatenate([source_max_1, source_max_2, source_max_3], axis=-1)
    
    #add dropout on top of the concatenation from all channels
    source_dropout = layers.Dropout(0.25)(source_concat)
    
    ###target lstm implimentation########
    #first LSTM layer
    target_mid_1 = lstm_1(target_embd_1)
    target_mid_2 = lstm_1(target_embd_2)
    target_mid_3 = lstm_1(target_embd_3)
    
    #second LSTM layer
    target_out_1 = lstm_2(target_mid_1)
    target_out_2 = lstm_2(target_mid_2)
    target_out_3 = lstm_2(target_mid_3)
    
    #reduce max
    target_max_1 = layers.Lambda(get_max,output_shape = (300,))(target_out_1)
    target_max_2 = layers.Lambda(get_max,output_shape = (300,))(target_out_2)
    target_max_3 = layers.Lambda(get_max,output_shape = (300,))(target_out_3)
    
    #concatenate the output vector from both siamese tunnel: (Batch, 900)
    target_concat = layers.concatenate([target_max_1, target_max_2, target_max_3], axis=-1)
    
    #add dropout on top of the concatenation from all channels
    target_dropout = layers.Dropout(0.25)(target_concat)
    
    #further concatenate source and target output embeddings: (Batch, 1800)
    final_concat = layers.concatenate([source_dropout, target_dropout], axis=-1)
    
    #multiply into output embd size by dense layer: (Batch, 300)
    out_vect = layers.Dense(300, activation='tanh')(final_concat)
    
    #remove the time dimension from the output embd since there is only one step
    rela_out_embd_ = layers.Lambda(get_max,output_shape = (300,))(rela_embd_)
    
    # Normalize the vectors to have unit length
    out_vect_norm = layers.Lambda(l2norm,output_shape = (300,))(out_vect)
    rela_out_embd_norm_ = layers.Lambda(l2norm,output_shape = (300,))(rela_out_embd_)

    
    # Calculate the dot product
    dot_product_ = layers.Dot(axes=-1)([out_vect_norm, rela_out_embd_norm_])
    
    #put together the model
    model_2 = keras.Model([source_path_1, source_path_2, source_path_3,
                           target_path_1, target_path_2, target_path_3, id_rela_], dot_product_)

    #config the Adam optimizer 
    opt_ = keras.optimizers.Adam(learning_rate=lr_schedule)
    
    #compile the model
    model_2.compile(loss='binary_crossentropy', optimizer=opt_, metrics=['binary_accuracy'])



    ####################################
    ####start training##################
    ####################################
    #first, we save the relation and ids
    Dict = dict()
    
    #save training data
    Dict['one_hop'] = one_hop
    Dict['data'] = data
    Dict['s_t_r'] = s_t_r
    
    #save valid data
    Dict['one_hop_valid'] = one_hop_valid
    Dict['data_valid'] = data_valid
    Dict['s_t_r_valid'] = s_t_r_valid
    
    #save test data
    Dict['one_hop_test'] = one_hop_test
    Dict['data_test'] = data_test
    Dict['s_t_r_test'] = s_t_r_test
    
    #save shared dictionaries
    Dict['entity2id'] = entity2id
    Dict['id2entity'] = id2entity
    Dict['relation2id'] = relation2id
    Dict['id2relation'] = id2relation
    
    with open('./weight_bin/' + ids_name + '.pickle', 'wb') as handle:
        pickle.dump(Dict, handle, protocol=pickle.HIGHEST_PROTOCOL)


    ###train the connection-based model
    lower_bd = lower_bound
    upper_bd = upper_bound_path
            
    #define the training lists
    train_p_list, train_r_list, train_y_list = {'1': [], '2': [], '3': []}, list(), list()
    
    #define the validation lists
    valid_p_list, valid_r_list, valid_y_list = {'1': [], '2': [], '3': []}, list(), list()
    
    #######################################
    ###build the big-batches###############      
    
    #fill in the training array list
    build_big_batches_path(lower_bd, upper_bd, data, one_hop, s_t_r,
                          train_p_list, train_r_list, train_y_list,
                          relation2id, entity2id, id2relation, id2entity)
    
    #fill in the validation array list
    build_big_batches_path(lower_bd, upper_bd, data_valid, one_hop_valid, s_t_r_valid,
                          valid_p_list, valid_r_list, valid_y_list,
                          relation2id, entity2id, id2relation, id2entity)    
    
    

    ############################################
    ###train connection-based model#############
    ############################################
    
    #sometimes the validation dataset is so small so sparse, 
    #which cannot find three paths between any pair of s and t.
    #in such a case, we will divide the training big-batch into train and valid
    if len(valid_y_list) >= 100:
        #generate the input arrays
        x_train_1 = np.asarray(train_p_list['1'], dtype='int')
        x_train_2 = np.asarray(train_p_list['2'], dtype='int')
        x_train_3 = np.asarray(train_p_list['3'], dtype='int')
        x_train_r = np.asarray(train_r_list, dtype='int')
        y_train = np.asarray(train_y_list, dtype='int')
    
        #generate the validation arrays
        x_valid_1 = np.asarray(valid_p_list['1'], dtype='int')
        x_valid_2 = np.asarray(valid_p_list['2'], dtype='int')
        x_valid_3 = np.asarray(valid_p_list['3'], dtype='int')
        x_valid_r = np.asarray(valid_r_list, dtype='int')
        y_valid = np.asarray(valid_y_list, dtype='int')
    
    else:
        split = int(len(train_y_list)*0.8)
        #generate the input arrays
        x_train_1 = np.asarray(train_p_list['1'][:split], dtype='int')
        x_train_2 = np.asarray(train_p_list['2'][:split], dtype='int')
        x_train_3 = np.asarray(train_p_list['3'][:split], dtype='int')
        x_train_r = np.asarray(train_r_list[:split], dtype='int')
        y_train = np.asarray(train_y_list[:split], dtype='int')
    
        #generate the validation arrays
        x_valid_1 = np.asarray(train_p_list['1'][split:], dtype='int')
        x_valid_2 = np.asarray(train_p_list['2'][split:], dtype='int')
        x_valid_3 = np.asarray(train_p_list['3'][split:], dtype='int')
        x_valid_r = np.asarray(train_r_list[split:], dtype='int')
        y_valid = np.asarray(train_y_list[split:], dtype='int')
    
    #do the training
    model.fit([x_train_1, x_train_2, x_train_3, x_train_r], y_train, 
              validation_data=([x_valid_1, x_valid_2, x_valid_3, x_valid_r], y_valid),
              batch_size=batch_size, epochs=num_epoch)   
    
    # Save model and weights
    add_h5 = model_name_ + '.h5'
    save_dir = os.path.join(os.getcwd(), './weight_bin')
    
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    model_path = os.path.join(save_dir, add_h5)
    model.save(model_path)
    print('Save model')
    del(model)


    ###train the subgraph-based model
    lower_bd = lower_bound
    upper_bd = upper_bound_subg
    
    Dict_train = store_subgraph_dicts(lower_bd, upper_bd, data, one_hop, s_t_r,
                             relation2id, entity2id, id2relation, id2entity)
    
    Dict_valid = store_subgraph_dicts(lower_bd, upper_bd, data_valid, one_hop_valid, s_t_r_valid,
                             relation2id, entity2id, id2relation, id2entity)
            
    #define the training lists
    train_s_list, train_t_list, train_r_list, train_y_list = {'1': [], '2': [], '3': []}, {'1': [], '2': [], '3': []}, list(), list()
    
    #define the validation lists
    valid_s_list, valid_t_list, valid_r_list, valid_y_list = {'1': [], '2': [], '3': []}, {'1': [], '2': [], '3': []}, list(), list()
    
    #######################################
    ###build the big-batches###############      
    
    #fill in the training array list
    build_big_batches_subgraph(lower_bd, upper_bd, data, one_hop, s_t_r,
                          train_s_list, train_t_list, train_r_list, train_y_list, Dict_train,
                          relation2id, entity2id, id2relation, id2entity)
    
    #fill in the validation array list
    build_big_batches_subgraph(lower_bd, upper_bd, data_valid, one_hop_valid, s_t_r_valid,
                          valid_s_list, valid_t_list, valid_r_list, valid_y_list, Dict_valid,
                          relation2id, entity2id, id2relation, id2entity)    
    
    #######################################
    ###train subgraph-based model##########
    #######################################
    
    #sometimes the validation dataset is so small so sparse, 
    #which cannot find three paths between any pair of s and t.
    #in such a case, we will divide the training big-batch into train and valid
    if len(valid_y_list) >= 100:
        #generate the input arrays
        x_train_s_1 = np.asarray(train_s_list['1'], dtype='int')
        x_train_s_2 = np.asarray(train_s_list['2'], dtype='int')
        x_train_s_3 = np.asarray(train_s_list['3'], dtype='int')
    
        x_train_t_1 = np.asarray(train_t_list['1'], dtype='int')
        x_train_t_2 = np.asarray(train_t_list['2'], dtype='int')
        x_train_t_3 = np.asarray(train_t_list['3'], dtype='int')
    
        x_train_r = np.asarray(train_r_list, dtype='int')
        y_train = np.asarray(train_y_list, dtype='int')
    
        #generate the validation arrays
        x_valid_s_1 = np.asarray(valid_s_list['1'], dtype='int')
        x_valid_s_2 = np.asarray(valid_s_list['2'], dtype='int')
        x_valid_s_3 = np.asarray(valid_s_list['3'], dtype='int')
    
        x_valid_t_1 = np.asarray(valid_t_list['1'], dtype='int')
        x_valid_t_2 = np.asarray(valid_t_list['2'], dtype='int')
        x_valid_t_3 = np.asarray(valid_t_list['3'], dtype='int')
    
        x_valid_r = np.asarray(valid_r_list, dtype='int')
        y_valid = np.asarray(valid_y_list, dtype='int')
    
    else:
        split = int(len(train_y_list)*0.8)
        #generate the input arrays
        x_train_s_1 = np.asarray(train_s_list['1'][:split], dtype='int')
        x_train_s_2 = np.asarray(train_s_list['2'][:split], dtype='int')
        x_train_s_3 = np.asarray(train_s_list['3'][:split], dtype='int')
    
        x_train_t_1 = np.asarray(train_t_list['1'][:split], dtype='int')
        x_train_t_2 = np.asarray(train_t_list['2'][:split], dtype='int')
        x_train_t_3 = np.asarray(train_t_list['3'][:split], dtype='int')
    
        x_train_r = np.asarray(train_r_list[:split], dtype='int')
        y_train = np.asarray(train_y_list[:split], dtype='int')
    
        #generate the validation arrays
        x_valid_s_1 = np.asarray(train_s_list['1'][split:], dtype='int')
        x_valid_s_2 = np.asarray(train_s_list['2'][split:], dtype='int')
        x_valid_s_3 = np.asarray(train_s_list['3'][split:], dtype='int')
    
        x_valid_t_1 = np.asarray(train_t_list['1'][split:], dtype='int')
        x_valid_t_2 = np.asarray(train_t_list['2'][split:], dtype='int')
        x_valid_t_3 = np.asarray(train_t_list['3'][split:], dtype='int')
    
        x_valid_r = np.asarray(train_r_list[split:], dtype='int')
        y_valid = np.asarray(train_y_list[split:], dtype='int')
    
    #do the training
    model_2.fit([x_train_s_1, x_train_s_2, x_train_s_3, x_train_t_1, x_train_t_2, x_train_t_3, x_train_r], y_train, 
              validation_data=([x_valid_s_1, x_valid_s_2, x_valid_s_3, x_valid_t_1, x_valid_t_2, x_valid_t_3, x_valid_r], y_valid),
              batch_size=batch_size, epochs=num_epoch)
    
    # Save model and weights
    one_hop_add_h5 = one_hop_model_name + '.h5'
    one_hop_save_dir = os.path.join(os.getcwd(), './weight_bin')
    
    if not os.path.isdir(one_hop_save_dir):
        os.makedirs(one_hop_save_dir)
    one_hop_model_path = os.path.join(one_hop_save_dir, one_hop_add_h5)
    model_2.save(one_hop_model_path)
    print('Save model')
    del(model_2, Dict_train, Dict_valid)




