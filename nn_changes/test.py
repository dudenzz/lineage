import os
import sys
import numpy as np
import random
import argparse
import pickle
import tensorflow as tf
from collections import defaultdict
from copy import deepcopy
from sklearn.utils import shuffle
from tqdm import tqdm


from tensorflow import keras

from sklearn import datasets, metrics
#from sklearn.model_selection import train_test_split
#from sklearn.linear_model import LogisticRegression
#from sklearn.metrics import average_precision_score, precision_recall_curve
from sklearn.metrics import auc

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
def get_max(t):
    import tensorflow as tf
    return tf.reduce_max(t, axis=1)
def get_sum(t):
    import tensorflow as tf
    return tf.reduce_sum(t, axis=1)
def l2norm(t):
    import tensorflow as tf
    return tf.math.l2_normalize(t, axis=1)


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
            print('s', s)
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


#the function to do path-based relation scoring
def path_based_relation_scoring(s, t, lower_bd, upper_bd, one_hop, id2relation, model):
    
    path_holder = set()
    
    for iteration in range(3):
    
        result, length_dict = Class_2.obtain_paths('target_specified', 
                                                   s, t, lower_bd, upper_bd, one_hop)
        if t in result:
            
            for path in result[t]:
                
                path_holder.add(path)
                
        del(result, length_dict)
    
    path_holder = list(path_holder)
    random.shuffle(path_holder)
    
    score_dict = defaultdict(float)
    count_dict = defaultdict(int)
    
    count = 0
    
    if len(path_holder) >= 3:
    
        #iterate over path_1
        while count < 10:

            temp_pair = random.sample(path_holder, 3)

            path_1, path_2, path_3 = temp_pair[0], temp_pair[1], temp_pair[2]

            list_1 = list()
            list_2 = list()
            list_3 = list()
            list_r = list()

            for i in range(len(id2relation)):

                if i not in id2relation:

                    raise ValueError ('error when generating id2relation')
                
                #only care about initial relations
                if i % 2 == 0:

                    list_1.append(list(path_1) + [num_r]*abs(len(path_1)-upper_bd))
                    list_2.append(list(path_2) + [num_r]*abs(len(path_2)-upper_bd))
                    list_3.append(list(path_3) + [num_r]*abs(len(path_3)-upper_bd))
                    list_r.append([i])
            
            #change to arrays
            input_1 = np.array(list_1)
            input_2 = np.array(list_2)
            input_3 = np.array(list_3)
            input_r = np.array(list_r)

            pred = model.predict([input_1, input_2, input_3, input_r], verbose = 0)

            for i in range(pred.shape[0]):
                #need to times 2 to go back to relation id from pred position
                score_dict[2*i] += float(pred[i])
                count_dict[2*i] += 1

            count += 1
            
    #average the score
    for r in score_dict:
        score_dict[r] = deepcopy(score_dict[r]/float(count_dict[r]))
    
    #print(len(score_dict), len(path_holder))

    return(score_dict)


#the function to do path-based triple scoring: input one triple
def path_based_triple_scoring(s, r, t, lower_bd, upper_bd, one_hop, id2relation, model):
    
    path_holder = set()
    
    for iteration in range(3):
    
        result, length_dict = Class_2.obtain_paths('target_specified', 
                                                   s, t, lower_bd, upper_bd, one_hop)
        if t in result:
            
            for path in result[t]:
                
                path_holder.add(path)
                
        del(result, length_dict)
    
    path_holder = list(path_holder)
    random.shuffle(path_holder)
    
    score = 0.
    count = 0
    
    if len(path_holder) >= 3:
        
        list_1 = list()
        list_2 = list()
        list_3 = list()
        list_r = list()
    
        #iterate over path_1
        while count < 10:

            temp_pair = random.sample(path_holder, 3)
            path_1, path_2, path_3 = temp_pair[0], temp_pair[1], temp_pair[2]

            list_1.append(list(path_1) + [num_r]*abs(len(path_1)-upper_bd))
            list_2.append(list(path_2) + [num_r]*abs(len(path_2)-upper_bd))
            list_3.append(list(path_3) + [num_r]*abs(len(path_3)-upper_bd))
            list_r.append([r])
            
            count += 1
            
        #change to arrays
        input_1 = np.array(list_1)
        input_2 = np.array(list_2)
        input_3 = np.array(list_3)
        input_r = np.array(list_r)

        pred = model.predict([input_1, input_2, input_3, input_r], verbose = 0)

        for i in range(pred.shape[0]):
            score += float(pred[i])
            
        #average the score
        score = score/float(count)

    return(score)


#subgraph based relation scoring
def subgraph_relation_scoring(s, t, lower_bd, upper_bd, one_hop, id2relation, model_2):
    
    path_s, path_t = set(), set() #sets holding all the paths from s or t
    
    for iteration in range(3):
    
        #obtain the paths out from s or t by "any target" mode. That is, 
        result_s, length_dict_s = Class_2.obtain_paths('any_target', s, 'any', lower_bd, upper_bd, one_hop)
        result_t, length_dict_t = Class_2.obtain_paths('any_target', t, 'any', lower_bd, upper_bd, one_hop)

        #add paths to the source/target path_set
        for e in result_s:
            for path in result_s[e]:
                path_s.add(path)
        for e in result_t:
            for path in result_t[e]:
                path_t.add(path)
                
        del(result_s, length_dict_s, result_t, length_dict_t)
    
    #final output: the score dict
    score_dict = defaultdict(float)
    count_dict = defaultdict(int)
    
    #see if both path_s and path_t have at least three paths
    if len(path_s) >= 3 and len(path_t) >= 3:

        #change to lists
        path_s, path_t = list(path_s), list(path_t)
        
        count = 0
        while count < 10:
            
            #lists holding the input to the network
            list_s_1 = list()
            list_s_2 = list()
            list_s_3 = list()
            list_t_1 = list()
            list_t_2 = list()
            list_t_3 = list()
            list_r = list()

            #randomly obtain three paths
            temp_s = random.sample(path_s, 3)
            temp_t = random.sample(path_t, 3)
            s_p_1, s_p_2, s_p_3 = temp_s[0], temp_s[1], temp_s[2]
            t_p_1, t_p_2, t_p_3 = temp_t[0], temp_t[1], temp_t[2]
            
            #add all forward (initial relation)
            for i in range(len(id2relation)):

                if i not in id2relation:

                    raise ValueError ('error when generating id2relation')
                    
                if i % 2 == 0:

                    #append the paths: note that we add the space holder id at the end of the shorter path
                    list_s_1.append(list(s_p_1) + [num_r]*abs(len(s_p_1)-upper_bd))
                    list_s_2.append(list(s_p_2) + [num_r]*abs(len(s_p_2)-upper_bd))
                    list_s_3.append(list(s_p_3) + [num_r]*abs(len(s_p_3)-upper_bd))
                    
                    list_t_1.append(list(t_p_1) + [num_r]*abs(len(t_p_1)-upper_bd))
                    list_t_2.append(list(t_p_2) + [num_r]*abs(len(t_p_2)-upper_bd))
                    list_t_3.append(list(t_p_3) + [num_r]*abs(len(t_p_3)-upper_bd))
                    
                    list_r.append([i])
                
            #change to arrays
            input_s_1 = np.array(list_s_1)
            input_s_2 = np.array(list_s_2)
            input_s_3 = np.array(list_s_3)
            input_t_1 = np.array(list_t_1)
            input_t_2 = np.array(list_t_2)
            input_t_3 = np.array(list_t_3)
            input_r = np.array(list_r)
            
            pred = model_2.predict([input_s_1, input_s_2, input_s_3,
                                    input_t_1, input_t_2, input_t_3, input_r], verbose = 0)

            for i in range(pred.shape[0]):
                #need to times 2 to go back to relation id from pred position
                score_dict[2*i] += float(pred[i])
                count_dict[2*i] += 1

            count += 1
            
    #average the score
    for r in score_dict:
        score_dict[r] = deepcopy(score_dict[r]/float(count_dict[r]))
            
    #print(len(score_dict), len(path_s), len(path_t))
        
    return(score_dict)


#subgraph based triple scoring
def subgraph_triple_scoring(s, r, t, lower_bd, upper_bd, one_hop, id2relation, model_2):
    
    path_s, path_t = set(), set() #sets holding all the paths from s or t
    
    for iteration in range(3):
    
        #obtain the paths out from s or t by "any target" mode. That is, 
        result_s, length_dict_s = Class_2.obtain_paths('any_target', s, 'any', lower_bd, upper_bd, one_hop)
        result_t, length_dict_t = Class_2.obtain_paths('any_target', t, 'any', lower_bd, upper_bd, one_hop)

        #add paths to the source/target path_set
        for e in result_s:
            for path in result_s[e]:
                path_s.add(path)
        for e in result_t:
            for path in result_t[e]:
                path_t.add(path)
                
        del(result_s, length_dict_s, result_t, length_dict_t)
    
    #final output: the score dict
    score = 0.
    
    #see if both path_s and path_t have at least three paths
    if len(path_s) >= 3 and len(path_t) >= 3:

        #change to lists
        path_s, path_t = list(path_s), list(path_t)
        
        #lists holding the input to the network
        list_s_1 = list()
        list_s_2 = list()
        list_s_3 = list()
        list_t_1 = list()
        list_t_2 = list()
        list_t_3 = list()
        list_r = list()
        
        count = 0
        while count < 10:

            #randomly obtain three paths
            temp_s = random.sample(path_s, 3)
            temp_t = random.sample(path_t, 3)
            s_p_1, s_p_2, s_p_3 = temp_s[0], temp_s[1], temp_s[2]
            t_p_1, t_p_2, t_p_3 = temp_t[0], temp_t[1], temp_t[2]

            #append the paths: note that we add the space holder id at the end of the shorter path
            list_s_1.append(list(s_p_1) + [num_r]*abs(len(s_p_1)-upper_bd))
            list_s_2.append(list(s_p_2) + [num_r]*abs(len(s_p_2)-upper_bd))
            list_s_3.append(list(s_p_3) + [num_r]*abs(len(s_p_3)-upper_bd))

            list_t_1.append(list(t_p_1) + [num_r]*abs(len(t_p_1)-upper_bd))
            list_t_2.append(list(t_p_2) + [num_r]*abs(len(t_p_2)-upper_bd))
            list_t_3.append(list(t_p_3) + [num_r]*abs(len(t_p_3)-upper_bd))

            list_r.append([r])
            count += 1
                
        #change to arrays
        input_s_1 = np.array(list_s_1)
        input_s_2 = np.array(list_s_2)
        input_s_3 = np.array(list_s_3)
        input_t_1 = np.array(list_t_1)
        input_t_2 = np.array(list_t_2)
        input_t_3 = np.array(list_t_3)
        input_r = np.array(list_r)

        pred = model_2.predict([input_s_1, input_s_2, input_s_3,
                                input_t_1, input_t_2, input_t_3, input_r], verbose = 0)

        for i in range(pred.shape[0]):
            score += float(pred[i])

        #average the score
        score = score/float(count)
        
    return(score)



###################################
####implementation#################

if __name__ == "__main__":
    
    
    ##################################
    ####original parameters###########
    parser = argparse.ArgumentParser(description="Run the training model with specified parameters.")
    parser.add_argument('--data_name', type=str, required=True, help='The name of the dataset')
    parser.add_argument('--model_name', type=str, required=True, help='The name of the model')

    args = parser.parse_args()
    
    data_name = args.data_name 
    model_name = args.model_name
    
    lower_bound = 1 #the lower bound on path length, shared by both the connection-based model and sub-graph based model
    upper_bound_path = 10 #the upper bound on path length, for connection-based model
    upper_bound_subg = 3 #the upper bound on path length, for subgraph-based model
    
    
    #difine the names for checkpoint saving
    model_name_ = 'Model_' + model_name + '_' + data_name
    one_hop_model_name = 'One_hop_model_' + model_name + '_' + data_name
    ids_name = 'IDs_' + model_name + '_' + data_name
    #################################

    #load the classes
    Class_1 = LoadKG()
    Class_2 = ObtainPathsByDynamicProgramming()


    #load ids and relation/entity dicts
    with open('./weight_bin/' + ids_name + '.pickle', 'rb') as handle:
        Dict = pickle.load(handle)
        
    #save training data
    one_hop = Dict['one_hop']
    data = Dict['data']
    s_t_r = Dict['s_t_r']
    
    #save valid data
    one_hop_valid = Dict['one_hop_valid']
    data_valid = Dict['data_valid']
    s_t_r_valid = Dict['s_t_r_valid']
    
    #save test data
    one_hop_test = Dict['one_hop_test']
    data_test = Dict['data_test']
    s_t_r_test = Dict['s_t_r_test']
    
    #save shared dictionaries
    entity2id = Dict['entity2id']
    id2entity = Dict['id2entity']
    relation2id = Dict['relation2id']
    id2relation = Dict['id2relation']
    
    #we want to keep the initial entity/relation dicts before adding new entities
    entity2id_ini = deepcopy(entity2id)
    id2entity_ini = deepcopy(id2entity)
    relation2id_ini = deepcopy(relation2id)
    id2relation_ini = deepcopy(id2relation)
    
    num_r = len(id2relation)
    num_r
    keras.config.enable_unsafe_deserialization()
    #load the model
    model = keras.models.load_model('./weight_bin/' + model_name_ + '.h5', safe_mode=False,     custom_objects={
        'get_max': get_max,
        'get_sum': get_sum,
        'l2norm': l2norm
    })


    #load the one-hop neighbor model
    model_2 = keras.models.load_model('./weight_bin/' + one_hop_model_name + '.h5', safe_mode=False,     custom_objects={
        'get_max': get_max,
        'get_sum': get_sum,
        'l2norm': l2norm
    })

    ind_train_path = './data/' + data_name + '_ind/train.txt'
    ind_valid_path = './data/' + data_name + '_ind/valid.txt'
    ind_test_path = './data/' + data_name + '_ind/test.txt'


    #load the test dataset
    one_hop_ind = dict() 
    data_ind = set()
    s_t_r_ind = defaultdict(set)
    
    len_0 = len(relation2id)
    size_0 = len(entity2id)
    
    #fill in the sets and dicts
    Class_1.load_train_data(ind_train_path, 
                            one_hop_ind, data_ind, s_t_r_ind,
                            entity2id, id2entity, relation2id, id2relation)
    # print(id2entity)
    len_1 = len(relation2id)
    size_1 = len(entity2id)
    
    if len_0 != len_1:
        raise ValueError('unseen relation!')

    #load the test dataset
    one_hop_ind_test = dict() 
    data_ind_test = set()
    s_t_r_ind_test = defaultdict(set)
    
    len_0 = len(relation2id)
    size_0 = len(entity2id)
    
    #fill in the sets and dicts
    Class_1.load_train_data(ind_test_path, 
                            one_hop_ind_test, data_ind_test, s_t_r_ind_test,
                            entity2id, id2entity, relation2id, id2relation)
    
    
    len_1 = len(relation2id)
    size_1 = len(entity2id)
    
    if len_0 != len_1:
        raise ValueError('unseen relation!')


    #load the validation for existing triple removal when ranking
    one_hop_ind_valid = dict() 
    data_ind_valid = set()
    s_t_r_ind_valid = defaultdict(set)
    
    len_0 = len(relation2id)
    size_0 = len(entity2id)
    
    #fill in the sets and dicts
    Class_1.load_train_data(ind_valid_path, 
                            one_hop_ind_valid, data_ind_valid, s_t_r_ind_valid,
                            entity2id, id2entity, relation2id, id2relation)
    
    len_1 = len(relation2id)
    size_1 = len(entity2id)
    
    if len_0 != len_1:
        raise ValueError('unseen relation!')


    #obtain all the inital entities and new entities
    ini_ent_set, new_ent_set, all_ent_set = set(), set(), set()
    
    for ID in id2entity:
        all_ent_set.add(ID)
        if ID in id2entity_ini:
            ini_ent_set.add(ID)
        else:
            new_ent_set.add(ID)
            
    print(len(ini_ent_set), len(new_ent_set), len(all_ent_set))
    
    
    
    ##########################################################
    ##obtain the AUC-PR for the test triples, using sklearn###
    
    #we select all the triples in the inductive test set
    pos_triples = list(data_ind_test)
    
    #we build the negative samples by randomly replace head or tail entity in the triple.
    neg_triples = list()
    
    for i in tqdm(range(len(pos_triples)), desc='relation corrupted ranking: generating test data'):
        
        s_pos, r_pos, t_pos = pos_triples[i][0], pos_triples[i][1], pos_triples[i][2]
        
        #decide to replace the head or tail entity
        number_0 = random.uniform(0, 1)
        
        if number_0 < 0.5: #replace head entity
            
            s_neg = random.choice(list(ini_ent_set))
            
            #filter out the existing triples
            while ((s_neg, r_pos, t_pos) in data_test) or (
                   (s_neg, r_pos, t_pos) in data_valid) or (
                   (s_neg, r_pos, t_pos) in data) or (
                   (s_neg, r_pos, t_pos) in data_ind) or (
                   (s_neg, r_pos, t_pos) in data_ind_valid) or (
                   (s_neg, r_pos, t_pos) in data_ind_test):
                
                s_neg = random.choice(list(ini_ent_set))
            
            neg_triples.append((s_neg, r_pos, t_pos))
        
        else: #replace tail entity
    
            t_neg = random.choice(list(ini_ent_set))
            
            #filter out the existing triples
            while ((s_pos, r_pos, t_neg) in data_test) or (
                   (s_pos, r_pos, t_neg) in data_valid) or (
                   (s_pos, r_pos, t_neg) in data) or (
                   (s_pos, r_pos, t_neg) in data_ind) or (
                   (s_pos, r_pos, t_neg) in data_ind_valid) or (
                   (s_pos, r_pos, t_neg) in data_ind_test):
                
                t_neg = random.choice(list(ini_ent_set))
            
            neg_triples.append((s_pos, r_pos, t_neg))
    
    if len(pos_triples) != len(neg_triples):
        raise ValueError('error when generating negative triples')
            
    #combine all triples
    all_triples = pos_triples + neg_triples
    
    #obtain the label array
    arr1 = np.ones((len(pos_triples),))
    arr2 = np.zeros((len(neg_triples),))
    y_test = np.concatenate((arr1, arr2))
    
    #shuffle positive and negative triples (optional)
    all_triples, y_test = shuffle(all_triples, y_test)
    
    #obtain the score aray
    y_score = np.zeros((len(y_test),))
    
    #implement the scoring
    # for i in tqdm(range(len(all_triples)), desc='relation corrupted ranking: evaluating'):
        
    #     s, r, t = all_triples[i][0], all_triples[i][1], all_triples[i][2]
        
    #     #path_score = path_based_triple_scoring(s, r, t, lower_bound, upper_bound_path, one_hop_ind, id2relation, model)
        
    #     subg_score = subgraph_triple_scoring(s, r, t, lower_bound, upper_bound_subg, one_hop_ind, id2relation, model_2)
        
    #     #ave_score = (path_score + subg_score)/float(2)
        
    #     #y_score[i] = ave_score
    #     y_score[i] = subg_score
        
    #     if i % 20 == 0 and i > 0:
    #         #print('evaluating scores', i, len(all_triples))
    #         auc_ = metrics.roc_auc_score(y_test[:i], y_score[:i])
    #         auc_pr = metrics.average_precision_score(y_test[:i], y_score[:i])
    #         #print('auc, auc-pr', auc_, auc_pr)
            
    # #print('evaluating scores', i, len(all_triples))
    # auc_ = metrics.roc_auc_score(y_test, y_score)
    # auc_pr = metrics.average_precision_score(y_test, y_score)
    # print('AUC', auc_)
    # print('AUC-PR', auc_pr)
    


    ######################################################
    #obtain the Hits@N for entity prediction##############
    
    #we select all the triples in the inductive test set
    selected = list(data_ind_test)
    
    ###Hit at 1#############################
    #generate the negative samples by randomly replace relation with all the other relaiton
    Hits_at_1 = 0
    Hits_at_3 = 0
    Hits_at_10 = 0
    MRR_raw = 0.
    
    for i in tqdm(range(len(selected)), desc='Hits@N for entity corrupted ranking'):
        
        triple_list = list()
        
        #score the true triple
        s_pos, r_pos, t_pos = selected[i][0], selected[i][1], selected[i][2]
    
        #path_score = path_based_triple_scoring(s_pos, r_pos, t_pos, lower_bound, upper_bound_path, one_hop_ind, id2relation, model)
    
        subg_score = subgraph_triple_scoring(s_pos, r_pos, t_pos, lower_bound, upper_bound_subg, one_hop_ind, id2relation, model_2)
        
        #ave_score = (path_score + subg_score)/float(2)
        
        triple_list.append([(s_pos, r_pos, t_pos), subg_score])
        
        #generate the 50 random samples
        for sub_i in range(50):
            
            #decide to replace the head or tail entity
            number_0 = random.uniform(0, 1)
    
            if number_0 < 0.5: #replace head entity
                
                s_neg = random.choice(list(ini_ent_set))
                
                while ((s_neg, r_pos, t_pos) in data_test) or (
                       (s_neg, r_pos, t_pos) in data_valid) or (
                       (s_neg, r_pos, t_pos) in data) or (
                       (s_neg, r_pos, t_pos) in data_ind) or (
                       (s_neg, r_pos, t_pos) in data_ind_valid) or (
                       (s_neg, r_pos, t_pos) in data_ind_test):
    
                    s_neg = random.choice(list(ini_ent_set))
                
                #path_score = path_based_triple_scoring(s_neg, r_pos, t_pos, lower_bound, upper_bound_path, one_hop_ind, id2relation, model)
    
                #subg_score = subgraph_triple_scoring(s_neg, r_pos, t_pos, lower_bound, upper_bound_subg, one_hop_ind, id2relation, model_2)
    
                #ave_score = (path_score + subg_score)/float(2)
    
                triple_list.append([(s_neg, r_pos, t_pos), 0])
                
            else: #replace tail entity
    
                t_neg = random.choice(list(ini_ent_set))
                
                #filter out the existing triples
                while ((s_pos, r_pos, t_neg) in data_test) or (
                       (s_pos, r_pos, t_neg) in data_valid) or (
                       (s_pos, r_pos, t_neg) in data) or (
                       (s_pos, r_pos, t_neg) in data_ind) or (
                       (s_pos, r_pos, t_neg) in data_ind_valid) or (
                       (s_pos, r_pos, t_neg) in data_ind_test):
    
                    t_neg = random.choice(list(ini_ent_set))
                
                #path_score = path_based_triple_scoring(s_pos, r_pos, t_neg, lower_bound, upper_bound_path, one_hop_ind, id2relation, model)
    
                #subg_score = subgraph_triple_scoring(s_pos, r_pos, t_neg, lower_bound, upper_bound_subg, one_hop_ind, id2relation, model_2)
    
                #ave_score = (path_score + subg_score)/float(2)
    
                triple_list.append([(s_pos, r_pos, t_neg), 0])
                
        #random shuffle!
        random.shuffle(triple_list)
        
        #sort
        sorted_list = sorted(triple_list, key = lambda x: x[-1], reverse=True)
        
        p = 0
        
        while p < len(sorted_list) and sorted_list[p][0] != (s_pos, r_pos, t_pos):
                
            p += 1
        
        if p == 0:
            
            Hits_at_1 += 1
            
        if p < 3:
            
            Hits_at_3 += 1
            
        if p < 10:
            
            Hits_at_10 += 1
            
        MRR_raw += 1./float(p + 1.) 
            
    print('Hits@1', Hits_at_1/(i+1))
    print('Hits@3', Hits_at_3/(i+1))
    print('Hits@10 (reported in the paper)', Hits_at_10/(i+1))
    print('MRR', MRR_raw/(i+1))




    ########################################################
    #obtain the Hits@N for relation prediction##############
    
    #we select all the triples in the inductive test set
    selected = list(data_ind_test)
    
    ###Hit at 1#############################
    #generate the negative samples by randomly replace relation with all the other relaiton
    Hits_at_1 = 0
    Hits_at_3 = 0
    Hits_at_10 = 0
    MRR_raw = 0.
    
    for i in tqdm(range(len(selected)), desc='Hits@N for relation corrupted ranking'):
        
        s_true, r_true, t_true = selected[i][0], selected[i][1], selected[i][2]
        
        #run the path-based scoring
        score_dict_path = path_based_relation_scoring(s_true, t_true, lower_bound, upper_bound_path, one_hop_ind, id2relation, model)
        
        #run the one-hop neighbour based scoring
        score_dict_subg = subgraph_relation_scoring(s_true, t_true, lower_bound, upper_bound_subg, one_hop_ind, id2relation, model_2)
        
        #final score dict
        score_dict = defaultdict(float)
        
        for r in score_dict_path:
            score_dict[r] += score_dict_path[r]
        for r in score_dict_subg:
            score_dict[r] += score_dict_subg[r]
        
        #[... [score, r], ...]
        temp_list = list()
        
        for r in id2relation:
            
            #again, we only care about initial relation prediciton
            if r % 2 == 0:
            
                if r in score_dict:
    
                    temp_list.append([score_dict[r], r])
    
                else:
    
                    temp_list.append([0.0, r])
            
        sorted_list = sorted(temp_list, key = lambda x: x[0], reverse=True)
        
        p = 0
        exist_tri = 0
        
        while p < len(sorted_list) and sorted_list[p][1] != r_true:
            
            #moreover, we want to remove existing triples
            if ((s_true, sorted_list[p][1], t_true) in data_test) or (
                (s_true, sorted_list[p][1], t_true) in data_valid) or (
                (s_true, sorted_list[p][1], t_true) in data) or (
                (s_true, sorted_list[p][1], t_true) in data_ind) or (
                (s_true, sorted_list[p][1], t_true) in data_ind_valid) or (
                (s_true, sorted_list[p][1], t_true) in data_ind_test):
                
                exist_tri += 1
                
            p += 1
        
        if p - exist_tri == 0:
            
            Hits_at_1 += 1
            
        if p - exist_tri < 3:
            
            Hits_at_3 += 1
            
        if p - exist_tri < 10:
            
            Hits_at_10 += 1
            
        MRR_raw += 1./float(p - exist_tri + 1.) 
            
    print('Hits@1 (reported in the paper)', Hits_at_1/(i+1))
    print('Hits@3 (reported in the paper)', Hits_at_3/(i+1))
    print('Hits@10', Hits_at_10/(i+1))
    print('MRR', MRR_raw/(i+1))


