import numpy as np
import tqdm
from collections import deque
class Edge:
    def __init__(self, node1, node2, relationship):
        self.node1 = node1
        self.node2 = node2
        self.relationship = relationship
    def __eq__(self, other):
        return self.node1.value == other.node1.value and self.node2.value == other.node2.value and self.relationship.name == other.relationship.name 
    def __hash__(self):
        return hash(self.node1.value + self.node2.value + self.relationship.name) 
    def __str__(self):
        return f"{str(self.node1)} {str(self.relationship)}  {str(self.node2)}"
class Relationship:
    """Hashable relationship with its name as ID"""
    def __init__(self, name):
        self.name = name.strip()
    def __eq__(self, n1):
        """Relationships are equal if they have the same name"""
        return self.name == n1.name 
    def __str__(self):
        return self.name
    def __hash__(self):
        """Relationship name identifies the relationship"""
        return hash(self.name) 
class Node:
    """Hashable node with its value as ID"""
    def __init__(self, value):
        self.value = value.strip()
    def __str__(self):
        return self.value
    """Nodes are equal if they have the same value"""
    def __eq__(self, n1):
        return self.value == n1.value
    """Node value identifies the node"""
    def __hash__(self):
        return hash(self.value) 
class Graph:
    """Graph stucture"""
    def __init__(self):
        """Initialize the graph"""
        #sets because we need efficient lookups (O(1) for set O(n) for list)
        self.nodes = set()
        self.relationships = set()
        self.edges = []
        self.node2id = {}
        self.rel2id = {}
    def add_triple(self, s_val, o_val, p_val):
        """Add a single triple to the graph."""
        #set is comprised of unique element, adding an existing element doesn't affect the set 
        self.nodes.add(s_val)
        self.nodes.add(o_val)
        self.relationships.add(p_val)
        self.edges.append(Edge(s_val, o_val, p_val))

    def finalize(self):
        """Build indices once after all data is loaded."""
        self.node2id = {node: i for i, node in enumerate(self.nodes)}
        # nodes_n = len(list(self.node2id))
        self.rel2id = {rel: i for i, rel in enumerate(self.relationships)}
        self.adj = {node: [] for node in self.nodes}
        for e in self.edges:
            self.adj[e.node1].append((e.node2, e.relationship))
    def find_l1_paths(self, node):
        pass
    def paths2dataset(self):
        pass      
    def find_paths(self, s, t):
        """Optimized iterative path finding with max depth 6. This function is slow for our problem, moving implementation to C++ with paralellization."""
        all_paths = []
        # Queue stores: (current_node, current_path, visited_set)
        queue = deque([(s, [(s, None)], {s})])
        
        while queue:
            curr_node, path, visited = queue.popleft()
            
            # Stop if path exceeds max length
            if len(path) > 6:
                continue
            # if len(all_paths) == 3:
            #     continue
            for neighbor, rel in self.adj.get(curr_node, []):
                if neighbor == t:
                    # Path length must be > 1 (more than 2 nodes in list)
                    if len(path) > 2:
                        all_paths.append(path + [(neighbor, rel)])
                    continue # Found target, don't need to go deeper from here (simple path)
                
                if neighbor not in visited and len(path) < 4:
                    # Use set union for efficiency in creating the next visited set
                    queue.append((neighbor, path + [(neighbor, rel)], visited | {neighbor}))
        return all_paths
    def graph2dataset(self, negatives = True):
        """Convert data to numpy arrays compatible as neural net input"""
        try:
            pos_triples = np.array([
                [self.node2id[e.node1], self.node2id[e.node2], self.rel2id[e.relationship]] 
                for e in self.edges
            ], dtype=np.int32)
        except KeyError as ke:
            print(f"KeyError: {ke}. This likely means that the graph contains a node or relationship that was not properly indexed. Please check that all nodes and relationships have been added to the graph and that finalize() has been called.")
            raise ke

        num_pos = len(pos_triples)
        num_nodes = len(self.nodes)
        num_rels = len(self.relationships)
        # We over-sample by 10% to account for accidental "real" edges being picked
        oversample_factor = 1.1
        num_to_sample = int(num_pos * oversample_factor)

        neg_subs = np.random.randint(0, num_nodes, num_to_sample)
        neg_objs = np.random.randint(0, num_nodes, num_to_sample)
        neg_rels = np.random.randint(0, num_rels, num_to_sample)
        
        neg_triples = np.stack([neg_subs, neg_objs, neg_rels], axis=1)

        #Pruning: Filter out samples where sub == obj or triple exists in positive set

        def hash_triples(triples):
            # Maps (s, o, r) to a single unique integer
            return triples[:, 0] * (num_nodes * num_rels) + triples[:, 1] * num_rels + triples[:, 2]

        pos_hashes = set(hash_triples(pos_triples))
        neg_hashes = hash_triples(neg_triples)
        mask = np.array([(h not in pos_hashes) for h in neg_hashes])
        mask &= (neg_triples[:, 0] != neg_triples[:, 1])
        valid_negatives = neg_triples[mask][:num_pos]
        X = np.vstack([pos_triples, valid_negatives])
        y = np.concatenate([np.ones(num_pos), np.zeros(len(valid_negatives))])

        return X, y
    def graph2testset(self):
        """Convert data to numpy arrays compatible as neural net input"""
        pos_triples = np.array([
            [self.node2id[e.node1], self.node2id[e.node2], self.rel2id[e.relationship]] 
            for e in self.edges if e.relationship.name == 'rowDerivedFrom' or e.relationship.name == 'rowDerivedFrom_inverse'
        ], dtype=np.int32)
        for rel in self.rel2id:

            if rel.name == 'rowDerivedFrom':
                d1rel = rel
            elif rel.name == 'rowDerivedFrom_inverse':
                d2rel = rel

        print(d2rel.name, d1rel.name, self.rel2id[d2rel], self.rel2id[d1rel])
        rowNodes = [node for node in self.nodes if 'row' in node.value]
        neg_triples = []

        while neg_triples.__len__() < 2500:
            subj = np.random.choice(rowNodes, size=1, replace=False)[0]
            obj = np.random.choice(rowNodes, size=1, replace=False)[0]
            clear = True
            for t in pos_triples:
                if t[0] == self.node2id[subj] and t[1] == self.node2id[obj] and (t[2] == self.rel2id[d1rel] or t[2] == self.rel2id[d2rel]):
                    clear = False
                    break     
            for t in neg_triples:
                if t[0] == self.node2id[subj] and t[1] == self.node2id[obj] and (t[2] == self.rel2id[d1rel] or t[2] == self.rel2id[d2rel]):
                    clear = False
                    break                     
            if clear:
                neg_triples.append([self.node2id[subj], self.node2id[obj], self.rel2id[d1rel]])
        while neg_triples.__len__() < 5000:
            subj = np.random.choice(rowNodes, size=1, replace=False)[0]
            obj = np.random.choice(rowNodes, size=1, replace=False)[0]
            clear = True
            for t in pos_triples:
                if t[0] == self.node2id[subj] and t[1] == self.node2id[obj] and (t[2] == self.rel2id[d1rel] or t[2] == self.rel2id[d2rel]):
                    clear = False
                    break  
            for t in neg_triples:
                if t[0] == self.node2id[subj] and t[1] == self.node2id[obj] and (t[2] == self.rel2id[d1rel] or t[2] == self.rel2id[d2rel]):
                    clear = False
                    break  
            if clear:
                neg_triples.append([self.node2id[subj], self.node2id[obj], self.rel2id[d2rel]])

        neg_triples = np.array(neg_triples, dtype=np.int32)
        X = np.vstack([pos_triples, neg_triples])
        y = np.concatenate([np.ones(len(pos_triples)), np.zeros(len(neg_triples))])        
        return X, y