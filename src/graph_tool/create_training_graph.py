from graph import Graph, Node, Relationship
from tqdm import tqdm   

experiment = open('experiment', 'r').read().strip()

g = Graph()  
l_bar = '{desc}: {percentage:.3f}%|'
r_bar = '| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, ' '{rate_fmt}{postfix}]'
format = '{l_bar}{bar}{r_bar}'

data = open(f'../../csv/kgs/{experiment}/train.txt', encoding='utf8').readlines()  
for line in tqdm(data, ncols=100, bar_format=format):
    s,p,o = line.split('\t')
    g.add_triple(Node(s), Node(o), Relationship(p))
g.finalize()
X,y = g.graph2dataset()

triples_file = open(f'../../csv/kgs/{experiment}/triples.data', 'w+')
for i,item in enumerate(X):
    triples_file.write(f"{int(y[i])} {int(item[0])} {int(item[1])} {int(item[2])}\n")
triples_file.close()

rel2id_file = open(f'../../csv/kgs/{experiment}/rel2id.data', 'w+')
for rel, idx in g.rel2id.items():
    rel2id_file.write(f"{rel.name} {idx}\n")
rel2id_file.close()