from tqdm import tqdm
from graph import Graph, Node, Relationship

g = Graph()  
experiment = open('experiment', 'r').read().strip()
l_bar = '{desc}: {percentage:.3f}%|'
r_bar = '| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, ' '{rate_fmt}{postfix}]'
format = '{l_bar}{bar}{r_bar}'

data = open(f'../../csv/kgs/{experiment}/test.txt', encoding='utf8').readlines()  
for line in tqdm(data, ncols=100, bar_format=format):
    s,p,o = line.split('\t')
    g.add_triple(Node(s), Node(o), Relationship(p))
    
data = open(f'../../csv/kgs/{experiment}/test_gold.txt', encoding='utf8').readlines()  
for line in tqdm(data, ncols=100, bar_format=format):
    s,p,o = line.split('\t')
    g.add_triple(Node(s), Node(o), Relationship(p))
g.finalize()

rel2id_file = open(f'../../csv/kgs/{experiment}/rel2id.data')
rel2id = {}
for line in rel2id_file.readlines():
    rel, idx = line.split()
    rel2id[Relationship(rel)] = int(idx)
rel2id_file.close()
g.rel2id = rel2id

X,y = g.graph2dataset(negatives = False)

triples_file = open(f'../../csv/kgs/{experiment}/triples.data.inductive', 'w+')
for i,item in enumerate(X):
    triples_file.write(f"{int(y[i])} {int(item[0])} {int(item[1])} {int(item[2])}\n")
triples_file.close()

X,y = g.graph2testset()

triples_file = open(f'../../csv/kgs/{experiment}/test.triples.data', 'w+')
for i,item in enumerate(X):
    triples_file.write(f"{int(y[i])} {int(item[0])} {int(item[1])} {int(item[2])}\n")
triples_file.close()