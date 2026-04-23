import tensorflow
from tensorflow.python.client import device_lib
print(device_lib.list_local_devices())

from tqdm import tqdm
import numpy as np
import random
from collections import deque

experiment = open('experiment', 'r').read().strip()

lines = open(f'../../csv/kgs/{experiment}/paths_output.train.txt', 'r').readlines()
i = 0
skips = 0
noskips = 0
X_conn_based_rels = []
X_conn_based_p1s = []
X_conn_based_p2s = []
X_conn_based_p3s = []
y_conn_based = []
while i < len(lines):
    [meta1, path1] = lines[i].strip().split(':')
    [pos1, start1, rel1, end1] = meta1.split()
    path1 = np.array(path1.split()).astype(np.int32)
    i += 1
    if i >= len(lines):
        skips += 1
        break
    [meta2, path2]  = lines[i].strip().split(':')
    [pos2, start2, rel2, end2] = meta2.split()
    path2 = np.array(path2.split()).astype(np.int32)
    if rel1 != rel2: 
        skips += 1
        continue
    i+=1
    if i >= len(lines):
        skips += 1
        break
    [meta3, path3]  = lines[i].strip().split(':')
    [pos3, start3, rel3, end3] = meta3.split()
    path3 = np.array(path3.split()).astype(np.int32)
    if rel2 != rel3: 
        skips += 1
        continue
    if i >= len(lines):
        skips += 1
        break
    i+=1
    noskips += 1
    y_conn_based.append(np.array(pos1).astype(np.int32))
    X_conn_based_p1s.append([path1,])
    X_conn_based_p2s.append([path2,])
    X_conn_based_p3s.append([path3,])
    X_conn_based_rels.append([np.array(rel1).astype(np.int32),])
print(f"skips: {skips} noskips: {noskips}")

from tensorflow.keras.layers import Input, Dense, Embedding, Concatenate, Dot, Normalization, Lambda, LSTM, Bidirectional, MaxPooling2D, Flatten, Reshape, GlobalMaxPooling1D
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.sequence import pad_sequences
import numpy as np
# Ensure every element is a simple Python list or Numpy array of integers
def clean_sequence(data):
    # This converts everything to a list of numpy arrays
    return [np.array(i).flatten() for i in data]

X_p1_clean = clean_sequence(X_conn_based_p1s)
X_p2_clean = clean_sequence(X_conn_based_p2s)
X_p3_clean = clean_sequence(X_conn_based_p3s)

# Now pad. We specify maxlen to be safe.
max_len = 10
#max_len = max(len(s) for s in X_p1_clean + X_p2_clean + X_p3_clean)
print(max_len)
X_p1 = pad_sequences(X_p1_clean, maxlen=max_len, padding='post')
X_p2 = pad_sequences(X_p2_clean, maxlen=max_len, padding='post')
X_p3 = pad_sequences(X_p3_clean, maxlen=max_len, padding='post')

# 2. Ensure your relation and labels are numpy arrays

# Convert to numpy and force the (batch, 1) shape
X_rel = np.array(X_conn_based_rels).reshape(-1, 1)
Y = np.array(y_conn_based) # Ensure labels are also a numpy array

num_nodes = 41
num_rels = 41



#input is not rectangular, we need separate inputs for each path
path1_inp = Input(shape=(10,), name="p1 input")
path2_inp = Input(shape=(10,), name="p2 input")
path3_inp = Input(shape=(10,), name="p3 input")
rel_inp = Input(shape=(1,), name="rel input")
#embedding Layers
node_embedding = Embedding(input_dim=num_rels, output_dim=300, name="Node_Embedding")
rel_embedding = Embedding(input_dim=num_rels, output_dim=300, name="Rel_Embedding")
p1_emb = node_embedding(path1_inp)
p2_emb = node_embedding(path2_inp)
p3_emb = node_embedding(path3_inp)
r_emb = rel_embedding(rel_inp)
#reshape in order to fit LSTM
p1_seq = Reshape((10, 300))(p1_emb)
p2_seq = Reshape((10, 300))(p2_emb)
p3_seq = Reshape((10, 300))(p3_emb)
#add 2 layer bi-directional LSTM
lstm_layer_1 = Bidirectional(LSTM(150, return_sequences=True))
lstm_layer_2 = Bidirectional(LSTM(150, return_sequences=True))
#first LSTM layer
fst_lstm_mid = lstm_layer_1(p1_seq)
scd_lstm_mid = lstm_layer_1(p2_seq)
trd_lstm_mid = lstm_layer_1(p3_seq)
#second LSTM layer
fst_lstm_fin = lstm_layer_2(fst_lstm_mid)
scd_lstm_fin = lstm_layer_2(scd_lstm_mid)
trd_lstm_fin = lstm_layer_2(trd_lstm_mid)
#reduce max
pool_layer = GlobalMaxPooling1D() 
fst_pooled = pool_layer(fst_lstm_fin)
scd_pooled = pool_layer(scd_lstm_fin)
trd_pooled = pool_layer(trd_lstm_fin)
#flatten
fst_final = Flatten()(fst_pooled)
scd_final = Flatten()(scd_pooled)
trd_final = Flatten()(trd_pooled)
#merge node embeddings with DNN
nodes_concat = Concatenate()([fst_final, scd_final, trd_final])
nodes_representation = Dense(300)(nodes_concat)
#normalization
rel_normalized = Normalization(axis=-1)(r_emb)
nodes_normalized = Normalization(axis=-1)(nodes_representation)
#edge probability
edge_probability = Dot(axes=-1)([rel_normalized, nodes_normalized])
edge_probability = Flatten()(edge_probability)
output = Dense(1, activation='sigmoid')(edge_probability)

m = Model(inputs = [path1_inp,path2_inp,path3_inp,rel_inp], outputs = output)
m.compile(loss='binary_crossentropy', metrics=['accuracy'])

try: 
    m.fit(
        x={
            "p1 input": np.array(X_p1),
            "p2 input": np.array(X_p2),
            "p3 input": np.array(X_p3),
            "rel input": np.array(X_conn_based_rels)
        },
        y=np.array(Y),
        epochs=3,
        batch_size=32,

    )
except Exception as e:
    print(e)

m.save(f'../../csv/kgs/{experiment}/model.h5')