import tensorflow
from tensorflow.python.client import device_lib
print(device_lib.list_local_devices())

from tqdm import tqdm
import numpy as np
import random
from collections import deque
from network_models.vae import create_vae_model
from network_models.attention import create_gat_model
from network_models.siamese import create_siamese_model
from network_models.autoencoder import create_autoencoder
from network_models.gan import create_gan_components
experiment = open('experiment', 'r').read().strip()
network = open('network', 'r').read().strip()

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
Y_reshaped = np.array(y_conn_based).reshape(-1, 1)
num_nodes = 41
num_rels = 41

m = create_siamese_model(num_nodes=41, num_rels=41)
if network == 'vae':
    m = create_vae_model(num_nodes=41, num_rels=41)
if network == 'siamese':
    m = create_siamese_model(num_nodes=41, num_rels=41)
if network == 'gat':
    m = create_gat_model(num_nodes=41, num_rels=41)
if network == 'autoencoder':
    m = create_autoencoder(num_nodes=41, num_rels=41)


if network == 'vae' or network == 'autoencoder':
    try: 
        m.fit(
            x={
                "p1_input": np.array(X_p1),
                "p2_input": np.array(X_p2),
                "p3_input": np.array(X_p3),
                "rel_input": np.array(X_rel)
            },
            y=[Y_reshaped, np.array(X_p1), np.array(X_p2), np.array(X_p3)],
            epochs=3,
            batch_size=32,

        )
    except Exception as e:
        print(e)
if network == 'siamese' or network == 'gat':
    try: 
        m.fit(
            x={
                "p1_input": np.array(X_p1),
                "p2_input": np.array(X_p2),
                "p3_input": np.array(X_p3),
                "rel_input": np.array(X_rel)
            },
            y=Y_reshaped,
            epochs=4,
            batch_size=32,

        )
    except Exception as e:
        print(e)

m.save(f'../../csv/kgs/{experiment}/model_{network}.h5')