import tensorflow
from tensorflow.python.client import device_lib
print(device_lib.list_local_devices())

from tqdm import tqdm
import numpy as np
import random
from collections import deque
from tensorflow.keras.layers import Input, Dense, Embedding, Concatenate, Dot, Normalization, Lambda, LSTM, Bidirectional, MaxPooling2D, Flatten, Reshape, GlobalMaxPooling1D
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.sequence import pad_sequences
import numpy as np
from network_models.siamese import create_siamese_model
from network_models.autoencoder import create_autoencoder   
from network_models.vae import create_vae_model   
from network_models.attention import create_gat_model   

experiment = open('experiment', 'r').read().strip()
network = open('network', 'r').read().strip()
lines = open(f'../../csv/kgs/{experiment}/test_paths_output.txt', 'r').readlines()
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
    try:
        path1 = np.array(path1.split()).astype(np.int32)
    except:
        print(f"Error processing line {i}: {lines[i]}")
        raise


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

m = create_siamese_model(num_nodes=41, num_rels=41)
if network == 'vae':
    m = create_vae_model(num_nodes=41, num_rels=41)
if network == 'siamese':
    m = create_siamese_model(num_nodes=41, num_rels=41)
if network == 'gat':
    m = create_gat_model()
if network == 'autoencoder':
    m = create_autoencoder(num_nodes=41, num_rels=41)
m.load_weights(f'../../csv/kgs/{experiment}/model_{network}.h5')

from sklearn.metrics import classification_report, confusion_matrix

#precision, recall, f1
if network == 'vae':
    y_est = m.predict({
    "p1_input": X_p1,
    "p2_input": X_p2,
    "p3_input": X_p3,
    "rel_input": X_rel
    })[:][0]
if network == 'siamese':
    y_est = m.predict([X_p1,X_p2,X_p3,X_rel])
if network == 'gat':
    m = create_gat_model()
if network == 'autoencoder':
    y_est = m.predict({
    "p1_input": X_p1,
    "p2_input": X_p2,
    "p3_input": X_p3,
    "rel_input": X_rel
    })[:][0]

# for i,y_ in enumerate(y_est):
#     print(y_est[i], Y[i])
print(classification_report(Y, (y_est > 0.95).astype(int)))


#empricially estimated thresholds:
# 0.970 for linear
# 0.999 for non-linear
# 0.9 for non-linear with noschema
# 0.9 for linear with noschema



from sklearn.metrics import precision_recall_curve, auc, roc_auc_score
precision, recall, thresholds = precision_recall_curve(Y, y_est)
auprc = auc(recall, precision)
print(f"\nAUPRC (Area Under PR Curve): {auprc:.4f}")
auroc = roc_auc_score(Y, y_est)
print(f"AUROC: {auroc:.4f}")
# Hits@10

y_est_flat = y_est.flatten()
pos_indices = np.where(Y == 1)[0]
neg_indices = np.where(Y == 0)[0]
neg_preds = y_est_flat[neg_indices]

hits_at_10 = 0
for pos_idx in pos_indices:
    pos_pred = y_est_flat[pos_idx]
    
    # Calculate how many negative samples scored higher than this positive sample.
    higher_scoring_negs = np.sum(neg_preds > pos_pred)
    
    # If the number of higher scoring negatives is less than 10, it's in the Top 10.
    if higher_scoring_negs < 10:
        hits_at_10 += 1

# Calculate the final Hits@10 ratio
if len(pos_indices) > 0:
    hits_at_10_score = hits_at_10 / len(pos_indices)
else:
    hits_at_10_score = 0.0

print(f"Hits@10: {hits_at_10_score:.4f} ({hits_at_10} hits out of {len(pos_indices)} positive samples)")