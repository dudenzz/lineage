
import tensorflow as tf
from tensorflow.keras.layers import Input, Embedding, Bidirectional, LSTM, Dense, Concatenate, Dot, Lambda, Flatten, Reshape, Normalization, GlobalMaxPooling1D
from tensorflow.keras.models import Model


def create_siamese_model(num_nodes, num_rels):
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
    return m