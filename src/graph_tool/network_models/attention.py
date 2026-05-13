import tensorflow as tf
from tensorflow.keras import layers, models
from tensorflow.keras.layers import Input, Reshape, Embedding, GlobalMaxPooling1D, Flatten, Concatenate, Normalization, Dense, Dot
from tensorflow.keras.models import  Model


class GATLayer(layers.Layer):
    def __init__(self, units, attn_heads=1, return_sequences=True):
        super(GATLayer, self).__init__()
        self.units = units
        self.attn_heads = attn_heads
        self.return_sequences = return_sequences # Added toggle
        self.dense = layers.Dense(units)
        self.attn_dense = layers.Dense(attn_heads, activation='leaky_relu')

    def call(self, inputs):
        z = self.dense(inputs) 
        attn_logits = self.attn_dense(z) 
        attn_weights = tf.nn.softmax(attn_logits, axis=1) 
        avg_attn = tf.reduce_mean(attn_weights, axis=-1, keepdims=True) 
        weighted_z = avg_attn * z 
        
        if self.return_sequences:
            return weighted_z # Returns (batch, 10, 300)
        else:
            return tf.reduce_sum(weighted_z, axis=1) # Returns (batch, 300)
def create_gat_model(num_nodes, num_rels):
    #input is not rectangular, we need separate inputs for each path
    path1_inp = Input(shape=(10,), name="p1_input")
    path2_inp = Input(shape=(10,), name="p2_input")
    path3_inp = Input(shape=(10,), name="p3_input")
    rel_inp = Input(shape=(1,), name="rel_input")
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
    gat_layer_1 = GATLayer(300,4,return_sequences=True)
    gat_layer_2 = GATLayer(300,4,return_sequences=True)
    #first LSTM layer
    fst_gat_mid = gat_layer_1(p1_seq)
    scd_gat_mid = gat_layer_1(p2_seq)
    trd_gat_mid = gat_layer_1(p3_seq)
    #second LSTM layer
    fst_gat_fin = gat_layer_2(fst_gat_mid)
    scd_gat_fin = gat_layer_2(scd_gat_mid)
    trd_gat_fin = gat_layer_2(trd_gat_mid)
    #reduce max
    pool_layer = GlobalMaxPooling1D() 
    fst_pooled = pool_layer(fst_gat_fin)
    scd_pooled = pool_layer(scd_gat_fin)
    trd_pooled = pool_layer(trd_gat_fin)
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