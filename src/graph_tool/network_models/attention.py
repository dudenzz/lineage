import tensorflow as tf
from tensorflow.keras import layers, models


class GATLayer(layers.Layer):
    def __init__(self, units, attn_heads=1):
        super(GATLayer, self).__init__()
        self.units = units
        self.attn_heads = attn_heads
        self.dense = layers.Dense(units)
        self.attn_dense = layers.Dense(attn_heads, activation='leaky_relu')

    def call(self, inputs):
        # inputs: [batch, seq_len, hidden_dim]
        z = self.dense(inputs) 
        # Self-attention mechanism across the sequence (the "path")
        attn_weights = tf.nn.softmax(self.attn_dense(z), axis=1)
        output = tf.reduce_sum(attn_weights * z, axis=1)
        return output

def create_gat_model(num_nodes, num_rels):
    # Inputs
    p1_inp = layers.Input(shape=(10,), name="p1_input")
    p2_inp = layers.Input(shape=(10,), name="p2_input")
    p3_inp = layers.Input(shape=(10,), name="p3_input")
    rel_inp = layers.Input(shape=(1,), name="rel_input")


    node_emb = layers.Embedding(input_dim=num_nodes, output_dim=300, name="node_emb")
    rel_emb_layer = layers.Embedding(input_dim=num_rels, output_dim=300, name="rel_emb")
    gat_processor = GATLayer(units=300, attn_heads=4)
    p1_gat = gat_processor(node_emb(p1_inp))
    p2_gat = gat_processor(node_emb(p2_inp))
    p3_gat = gat_processor(node_emb(p3_inp))


    r_emb = layers.Flatten()(rel_emb_layer(rel_inp))
    combined_paths = layers.Concatenate()([p1_gat, p2_gat, p3_gat])
    latent_rep = layers.Dense(300, activation='relu')(combined_paths)
    l2_norm = layers.Lambda(lambda x: tf.math.l2_normalize(x, axis=-1))
    path_norm = l2_norm(latent_rep)
    rel_norm = l2_norm(r_emb)

    score = layers.Dot(axes=1)([path_norm, rel_norm])
    output = layers.Dense(1, activation='sigmoid')(score)

    model = models.Model(inputs=[p1_inp, p2_inp, p3_inp, rel_inp], outputs=output)
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    
    return model