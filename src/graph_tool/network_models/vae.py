import tensorflow as tf
from tensorflow.keras import layers, models

class KLLatentLayer(layers.Layer):
    """
    Custom layer that performs the reparameterization trick 
    and adds the KL divergence loss automatically.
    """
    def __init__(self, **kwargs):
        super(KLLatentLayer, self).__init__(**kwargs)

    def call(self, inputs):
        z_mean, z_log_var = inputs
        
        # Calculate KL Loss
        kl_loss = -0.5 * tf.reduce_sum(
            1 + z_log_var - tf.square(z_mean) - tf.exp(z_log_var), 
            axis=-1
        )
        # Add loss to the model, scaled by a beta factor if desired
        self.add_loss(tf.reduce_mean(kl_loss) * 0.01)
        
        # Reparameterization trick
        batch = tf.shape(z_mean)[0]
        dim = tf.shape(z_mean)[1]
        epsilon = tf.random.normal(shape=(batch, dim))
        return z_mean + tf.exp(0.5 * z_log_var) * epsilon

def create_vae_model(num_nodes, num_rels, latent_dim=300):
    # --- ENCODER ---
    node_embedding = layers.Embedding(input_dim=num_nodes, output_dim=300, name="Node_Embedding")
    encoder_lstm = layers.Bidirectional(layers.LSTM(150, return_sequences=False))

    p1_inp = layers.Input(shape=(10,), name="p1_input")
    p2_inp = layers.Input(shape=(10,), name="p2_input")
    p3_inp = layers.Input(shape=(10,), name="p3_input")

    p1_encoded = encoder_lstm(node_embedding(p1_inp))
    p2_encoded = encoder_lstm(node_embedding(p2_inp))
    p3_encoded = encoder_lstm(node_embedding(p3_inp))

    merged = layers.Concatenate()([p1_encoded, p2_encoded, p3_encoded])
    
    # Latent parameters
    z_mean = layers.Dense(latent_dim, name="z_mean")(merged)
    z_log_var = layers.Dense(latent_dim, name="z_log_var")(merged)

    # Use  custom layer
    z = KLLatentLayer(name="kl_latent_space")([z_mean, z_log_var])

    # --- DECODERS ---
    # (Keeping original reconstruction logic)
    def build_reconstruction(latent_input, name):
        x = layers.RepeatVector(10)(latent_input)
        x = layers.LSTM(150, return_sequences=True)(x)
        return layers.TimeDistributed(layers.Dense(num_nodes, activation='softmax'), name=name)(x)

    recon_p1 = build_reconstruction(z, "recon_p1")
    recon_p2 = build_reconstruction(z, "recon_p2")
    recon_p3 = build_reconstruction(z, "recon_p3")

    # --- RELATION PREDICTION ---
    rel_inp = layers.Input(shape=(1,), name="rel_input")
    rel_emb = layers.Flatten()(layers.Embedding(input_dim=num_rels, output_dim=300)(rel_inp))

    l2_norm = layers.Lambda(lambda x: tf.math.l2_normalize(x, axis=-1))
    path_latent_norm = l2_norm(z)
    rel_norm = l2_norm(rel_emb)

    sim = layers.Dot(axes=1)([path_latent_norm, rel_norm])
    output = layers.Dense(1, activation='sigmoid', name="edge_prediction")(sim)

    # --- MODEL ---
    m = models.Model(inputs=[p1_inp, p2_inp, p3_inp, rel_inp], 
                     outputs=[output, recon_p1, recon_p2, recon_p3])

    m.compile(
        optimizer='adam',
        loss={
            'edge_prediction': 'binary_crossentropy', 
            'recon_p1': 'sparse_categorical_crossentropy',
            'recon_p2': 'sparse_categorical_crossentropy',
            'recon_p3': 'sparse_categorical_crossentropy'
        },
        loss_weights={'edge_prediction': 1.0, 'recon_p1': 0.33, 'recon_p2': 0.33, 'recon_p3': 0.33}
    )
    return m