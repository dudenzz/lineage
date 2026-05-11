import tensorflow as tf
from tensorflow.keras.layers import Input, Embedding, Bidirectional, LSTM, RepeatVector, TimeDistributed, Dense, Concatenate, Dot, Lambda, Flatten
from tensorflow.keras.models import Model
from keras import ops

def sampling(args):
    """Reparameterization trick: samples from a normal distribution."""
    z_mean, z_log_var = args
    batch = K.shape(z_mean)[0]
    dim = K.int_shape(z_mean)[1]
    epsilon = K.random_normal(shape=(batch, dim))
    return z_mean + K.exp(0.5 * z_log_var) * epsilon

def create_vae_model(num_nodes, num_rels, latent_dim=300):
    # --- ENCODER ---
    node_embedding = Embedding(input_dim=num_nodes, output_dim=300, name="Node_Embedding")
    encoder_lstm = Bidirectional(LSTM(150, return_sequences=False))

    p1_inp = Input(shape=(10,), name="p1_input")
    p2_inp = Input(shape=(10,), name="p2_input")
    p3_inp = Input(shape=(10,), name="p3_input")

    p1_encoded = encoder_lstm(node_embedding(p1_inp))
    p2_encoded = encoder_lstm(node_embedding(p2_inp))
    p3_encoded = encoder_lstm(node_embedding(p3_inp))

    merged = Concatenate()([p1_encoded, p2_encoded, p3_encoded])
    
    # VAE Latent Space
    z_mean = Dense(latent_dim, name="z_mean")(merged)
    z_log_var = Dense(latent_dim, name="z_log_var")(merged)

    # Use Lambda layer for the reparameterization trick
    z = Lambda(sampling, output_shape=(latent_dim,), name="z")([z_mean, z_log_var])

    # --- DECODERS ---
    decoder_lstm1 = LSTM(150, return_sequences=True)
    decoder_lstm2 = LSTM(150, return_sequences=True)
    decoder_lstm3 = LSTM(150, return_sequences=True)
    
    decoder_dense1 = TimeDistributed(Dense(num_nodes, activation='softmax'), name="recon_p1")
    decoder_dense2 = TimeDistributed(Dense(num_nodes, activation='softmax'), name="recon_p2")
    decoder_dense3 = TimeDistributed(Dense(num_nodes, activation='softmax'), name="recon_p3")

    # Reconstruct from sampled z
    recon_p1 = decoder_dense1(decoder_lstm1(RepeatVector(10)(z)))
    recon_p2 = decoder_dense2(decoder_lstm2(RepeatVector(10)(z)))
    recon_p3 = decoder_dense3(decoder_lstm3(RepeatVector(10)(z)))

    # --- RELATION PREDICTION ---
    rel_inp = Input(shape=(1,), name="rel_input")
    rel_emb = Flatten()(Embedding(input_dim=num_rels, output_dim=300)(rel_inp))

    l2_norm = Lambda(lambda x: tf.math.l2_normalize(x, axis=-1))
    path_latent_norm = l2_norm(z)
    rel_norm = l2_norm(rel_emb)

    sim = Dot(axes=1)([path_latent_norm, rel_norm])
    output = Dense(1, activation='sigmoid', name="edge_prediction")(sim)

    # --- MODEL & KL LOSS ---
    m = Model(inputs=[p1_inp, p2_inp, p3_inp, rel_inp], outputs=[output, recon_p1, recon_p2, recon_p3])

    # Define KL Loss function
    kl_loss = -0.5 * ops.sum(1 + z_log_var - ops.square(z_mean) - ops.exp(z_log_var), axis=-1)
    # Add loss to model and weight it (beta-VAE approach)
    m.add_loss(ops.mean(kl_loss) * 0.01) 

    m.compile(
        optimizer='adam',
        loss={
            'edge_prediction': 'binary_crossentropy', 
            'recon_p1': 'sparse_categorical_crossentropy',
            'recon_p2': 'sparse_categorical_crossentropy',
            'recon_p3': 'sparse_categorical_crossentropy'
        },
        loss_weights={
            'edge_prediction': 1.0, 
            'recon_p1': 0.33, 
            'recon_p2': 0.33, 
            'recon_p3': 0.33
        }
    )
    return m