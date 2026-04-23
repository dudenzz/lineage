import tensorflow as tf
from tensorflow.keras import layers, models, backend as K

def sampling(args):
    z_mean, z_log_var = args
    batch = tf.shape(z_mean)[0]
    dim = tf.shape(z_mean)[1]
    epsilon = K.random_normal(shape=(batch, dim))
    return z_mean + tf.exp(0.5 * z_log_var) * epsilon

def create_vae_model(num_nodes, num_rels, latent_dim=300):
    p1_inp = layers.Input(shape=(10,), name="p1_input")
    p2_inp = layers.Input(shape=(10,), name="p2_input")
    p3_inp = layers.Input(shape=(10,), name="p3_input")
    rel_inp = layers.Input(shape=(1,), name="rel_input")
    node_emb = layers.Embedding(input_dim=num_nodes, output_dim=300)
    encoder_lstm = layers.Bidirectional(layers.LSTM(150, return_sequences=False))

    p1_enc = encoder_lstm(node_emb(p1_inp))
    p2_enc = encoder_lstm(node_emb(p2_inp))
    p3_enc = encoder_lstm(node_emb(p3_inp))
    
    concat = layers.Concatenate()([p1_enc, p2_enc, p3_enc])
    hidden = layers.Dense(512, activation='relu')(concat)

    z_mean = layers.Dense(latent_dim, name="z_mean")(hidden)
    z_log_var = layers.Dense(latent_dim, name="z_log_var")(hidden)

    z = layers.Lambda(sampling, name="z_sampling")([z_mean, z_log_var])


    decoder_lstm = layers.LSTM(150, return_sequences=True)
    decoder_dense = layers.TimeDistributed(layers.Dense(num_nodes, activation='softmax'))
    
    recon_p1 = layers.RepeatVector(10)(z)
    recon_p1 = decoder_lstm(recon_p1)
    recon_p1 = decoder_dense(recon_p1)


    rel_emb = layers.Flatten()(layers.Embedding(num_rels, 300)(rel_inp))
    
    l2_norm = layers.Lambda(lambda x: tf.math.l2_normalize(x, axis=-1))
    z_norm = l2_norm(z)
    rel_norm = l2_norm(rel_emb)
    
    sim = layers.Dot(axes=1)([z_norm, rel_norm])
    edge_pred = layers.Dense(1, activation='sigmoid', name="edge_prediction")(sim)


    vae = models.Model(inputs=[p1_inp, p2_inp, p3_inp, rel_inp], 
                       outputs=[edge_pred, recon_p1])

    # Custom VAE Loss (KL Divergence)
    kl_loss = -0.5 * K.sum(1 + z_log_var - K.square(z_mean) - K.exp(z_log_var), axis=-1)
    vae.add_loss(K.mean(kl_loss) * 0.1)

    vae.compile(optimizer='adam', 
                loss={'edge_prediction': 'binary_crossentropy', 
                      'time_distributed': 'sparse_categorical_crossentropy'})
    
    return vae