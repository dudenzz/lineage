import tensorflow as tf
from tensorflow.keras import layers, models

def create_gan_components(num_nodes, num_rels, latent_dim=300):
    # --- DISCRIMINATOR ---
    path_rep_inp = layers.Input(shape=(latent_dim,), name="path_representation")
    rel_inp = layers.Input(shape=(1,), name="rel_input")
    
    rel_emb = layers.Flatten()(layers.Embedding(num_rels, latent_dim)(rel_inp))

    d_merged = layers.Concatenate()([path_rep_inp, rel_emb])
    d_hid = layers.Dense(512, activation='leaky_relu')(d_merged)
    d_out = layers.Dense(1, activation='sigmoid', name="discriminator_output")(d_hid)
    
    discriminator = models.Model([path_rep_inp, rel_inp], d_out, name="Discriminator")
    discriminator.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

    # --- GENERATOR / ENCODER ---
    p1_inp = layers.Input(shape=(10,))
    p2_inp = layers.Input(shape=(10,))
    p3_inp = layers.Input(shape=(10,))
    
    node_emb = layers.Embedding(input_dim=num_nodes, output_dim=300)
    encoder_lstm = layers.Bidirectional(layers.LSTM(150, return_sequences=False))
    
    p1_enc = encoder_lstm(node_emb(p1_inp))
    p2_enc = encoder_lstm(node_emb(p2_inp))
    p3_enc = encoder_lstm(node_emb(p3_inp))
    
    g_concat = layers.Concatenate()([p1_enc, p2_enc, p3_enc])
    g_out = layers.Dense(latent_dim, activation='tanh', name="path_representation_gen")(g_concat)
    
    generator = models.Model([p1_inp, p2_inp, p3_inp], g_out, name="Generator")

    # --- COMBINED GAN (Adversarial Model) ---
    discriminator.trainable = False  # Jak to zrobić, żeby nie trenować dyskryminatora w odpowiednim momencie; może dwie funkcje do trenowania generatora i dyskryminatora osobno?
    gan_rel_inp = layers.Input(shape=(1,))
    gan_path_rep = generator([p1_inp, p2_inp, p3_inp])
    gan_out = discriminator([gan_path_rep, gan_rel_inp])
    
    gan_model = models.Model([p1_inp, p2_inp, p3_inp, gan_rel_inp], gan_out)
    gan_model.compile(optimizer='adam', loss='binary_crossentropy')
    
    return generator, discriminator, gan_model