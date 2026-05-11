import tensorflow as tf
from tensorflow.keras.layers import Input, Embedding, Bidirectional, LSTM, RepeatVector, TimeDistributed, Dense, Concatenate, Dot, Lambda, Flatten
from tensorflow.keras.models import Model

def create_autoencoder(num_nodes, num_rels):
    node_embedding = Embedding(input_dim=num_nodes, output_dim=300, name="Node_Embedding")
    encoder_lstm = Bidirectional(LSTM(150, return_sequences=False)) # Return_sequences=False is the bottleneck

    p1_inp = Input(shape=(10,), name="p1_input")
    p2_inp = Input(shape=(10,), name="p2_input")
    p3_inp = Input(shape=(10,), name="p3_input")


    p1_encoded = encoder_lstm(node_embedding(p1_inp))
    p2_encoded = encoder_lstm(node_embedding(p2_inp))
    p3_encoded = encoder_lstm(node_embedding(p3_inp))


    bottleneck = Concatenate()([p1_encoded, p2_encoded, p3_encoded])
    bottleneck = Dense(300, activation='relu', name="latent_bottleneck")(bottleneck)

    decoder_lstm1 = LSTM(150, return_sequences=True)
    decoder_lstm2 = LSTM(150, return_sequences=True)
    decoder_lstm3 = LSTM(150, return_sequences=True)
    decoder_dense1 = TimeDistributed(Dense(num_nodes, activation='softmax'), name="recon_p1")
    decoder_dense2 = TimeDistributed(Dense(num_nodes, activation='softmax'), name="recon_p2")
    decoder_dense3 = TimeDistributed(Dense(num_nodes, activation='softmax'), name="recon_p3")

    recon_p1 = RepeatVector(10)(bottleneck)
    recon_p1 = decoder_lstm1(recon_p1)
    recon_p1 = decoder_dense1(recon_p1)
    recon_p2 = RepeatVector(10)(bottleneck)
    recon_p2 = decoder_lstm2(recon_p2)
    recon_p2 = decoder_dense2(recon_p2)
    recon_p3 = RepeatVector(10)(bottleneck)
    recon_p3 = decoder_lstm3(recon_p3)
    recon_p3 = decoder_dense3(recon_p3)

    rel_inp = Input(shape=(1,), name="rel_input")
    rel_emb = Flatten()(Embedding(input_dim=num_rels, output_dim=300)(rel_inp))

    # L2 Norm for Cosine Similarity
    l2_norm = Lambda(lambda x: tf.math.l2_normalize(x, axis=-1))
    path_latent_norm = l2_norm(bottleneck)
    rel_norm = l2_norm(rel_emb)

    # Similarity score
    sim = Dot(axes=1)([path_latent_norm, rel_norm])
    output = Dense(1, activation='sigmoid', name="edge_prediction")(sim)


    m = Model(inputs=[p1_inp, p2_inp, p3_inp, rel_inp], outputs=[output, recon_p1, recon_p2, recon_p3])
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