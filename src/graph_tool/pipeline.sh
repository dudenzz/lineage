#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "1/6 Creating training graph..."
python create_training_graph.py

echo "2/6 Generating training data (pathfinder)..."
./pathfinder_traindata 7 20 40

echo "3/6 Creating test graph..."
python create_test_graph.py

echo "4/6 Generating test data (pathfinder)..."
./pathfinder_testdata 7 20 40

echo "5/6 Starting training..."
python train.py

echo "6/6 Starting testing..."
python test.py

echo "Done!"
