#!/bin/sh
set -e

echo "Waiting for MinIO to start..."
until curl -s http://minio:9000/minio/health/live; do
    echo "MinIO not available yet. Retrying in 2 seconds..."
    sleep 2
done

echo "Configuring MinIO client (mc)..."
mc alias set myminio http://minio:9000 minioadmin minioadmin

echo "Creating bucket 'ecommerce-lake' if it does not exist..."
mc mb --ignore-existing myminio/ecommerce-lake

echo "Setting public/download policy for read access..."
mc anonymous set download myminio/ecommerce-lake

echo "Creating initial prefixes..."
mc touch myminio/ecommerce-lake/bronze/.keep
mc touch myminio/ecommerce-lake/silver/.keep
mc touch myminio/ecommerce-lake/gold/.keep
mc touch myminio/ecommerce-lake/checkpoints/.keep

echo "MinIO buckets and prefixes initialized successfully!"
