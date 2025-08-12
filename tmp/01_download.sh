#!/bin/bash
set -e
echo "Downloading routes.csv..."
# curl -L "https://drive.usercontent.google.com/download?id=1NFkmc6cUAgVOzmXwPxXFuv1mhVZMTOrS&confirm=t" -o /docker-entrypoint-initdb.d/routes.csv
curl -o /data/routes.csv -L "https://drive.usercontent.google.com/download?id=1NFkmc6cUAgVOzmXwPxXFuv1mhVZMTOrS&confirm=t"
