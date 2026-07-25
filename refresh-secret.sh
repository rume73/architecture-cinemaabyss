#!/bin/bash

# На будущее
# ./refresh-secret.sh
echo "Обновление секрета dockerconfigjson..."

kubectl -n cinemaabyss delete secret dockerconfigjson --ignore-not-found

kubectl create secret generic dockerconfigjson \
  --namespace cinemaabyss \
  --from-file=.dockerconfigjson=$HOME/.docker/config.json \
  --type=kubernetes.io/dockerconfigjson

echo "Перезапуск подов..."
kubectl -n cinemaabyss delete pods --all

echo "Ожидание запуска..."
sleep 20

kubectl -n cinemaabyss get pods
