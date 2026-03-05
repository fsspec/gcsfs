#!/bin/bash
set -e
VM_NAME=$1
ZONE=$2

for i in {1..10}; do
  if gcloud compute ssh ${VM_NAME} \
    --zone=${ZONE} \
    --internal-ip \
    --ssh-key-file=/workspace/.ssh/google_compute_engine \
    --command="echo VM is ready";
  then
    exit 0
  fi
  echo "Waiting for VM to become available... (attempt $i/10)"
  sleep 15
done
exit 1
