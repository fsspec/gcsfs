#!/bin/bash
VM_NAME=$1
ZONE=$2

echo "--- Removing SSH key from OS Login ---"
# gcloud compute os-login describe-profile --format=json | \
# python3 -c "import sys, json; print('\n'.join(json.load(sys.stdin).get('sshPublicKeys', {}).keys()))" | \
# xargs -I {} gcloud compute os-login ssh-keys remove --key={}

gcloud compute os-login describe-profile | \
grep "fingerprint:" | \
awk '{print $2}' | \
xargs -I {} gcloud compute os-login ssh-keys remove --key={} || true

for i in {1..10}; do
  if gcloud compute ssh ${VM_NAME} --zone=${ZONE} --internal-ip --ssh-key-file=/workspace/.ssh/google_compute_engine --command="echo VM is ready"; then
    exit 0
  fi
  echo "Waiting for VM to become available... (attempt $i/10)"
  sleep 15
done
exit 1
