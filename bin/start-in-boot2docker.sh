#!/bin/bash

PROJ_NAME='spandex-http'
PORT=8042
ENVFILE="/tmp/${PROJ_NAME}-env"     # This is the boot2docker vm path.

PROJ_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)/${PROJ_NAME}"

cp ${PROJ_ROOT}/target/scala*/${PROJ_NAME}-assembly*.jar \
   ${PROJ_ROOT}/docker/${PROJ_NAME}-assembly.jar \
    || { echo "Failed to copy assembly jar"; exit; }

boot2docker init
boot2docker down

VBoxManage sharedfolder add 'boot2docker-vm' \
           --name "${PROJ_NAME}-docker" \
           --hostpath "$PROJ_ROOT/docker" \
           --automount 2>/dev/null

echo "Starting VM..."
$(boot2docker up 2>&1 | tail -n 4)

echo "About to ssh into docker container!"

boot2docker ssh <<EOF
    mkdir ${PROJ_NAME}-docker
    sudo mount -t vboxsf -o uid=1000,gid=50 ${PROJ_NAME}-docker ${PROJ_NAME}-docker
    cd ${PROJ_NAME}-docker
    docker build --no-cache --rm -t ${PROJ_NAME} .
    echo "SPANDEX_ES_HOST=$1" > ${ENVFILE}
    echo "SPANDEX_ES_PORT=9300" >> ${ENVFILE}
    sed -i 's/\[/["/' ${ENVFILE}
    sed -i 's/, /", "/g' ${ENVFILE}
    sed -i 's/]/"]/' ${ENVFILE}
    echo starting ${PROJ_NAME}
    docker run --env-file=${ENVFILE} -p ${PORT}:${PORT} -d ${PROJ_NAME}
EOF

echo "Forwarding localhost:${PORT} to boot2docker (Ctrl+C to exit)"
echo "boot2docker ssh -L ${PORT}:localhost:${PORT} -N"
boot2docker ssh -L ${PORT}:localhost:${PORT} -N
