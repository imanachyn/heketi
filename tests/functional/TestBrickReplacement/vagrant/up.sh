#!/bin/sh

set -e
vagrant up --no-provision --provider=libvirt "$@"
vagrant provision
