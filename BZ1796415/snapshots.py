#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import contextlib
import logging
import ovirtsdk4 as sdk
import ovirtsdk4.types as types
import sys
import time

"""
Reproducer for https://bugzilla.redhat.com/1796415

Issue: live merge sometimes fails, unable to merge a snapshot.

Test scenario:
  * create two snapshots
  * remove/merge the older one
  * repeate this in cycle until merge failure happens
"""

# Name of the VM for which snapshots will be taken.
VM_NAME = "test-cirros"

# Number of runs of create/merge snapshots.
NUM_RUNS = 101

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger("snapshots")

connection = sdk.Connection(
    url='https://ovirt-bz.local/ovirt-engine/api',
    username='admin@internal',
    password='ovirt',
    ca_file='ca.pem',
    debug=True,
    log=logging.getLogger(),
)


def wait_for_snapshot(snap_service):
    log.info("Waiting for snapshot to be ready")
    while True:
        if snap_service.get().snapshot_status == types.SnapshotStatus.OK:
            break
        time.sleep(1)


def wait_for_removal(snap_service):
    log.info("Waiting for snapshot removal")
    while True:
        try:
            snap_service.get()
            # TODO: we should check here, if the snaphost removal failed and
            # eventually exit.
            time.sleep(1)
        except sdk.NotFoundError:
            break


def create_snapshot(snapshots_service, description):
    return snapshots_service.add(
        types.Snapshot(
            description=description,
            persist_memorystate=False,
        ),
    )


with contextlib.closing(connection):
    # Locate the virtual machines service and use it to find the virtual
    # machine.
    vms_service = connection.system_service().vms_service()
    vm = vms_service.list(search="name={}".format(VM_NAME))[0]

    # Locate the service that manages the snapshots of the virtual
    # machine.
    snapshots_service = vms_service.vm_service(vm.id).snapshots_service()

    # Create the initial snapshot.
    log.info("Creating new snapshot-%i", 0)
    prev_snap = create_snapshot(
            snapshots_service, "snapshot-{}".format(0))
    prev_snap_name = prev_snap.description
    prev_snap_service = snapshots_service.snapshot_service(prev_snap.id)

    wait_for_snapshot(prev_snap_service)

    # Run in the loop creating new snapshot and deleting the old one.
    for i in range(1, NUM_RUNS):
        log.info("Creating new snapshot-%i", i)
        curr_snap = create_snapshot(
            snapshots_service, "snapshot-{}".format(i))
        curr_snap_service = snapshots_service.snapshot_service(
            curr_snap.id)
        wait_for_snapshot(curr_snap_service)

        log.info("Removing snapshot %s", prev_snap_name)
        prev_snap_service.remove()
        wait_for_removal(prev_snap_service)

        prev_snap_service = curr_snap_service
        prev_snap_name = curr_snap.description
