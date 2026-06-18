#!/usr/bin/env python3
# NUM_IMAGES and NUM_ITERATIONS can be passed as arguments for the script
# python3 ../qa/workunits/rbd/test.py 10 30
# If no arguments are passed then function works with deafult values
# NUM_IMAGES=5, NUM_ITERATIONS=20
import os
import sys
import subprocess
import random
import time
import rados
import rbd
import tempfile
import threading
import xml.etree.ElementTree as ET


# =========================
# CONFIG (adjust these)
# =========================
CEPH_ROOT = os.getcwd()              # should be build/
CEPH_SRC = os.path.abspath("../src")

CEPH_ID = "admin"
MIRROR_USER_ID_PREFIX = "mirror."
LAST_MIRROR_INSTANCE = 1
IMAGE_SIZE = 600 #in MB
MIRRORS_RUNNING = True
KILLING_SCHEDULED = False
TEMPDIR = None
POOL = "testpool"
PARENT_POOL = "parentpool"

NS1 = "ns1"
NS2 = "ns2"

PRIMARY = "primary"
PRIMARY_DEMOTED = "primary_demoted"
NON_PRIMARY = "non_primary"
NON_PRIMARY_DEMOTED = "non_primary_demoted"
DISABLED = "disabled"
IMAGE_DELETED = "image_deleted"


MIRROR_IMAGE_STATUS_STATE_UNKNOWN         = 0
MIRROR_IMAGE_STATUS_STATE_ERROR           = 1
MIRROR_IMAGE_STATUS_STATE_SYNCING         = 2
MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY = 3
MIRROR_IMAGE_STATUS_STATE_REPLAYING       = 4
MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY = 5
MIRROR_IMAGE_STATUS_STATE_STOPPED         = 6

images = {}
CLUSTERS = {}
PENDING_SYNCS = []

from dataclasses import dataclass

@dataclass
class ImageState:
    cluster1_state: str
    cluster2_state: str


MIRROR_RUNNING = {
    "cluster1": True,
    "cluster2": True,
}

VALID_ACTIONS = {
    PRIMARY: ["snapshot", "demote", "disable"],

    PRIMARY_DEMOTED: ["promote", "force_disable", "resync"],

    NON_PRIMARY: ["resync", "force_disable"], #edit later to handle force_promote

    NON_PRIMARY_DEMOTED: ["promote", "resync", "force_disable"],

    DISABLED: ["enable"]
}

INVALID_ACTIONS = {
    PRIMARY: ["enable", "resync", "promote", "force_promote"],

    PRIMARY_DEMOTED: ["enable", "demote", "snapshot", "disable"],

    NON_PRIMARY: ["promote", "enable", "disable", "demote", "snapshot"],

    NON_PRIMARY_DEMOTED: ["enable", "disable", "demote", "snapshot"],

    DISABLED: ["promote", "demote", "snapshot", "resync", "force_promote",
               "force_disable"]
}

def dump_fd_count():
    print(
        f"[FD COUNT] {len(os.listdir('/proc/self/fd'))}"
    )

def connect_cluster(cluster):
    if cluster in CLUSTERS:
        return CLUSTERS[cluster]

    conf = f"{CEPH_ROOT}/run/{cluster}/ceph.conf"

    conn = rados.Rados(conffile=conf)
    conn.connect()

    CLUSTERS[cluster] = conn
    return conn

def choose_random_cluster():
    return random.choice([
        "cluster1",
        "cluster2"
    ])

def get_state(image, cluster):
    if cluster == "cluster1":
        return images[image].cluster1_state

    return images[image].cluster2_state

def choose_action(state):
    valid = random.choice([True, False])

    if valid:
        return (random.choice(VALID_ACTIONS[state]), True)

    return (random.choice(INVALID_ACTIONS[state]), False)

def execute_action_api(cluster, image, action):
    if action == "snapshot":
        create_snapshot(cluster, POOL, image)

    elif action == "promote":
        promote_image(cluster, POOL, image)

    elif action == "force_promote":
        promote_image(cluster, POOL, image, force=True)

    elif action == "demote":
        demote_image(cluster, POOL, image)

    elif action == "disable":
        disable_image_mirror(cluster, POOL, image)

    elif action == "force_disable":
        disable_image_mirror(cluster, POOL, image, True)

    elif action == "enable":
        enable_image_mirror(cluster, POOL, image)

    elif action == "resync":
        resync_image(cluster, POOL, image)

def update_local_state(image, cluster, action):
    other = (
        "cluster2"
        if cluster == "cluster1"
        else "cluster1"
    )

    if action == "snapshot":
        return

    if action == "resync":
        return

    if action in ("disable", "force_disable"):
        setattr(images[image], f"{cluster}_state", DISABLED)
        return

    if action == "enable":
        setattr(images[image], f"{cluster}_state", PRIMARY)
        return

    if action == "demote":
        setattr(images[image], f"{cluster}_state", PRIMARY_DEMOTED)
        return

    if action in ("promote", "force_promote"):
        setattr(images[image], f"{cluster}_state", PRIMARY)

def update_remote_state_after_sync(image,cluster, action):

    other = (
        "cluster2"
        if cluster == "cluster1"
        else "cluster1"
    )

    if action == "demote":
        setattr(images[image], f"{other}_state", NON_PRIMARY_DEMOTED)

    elif action in ("disable", "force_disable"):
        setattr(images[image], f"{other}_state", IMAGE_DELETED)

    elif action == "enable":
        setattr(images[image], f"{other}_state", NON_PRIMARY)

    elif action in ("promote", "force_promote"):
        setattr(images[image], f"{other}_state", NON_PRIMARY)

def execute_action(cluster, image, action, expected_success):

    try:
        execute_action_api(cluster, image, action)

        if not expected_success:
            raise Exception(
                f"{action} unexpectedly succeeded"
            )

    except Exception:

        if expected_success:
            raise

def run_cmd(cmd, check=True, capture_output=False):
    env = os.environ.copy()
    env["PATH"] = f"{CEPH_ROOT}/bin:" + env["PATH"]

    print(f"\n[CMD] {cmd}\n")

    result = subprocess.run(
        cmd,
        shell=True,
        text=True,
        capture_output=capture_output,
        env=env
    )

    if check and result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Command failed: {cmd}")

    return result.stdout if capture_output else None


def create_users(cluster):
    keyring_path = f"{CEPH_ROOT}/run/{cluster}/keyring"

    for instance in range(LAST_MIRROR_INSTANCE + 1):
        run_cmd(
            f"CEPH_ARGS='' ceph --cluster {cluster} "
            f"auth get-or-create client.{MIRROR_USER_ID_PREFIX}{instance} "
            f"mon 'profile rbd-mirror' osd 'profile rbd' mgr 'profile rbd' "
            f">> {keyring_path}"
        )

def setup_cluster(cluster):
    global TEMPDIR

    run_cmd(
        f"CEPH_ARGS='' MDS=0 {CEPH_SRC}/mstart.sh {cluster} -n --without-dashboard"
    )

    conf_src = os.path.realpath(f"{CEPH_ROOT}/run/{cluster}/ceph.conf")
    conf_dst = f"{TEMPDIR}/{cluster}.conf"

    if os.path.exists(conf_dst):
        os.remove(conf_dst)

    os.symlink(conf_src, conf_dst)

    os.chdir(TEMPDIR)
    create_users(cluster)

    # append mirror daemon config
    for instance in range(LAST_MIRROR_INSTANCE + 1):
        with open(conf_dst, "a") as f:
            f.write(f"""
[client.{MIRROR_USER_ID_PREFIX}{instance}]
    admin socket = {TEMPDIR}/rbd-mirror.$cluster-$name.asok
    pid file = {TEMPDIR}/rbd-mirror.$cluster-$name.pid
    log file = {TEMPDIR}/rbd-mirror.{cluster}_daemon.{instance}.log
""")

def peer_add(
        cluster, pool, client_cluster, uuid_var_name=None, extra_args=None):

    remote_cluster = client_cluster.split("@")[-1]
    backoff = [1, 2, 4, 8, 16, 32]

    if extra_args is None:
        extra_args = ""

    for s in backoff:
        cmd = (
            f"rbd --cluster {cluster} mirror pool peer add "
            f"{pool} {client_cluster} {extra_args}"
        )

        print(f"[CMD] {cmd}")
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        error_code = proc.returncode
        peer_uuid = proc.stdout.strip()

        if error_code == 17:
            # race condition → remove and retry
            time.sleep(s)

            xml_out = run_cmd(
                f"rbd mirror pool info --cluster {cluster} "
                f"--pool {pool} --format xml",
                capture_output=True
            )

            root = ET.fromstring(xml_out)

            peer_uuid = None
            for peer in root.findall(".//peer"):
                if peer.findtext("site_name") == remote_cluster:
                    peer_uuid = peer.findtext("uuid")

            if peer_uuid:
                run_cmd(
                    f"CEPH_ARGS='' rbd --cluster {cluster} "
                    f"--pool {pool} mirror pool peer remove {peer_uuid}"
                )

        else:
            if error_code != 0:
                raise Exception(f"peer_add failed: {proc.stderr}")

            if uuid_var_name is not None:
                return peer_uuid

            return

    raise Exception("peer_add failed after retries")

def setup_pools(cluster, remote_cluster):
    # create pools
    run_cmd(f"ceph --cluster {cluster} osd pool create {POOL} 64 64")
    run_cmd(f"ceph --cluster {cluster} osd pool create {PARENT_POOL} 64 64")

    # init pools
    run_cmd(f"rbd --cluster {cluster} pool init {POOL}")
    run_cmd(f"rbd --cluster {cluster} pool init {PARENT_POOL}")

    # enable mirroring
    run_cmd(
        f"rbd --cluster {cluster} mirror pool enable {POOL} image"
    )
    run_cmd(
        f"rbd --cluster {cluster} mirror pool enable {PARENT_POOL} image"
    )

    # namespaces
    run_cmd(f"rbd --cluster {cluster} namespace create {POOL}/{NS1}")
    run_cmd(f"rbd --cluster {cluster} namespace create {POOL}/{NS2}")
    run_cmd(f"rbd --cluster {cluster} namespace create {PARENT_POOL}/{NS1}")

    # enable namespace mirroring
    run_cmd(
        f"rbd --cluster {cluster} mirror pool enable {POOL}/{NS1} image"
    )
    run_cmd(
        f"rbd --cluster {cluster} mirror pool enable {POOL}/{NS2} image"
    )
    run_cmd(
        f"rbd --cluster {cluster} mirror pool enable {PARENT_POOL}/{NS1} image"
    )

    # peer add
    peer_add(cluster, POOL, remote_cluster)
    peer_add(cluster, PARENT_POOL, remote_cluster)

def start_mirror(cluster, instance):
    log = f"{TEMPDIR}/rbd-mirror-{cluster}-{instance}.log"

    run_cmd(
        f"rbd-mirror "
        f"--cluster {cluster} "
        f"--id {MIRROR_USER_ID_PREFIX}{instance} "
        f"--daemonize=true "
        f"--log-file={log}"
    )

def start_mirrors(cluster):
    for instance in range(LAST_MIRROR_INSTANCE):
        start_mirror(cluster, instance)

def stop_mirrors(cluster):
    print(f"[STOP] stopping mirrors for {cluster}")
    run_cmd(f"pkill -9 -f 'rbd-mirror --cluster {cluster}'", check=False)

def run_xml(cmd):
    out = run_cmd(cmd, capture_output=True)
    return ET.fromstring(out)

def kill_mirrors_after_delay(delay):
    def worker():
        global MIRRORS_RUNNING
        global KILLING_SCHEDULED
        print(
            f"[CHAOS] scheduled kill of rbd-mirror daemons "
            f"in {delay} seconds"
        )

        time.sleep(delay)

        print(
            f"[CHAOS] killing rbd-mirror daemons"
        )

        stop_mirrors("cluster1")
        stop_mirrors("cluster2")
        MIRRORS_RUNNING = False
        KILLING_SCHEDULED = False

    t = threading.Thread(
        target=worker,
        daemon=True
    )

    t.start()

    return t

def add_pending_sync(
    image, cluster, action, old_image_id=None):
    PENDING_SYNCS.append({
        "image": image,
        "cluster": cluster,
        "action": action,
        "old_image_id": old_image_id,
    })

    print(
        f"[PENDING] "
        f"image={image} "
        f"cluster={cluster} "
        f"action={action}"
    )

def process_pending_syncs():
    pending = list(PENDING_SYNCS)

    PENDING_SYNCS.clear()

    for item in pending:
        image = item["image"]
        cluster = item["cluster"]
        action = item["action"]
        old_image_id = item["old_image_id"]

        other = (
            "cluster2"
            if cluster == "cluster1"
            else "cluster1"
        )

        print(
            f"[SYNCING] "
            f"{image} "
            f"{action}"
        )

        if action == "resync":
            validate_resync(old_image_id, other, cluster, POOL, image)
        elif action == "disable":
            wait_for_image_deleted(other, POOL, image, old_image_id)
            update_remote_state_after_sync(image, cluster, action)
        elif action == "force_disable":
            state = get_state(image, other)
            if state == PRIMARY:
                wait_for_image_recreated(cluster, POOL, image, old_image_id)
                wait_for_snapshot_sync_complete(cluster, other, POOL, POOL, image)
                wait_for_status_in_pool_dir(cluster, POOL, image, True,
                    MIRROR_IMAGE_STATUS_STATE_REPLAYING)
                setattr(images[image], f"{cluster}_state", NON_PRIMARY)
        else:
            wait_for_snapshot_sync_complete(
                other, cluster, POOL, POOL, image)
            update_remote_state_after_sync(image, cluster, action)


def get_image_id(cluster, pool, image_name):
    ioctx = None
    image = None

    try:
        ioctx, image = open_image(cluster, pool, image_name)
        return image.id()

    except Exception:
        return None

    finally:
        if image is not None:
            try:
                image.close()
            except Exception:
                pass

        if ioctx is not None:
            try:
                ioctx.close()
            except Exception:
                pass

def test_image_present(
    cluster, pool, image_name, expected_state, image_id=None):

    current_state = "deleted"
    current_image_id = get_image_id(cluster, pool, image_name)

    if (
        current_image_id is not None
        and (
            image_id is None
            or image_id == current_image_id
        )
    ):
        current_state = "present"

    return current_state == expected_state

def wait_for_image_deleted(
    cluster, pool, image_name, image_id, timeout=300):

    intervals = [1, 2, 4, 8, 8, 8, 16, 16, 32]

    for s in intervals:
        time.sleep(s)

        if test_image_present(cluster, pool, image_name, "deleted", image_id):
            return

    raise Exception(
        f"{image_name} never disappeared"
    )

def wait_for_image_recreated(
    cluster, pool, image_name, old_image_id, timeout=300):

    intervals = [1, 2, 4, 8, 8, 8, 16, 16, 32]

    for s in intervals:
        time.sleep(s)

        new_id = get_image_id(cluster, pool, image_name)

        if (
            new_id is not None
            and new_id != old_image_id
        ):
            return new_id

    raise Exception(
        f"{image_name} never recreated"
    )

def validate_resync(old_image_id,
    primary_cluster, secondary_cluster, pool, image_name):

    print(
        f"[RESYNC] old image id={old_image_id}"
    )

    primary_state = get_state(image_name, primary_cluster)

    # If remote image is no longer primary,
    # resync shouldn't be expected to recreate the secondary image.

    if primary_state != PRIMARY:

        print(
            f"[RESYNC] skipping recreate validation "
            f"because {primary_cluster}/{image_name} "
            f"is {primary_state}"
        )

        try:
            wait_for_image_deleted(
                secondary_cluster,
                pool,
                image_name,
                old_image_id
            )

            raise Exception(
                "Image unexpectedly disappeared "
                "while remote was not PRIMARY"
            )

        except Exception:
            pass

        return

    wait_for_image_deleted(
        secondary_cluster, pool, image_name, old_image_id)

    new_image_id = wait_for_image_recreated(
        secondary_cluster, pool, image_name, old_image_id)

    print(
        f"[RESYNC] new image id={new_image_id}"
    )

    wait_for_snapshot_sync_complete(
        secondary_cluster, primary_cluster, pool, pool, image_name)
    wait_for_status_in_pool_dir(
        secondary_cluster, pool, image_name, True, MIRROR_IMAGE_STATUS_STATE_REPLAYING)

def get_latest_primary_snap_id(cluster, pool, image_name):
    id = 0
    ioctx, image = open_image(cluster, pool, image_name)

    for snap in image.list_snaps():
        if "mirror" not in snap:
            continue
        mirror_snaps = snap["mirror"]
        if mirror_snaps.get("complete", False):
            id = snap["id"]

    ioctx.close()
    image.close()

    if id == 0:
        raise Exception("No complete primary snapshots")

    return id

def get_latest_secondary_primary_snap_id(cluster, pool, image_name):
    id = 0
    ioctx, image = open_image(cluster, pool, image_name)

    for snap in image.list_snaps():
        if "mirror" not in snap:
            continue
        mirror_snaps = snap["mirror"]
        if mirror_snaps.get("complete", False):
            id = mirror_snaps.get("primary_snap_id")

    ioctx.close()
    image.close()

    if id == 0:
        return None

    return id

def wait_for_snapshot_sync_complete(
    local_cluster, remote_cluster, local_pool, remote_pool, image):

    sleep_intervals = [0.2, 0.4, 0.8, 1.6, 2, 2, 4, 4, 8, 8, 16, 16, 32, 32]

    primary_snap_id = get_latest_primary_snap_id(
        remote_cluster, remote_pool, image)

    print(f"[PRIMARY] target snap id = {primary_snap_id}")

    for s in sleep_intervals:
        time.sleep(s)

        try:
            secondary_snap_id = get_latest_secondary_primary_snap_id(
                local_cluster, local_pool, image)

            print(f"[CHECK] secondary snap id = {secondary_snap_id}")

            if secondary_snap_id == primary_snap_id:
                print("Snapshot fully synced")
                return True

        except Exception as e:
            print(f"[WAIT] retry due to: {e}")
            continue

    raise Exception("Snapshot sync failed (timeout)")

def test_image_status(
    image, require_up, state_pattern, description_pattern):

    status = image.mirror_image_get_status()

    if status["up"] != require_up:
        return False
    if status["state"] != state_pattern:
        return False
    if description_pattern:
        if not re.search(description_pattern, status["description"]):
            return False

    return True

def wait_for_status_in_pool_dir(cluster, pool, 
    image_name, require_up, state_pattern, description_pattern=None):

    sleeps = [1,2,4,8,8,8,8,8,16,16]

    for s in sleeps:
        ioctx, image = open_image(cluster, pool, image_name)
        try:

            if test_image_status(
                image, require_up, state_pattern, description_pattern):
                return True

        finally:
            image.close()
            ioctx.close()

        time.sleep(s)

    return False

def write_image(
    cluster, pool, image_name, image_size_mb=IMAGE_SIZE):
    ioctx = None
    image = None

    try:
        ioctx, image = open_image(cluster, pool, image_name)
        image_size = image_size_mb * 1024 * 1024

        # write between 200MB and 600MB
        write_size_mb = random.randint(200, image_size_mb)
        write_size = write_size_mb * 1024 * 1024
        offset = random.randint(0, image_size - write_size)

        print(
            f"[WRITE] {image_name} "
            f"offset={offset} "
            f"size={write_size_mb}MB"
        )

        chunk_size = 4 * 1024 * 1024
        remaining = write_size
        while remaining > 0:
            cur = min(chunk_size, remaining)
            image.write(os.urandom(cur), offset)
            offset += cur
            remaining -= cur

        image.flush()

    finally:
        if image:
            image.close()

        if ioctx:
            ioctx.close()

def remove_image(cluster, pool, image_name):
    ioctx = None

    try:
        cluster_conn = connect_cluster(cluster)

        ioctx = cluster_conn.open_ioctx(pool)

        rbd_inst = rbd.RBD()

        rbd_inst.remove(ioctx, image_name)

        print(
            f"[REMOVE] {cluster}/{pool}/{image_name}"
        )

    finally:
        if ioctx is not None:
            try:
                ioctx.close()
            except Exception:
                pass

def open_image(cluster, pool, image_name):
    cluster_conn = connect_cluster(cluster)

    ioctx = cluster_conn.open_ioctx(pool)

    image = rbd.Image(ioctx, image_name)

    return ioctx, image

def create_image(cluster, pool, image_name):
    cluster_conn = connect_cluster(cluster)

    ioctx = cluster_conn.open_ioctx(pool)

    rbd_inst = rbd.RBD()

    size_bytes = IMAGE_SIZE * 1024 * 1024

    rbd_inst.create(ioctx, image_name, size_bytes)

    ioctx.close()

def create_snapshot(cluster, pool, image_name, flags=0):
    ioctx, image = open_image(cluster, pool, image_name)

    image.mirror_image_create_snapshot(flags)

    image.close()
    ioctx.close()

def enable_image_mirror(cluster, pool, image_name):
    ioctx, image = open_image(cluster, pool, image_name)

    image.mirror_image_enable(
        rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT
    )

    image.close()
    ioctx.close()

def disable_image_mirror(cluster, pool, image_name, force=False):
    ioctx, image = open_image(cluster, pool, image_name)

    image.mirror_image_disable(force)

    image.close()
    ioctx.close()

def promote_image(cluster, pool, image_name, force=False):
    ioctx, image = open_image(cluster, pool, image_name)

    image.mirror_image_promote(force)

    image.close()
    ioctx.close()

def demote_image(cluster, pool, image_name):
    ioctx, image = open_image(cluster, pool, image_name)

    image.mirror_image_demote()

    image.close()
    ioctx.close()

def resync_image(cluster, pool, image_name):
    ioctx, image = open_image(cluster, pool, image_name)

    image.mirror_image_resync()

    image.close()
    ioctx.close()

def setup_tempdir():
    global TEMPDIR

    TEMPDIR = os.environ.get("RBD_MIRROR_TEMDIR")

    if TEMPDIR:
        os.makedirs(TEMPDIR, exist_ok=True)
    else:
        TEMPDIR = tempfile.mkdtemp()

    print(f"[TEMPDIR] {TEMPDIR}")


def main():

    NUM_IMAGES = 5
    NUM_ITERATIONS = 20

    if len(sys.argv) >= 2:
        NUM_IMAGES = int(sys.argv[1])

    if len(sys.argv) >= 3:
        NUM_ITERATIONS = int(sys.argv[2])

    print(
        f"[CONFIG] "
        f"num_images={NUM_IMAGES} "
        f"num_iterations={NUM_ITERATIONS}"
    )

    setup_tempdir()

    # clusters
    setup_cluster("cluster1")
    setup_cluster("cluster2")

    # pools
    setup_pools("cluster1", "cluster2")
    setup_pools("cluster2", "cluster1")

    # start mirror daemons
    start_mirrors("cluster1")
    start_mirrors("cluster2")

    for i in range(NUM_IMAGES):
        image = f"img{i}"
        create_image("cluster1", POOL, image)
        enable_image_mirror("cluster1", POOL, image)
        wait_for_snapshot_sync_complete(
            "cluster2", "cluster1", POOL, POOL, image)
        wait_for_status_in_pool_dir("cluster2", POOL, image, True,
                                    MIRROR_IMAGE_STATUS_STATE_REPLAYING)
        # compare_images("cluster2", "cluster1", POOL, POOL, image)

        images[image] = ImageState(
            cluster1_state=PRIMARY,
            cluster2_state=NON_PRIMARY
        )

    global MIRRORS_RUNNING
    global KILLING_SCHEDULED

    #schedule rbd-mirror daemons kill
    kill_after = random.randint(120, 240)
    kill_mirrors_after_delay(kill_after)
    KILLING_SCHEDULED = True

    for iteration in range(NUM_ITERATIONS):
        print(f"\n===== ITERATION {iteration} =====\n")
        dump_fd_count()
        old_image_id = 0
        if not MIRRORS_RUNNING:
            start_mirrors("cluster1")
            start_mirrors("cluster2")
            MIRRORS_RUNNING = True

        if len(PENDING_SYNCS) > 0:
            process_pending_syncs()

        # schedule next mirror daemons kill
        if not KILLING_SCHEDULED:
            kill_after = random.randint(120, 240)
            kill_mirrors_after_delay(kill_after)
            KILLING_SCHEDULED = True

        for image in images:
            cluster = random.choice([
                "cluster1",
                "cluster2"
            ])
            other = (
                "cluster2"
                if cluster == "cluster1"
                else "cluster1"
            )
            state = get_state(image, cluster)
            if state == IMAGE_DELETED:
                continue
            if state == PRIMARY:
                write_image(cluster, POOL, image)

            action, expected_success = \
                choose_action(state)

            print(
                f"[FUZZ] image={image} "
                f"cluster={cluster} "
                f"state={state} "
                f"action={action} "
                f"expect_success={expected_success}"
            )

            if expected_success:
                if action == "resync":
                    old_image_id = get_image_id(cluster, POOL, image)
                elif action in ("disable","force_disable"):
                    old_image_id = get_image_id(other, POOL, image)

            execute_action(cluster, image, action, expected_success)

            if not expected_success:
                continue

            update_local_state(image, cluster, action)
            if action == "resync":
                try:
                    validate_resync(old_image_id, other, cluster, POOL, image)
                except:
                    if not MIRRORS_RUNNING:
                        add_pending_sync(image, cluster, action, old_image_id)
                        continue
                    raise
            elif action in (
                "snapshot", "demote", "enable", "promote"):
                try:
                    wait_for_snapshot_sync_complete(other, cluster, POOL, POOL, image)
                    if action == "demote":
                        wait_for_status_in_pool_dir(other, POOL, image, True,
                            MIRROR_IMAGE_STATUS_STATE_UNKNOWN)
                    else:
                        wait_for_status_in_pool_dir(other, POOL, image, True,
                            MIRROR_IMAGE_STATUS_STATE_REPLAYING)
                    update_remote_state_after_sync(image, cluster, action)
                except:
                    if not MIRRORS_RUNNING:
                        add_pending_sync(image, cluster, action)
                        continue
                    raise
            elif action == "disable":
                try:
                    wait_for_image_deleted(other, POOL, image, old_image_id)
                    update_remote_state_after_sync(image, cluster, action)
                except:
                    if not MIRRORS_RUNNING:
                        add_pending_sync(image, cluster, action, old_image_id)
                        continue
                    raise
            elif action == "force_disable":
                if state in (NON_PRIMARY, NON_PRIMARY_DEMOTED):
                    remove_image(cluster, POOL, image)
                    setattr(images[image], f"{cluster}_state", IMAGE_DELETED)
                    other_state = get_state(image, other)
                    if other_state == PRIMARY:
                        # if remote is primary
                        # expect image to get copied again to local cluster
                        try:
                            wait_for_image_recreated(cluster, POOL, image, old_image_id)
                            wait_for_snapshot_sync_complete(cluster, other, POOL, POOL, image)
                            wait_for_status_in_pool_dir(cluster, POOL, image, True,
                                MIRROR_IMAGE_STATUS_STATE_REPLAYING)
                            setattr(images[image], f"{cluster}_state", NON_PRIMARY)
                        except:
                            if not MIRRORS_RUNNING:
                                add_pending_sync(image, cluster, action, old_image_id)
                                continue
                            raise
                else:
                    # when the image_state is NON_PRIMARY_DEMOTED
                    # the force_disable shouldn't propagate to the remote cluster
                    # expect image on remote/other cluster not getting deleted
                    try:
                        wait_for_image_deleted(other, POOL, image, old_image_id)
                    except Exception as e:
                        wait_for_status_in_pool_dir(cluster, POOL, image,
                            MIRRORS_RUNNING, MIRROR_IMAGE_STATUS_STATE_ERROR,
                            "error bootstrapping replay")
                        execute_action(other, image, "force_disable", True)
                        remove_image(other, POOL, image)
                        wait_for_image_deleted(other, POOL, image, old_image_id)
                        setattr(images[image], f"{other}_state", IMAGE_DELETED)

    for conn in CLUSTERS.values():
        conn.shutdown()

if __name__ == "__main__":
    main()
