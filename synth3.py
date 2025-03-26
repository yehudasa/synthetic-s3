import sys
import boto3
import hashlib
import string
import json
import time
import argparse
import random

from random import choices, randrange, sample
from datetime import datetime, UTC
from colorama import Fore, Style, init as colorama_init

class WorkloadConfig:
    def __init__(self):
        self.bucket_name = None
        self.prefix = "demo-objects/"
        self.metadata_object_key = self.prefix + "metadata.json"

        self.max_obj_ids = 100
        self.max_objs = 20
        self.obj_max_size = 10000

def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def sha256(data):
    return hashlib.sha256(data).hexdigest()

def random_text(length):
    return ''.join(choices(string.ascii_letters + string.digits + " ", k=length))

def enable_snapshots(env):
    env.s3.put_bucket_snapshots_configuration(Bucket = env.conf.bucket_name, BucketSnapsConf = { 'Enabled': True } )

def create_snapshot(env, snap_name, description):
    enable_snapshots(env)
    response = env.s3.create_bucket_snapshot(Bucket = env.conf.bucket_name, SnapConf = { 'Name': snap_name, 'Description': description or ''} )
    snap = response['Snapshot']

    print(json.dumps(snap, indent=4, default=str))
    #result = { 'ID': snap['ID'],
    #           'Name': snap['Info']['Name'],
    #           'Description': snap['Info']['Description'] }
    #print(json.dumps(result, indent=4))

def list_snapshots(env, start_after = None):
    enable_snapshots(env)
    response = env.s3.list_bucket_snapshots(Bucket = env.conf.bucket_name)
    snaps = response['Snapshots']

    ret_snaps = []

    snaps_by_name = {}

    for snap in snaps:
        snap_id = snap['ID']
        if start_after is not None and int(snap_id) <= int(start_after):
            continue

        ret_snaps.append(snap)
        snap_name = snap['Info']['Name']

        snaps_by_name[snap_name] = snap_id

    return ret_snaps, snaps_by_name

def get_snap_id(env, snap_id, snap_name):
    if snap_id is not None:
        return snap_id

    if not snap_name:
        return None

    snaps, snaps_by_name = list_snapshots(env)
    snap_id = snaps_by_name.get(snap_name)
    if snap_id is None:
        raise Exception(Fore.RED + 'ERROR: snapshot not found: ' + snap_name)

    return snap_id

def list_objects(env, snap_range = None):
    eff_snap_range = snap_range or env.snap_range
    if eff_snap_range is None:
        return env.s3.list_objects(Bucket=env.conf.bucket_name)['Contents']
    else:
        result = env.s3.list_objects(Bucket=env.conf.bucket_name, SnapRange = eff_snap_range)
        return result.get('Contents', [])

def get_object_key(env, obj_id):
    return f"{env.conf.prefix}object_{obj_id}.txt"

def load_metadata(env):
    print(f"loading {env.conf.bucket_name}/{env.conf.metadata_object_key}")
    try:
        metadata_content = get_object(env, env.conf.bucket_name, env.conf.metadata_object_key)
        return json.loads(metadata_content)
    except Exception as e:
        print(e)

    return { 'objects': {},
            'generated_at': None }

def get_object(env, bucket_name, key, snap_id = None):
    eff_snap_id = snap_id or env.snap_id
    if eff_snap_id is None:
        response = env.s3.get_object(Bucket=bucket_name, Key=key)
    else:
        response = env.s3.get_object(Bucket=bucket_name, Key=key, SnapId=int(eff_snap_id))
    return response['Body'].read().decode('utf-8')

def put_object(env, bucket_name, key, data):
    env.s3.put_object(Bucket=bucket_name, Key=key, Body=data)

class Env:
    def __init__(self, conf, init_snapshot = True):
        self.conf = conf
        self.s3 = boto3.client("s3")

        self.snap_name = conf.snap_name
        self.from_snap_name = conf.from_snap_name
        if init_snapshot:
            self.snap_id = get_snap_id(self, conf.snap_id, conf.snap_name)
            self.from_snap_id = get_snap_id(self, conf.from_snap_id, conf.from_snap_name)
        else:
            self.snap_id = conf.snap_id
            self.from_snap_id = conf.from_snap_id
        
        self.snap_range = None
        if self.snap_id is not None:
            self.snap_range = str(self.snap_id)

        snap_id_str = self.snap_range or ''

        if conf.all_objs:
            self.snap_range = f"-{snap_id_str}"
        elif conf.from_snap_id is not None:
            self.snap_range = f"{conf.from_snap_id}-{snap_id_str}"


class SyntheticS3Workload:
    def __init__(self, env):
        self.env = env
        self.conf = env.conf
        env.s3.create_bucket(Bucket=self.conf.bucket_name)
        self.metadata = load_metadata(env)

    def gen_object(self, obj_id):
        object_size = randrange(self.conf.obj_max_size)
        content = random_text(object_size)
        content_bytes = content.encode('utf-8')
        hash_digest = sha256(content_bytes)
        object_key = get_object_key(self.env, obj_id)

        put_object(self.env, self.conf.bucket_name, object_key, content_bytes)

        print(f"uploaded: {object_key}\tsize={object_size}\thash={hash_digest}")

        obj_meta = {
            "object_key": object_key,
            "size": object_size,
            "sha256": hash_digest
        }

        return obj_meta


    def generate_objects(self):
        num_objs = randrange(self.conf.max_objs) + 1
        obj_ids = random.sample(range(1, 100), num_objs)
        for obj_id in sorted(obj_ids):
            obj_meta = self.gen_object(obj_id)

            self.metadata['objects'][str(obj_id)] = obj_meta

        print(f'Created {num_objs} objects')


    def flush_meta(self):
        self.metadata["generated_at"] = datetime.now(UTC).isoformat() + "Z",
        metadata_json = json.dumps(self.metadata)

        objs_json = json.dumps(self.metadata['objects'])

        hash_digest = sha256(objs_json.encode('utf-8'))
        put_object(self.env, self.conf.bucket_name, self.conf.metadata_object_key, metadata_json.encode("utf-8"))

        print(f"Uploaded metadata to: s3://{self.conf.bucket_name}/{self.conf.metadata_object_key}")
        print(f"meta hash: {hash_digest}")

    def validate(self):

        success = True
        fail_count = 0

        for obj_id, obj_info in dict(sorted(self.metadata['objects'].items(), key=lambda item: int(item[0]))).items():
            key = obj_info['object_key']
            size = obj_info['size']
            obj_hash = obj_info['sha256']

            try:
                actual_obj_data = get_object(self.env, self.conf.bucket_name, key)
                actual_hash = sha256(actual_obj_data.encode('utf8'))
            except:
                actual_hash = '<error>'

            if actual_hash == obj_hash:
                result_str = Fore.GREEN + 'OK'
            else:
                result_str = Fore.RED + 'ERROR'
                fail_count += 1
                success = False

            print(f'{obj_id:>3} : expected: {obj_hash:<65} actual: {actual_hash:<65} ... ' + result_str)

        if success:
            print(Fore.GREEN + 'SUCCESS')
        else:
            print(f'{fail_count} objects with unexpected content')
            print(Fore.RED + 'FAIL')

    def copy_bucket(self, dest_bucket, snap_id = None, incremental = False):
        self.env.s3.create_bucket(Bucket=dest_bucket)
        snap_range = None
        if snap_id:
            snap_range = str(snap_id)

            if not incremental:
                snap_range = '-' + snap_range

        for key in list_objects(self.env, snap_range = snap_range):
            k = key['Key']
            if not k.startswith(self.conf.prefix):
                continue
            print(f'copying {self.conf.bucket_name}/{k} -> {dest_bucket}/{k}')

            obj_data = get_object(self.env, self.conf.bucket_name, k, snap_id)
            put_object(self.env, dest_bucket, k, obj_data)

    def sync_bucket(self, dest_bucket, follow_snaps, cur_snap_id, incremental):
        if not follow_snaps:
            self.copy_bucket(dest_bucket)

        else:
            snaps, _ = list_snapshots(self.env, cur_snap_id)

            if len(snaps) > 0 and not incremental:
                snaps = [ snaps[-1] ] # the last snapshot

            for snap in snaps:
                cur_snap_id=snap['ID']
                cur_snap_name=snap['Info']['Name']

                inc_str = 'incremental' if incremental else 'full'
                print(f'Syncing snapshot: {cur_snap_name} ({inc_str} sync)')

                self.copy_bucket(dest_bucket, snap_id = cur_snap_id, incremental=incremental)

                print(f'Finished syncing snapshot: {cur_snap_name} ({inc_str} sync)')

        return cur_snap_id


def main():
    colorama_init(autoreset = True)
    conf = WorkloadConfig()

    parser = argparse.ArgumentParser(description="Script that takes a command and an optional bucket.")
    
    # Positional argument
    parser.add_argument('command', choices=['generate', 'create-snapshot', 'get-meta', 'validate', 'list-snapshots',
                                            'copy-bucket', 'sync-bucket', 'list-objects'], type=str)
    
    # Optional argument
    parser.add_argument('--bucket', '-b', type=str, required=True)
    parser.add_argument('--prefix', type=str)
    parser.add_argument('--snap-name', type=str)
    parser.add_argument('--snap-id', type=str)
    parser.add_argument('--description', type=str)
    parser.add_argument('--dest-bucket', type=str)
    parser.add_argument('--from-snap-id', type=str)
    parser.add_argument('--from-snap-name', type=str)
    parser.add_argument('--all-objs', action='store_true')
    parser.add_argument('--forever', action='store_true')
    parser.add_argument('--follow-snapshots', action='store_true')
    parser.add_argument('--auto-snap', action='store_true')
    parser.add_argument('--auto-snap-ratio', type=int, default=5)

    args = parser.parse_args()

    print(f"Command: {args.command}")
    print(f"Bucket: {args.bucket}")

    conf.bucket_name = args.bucket
    conf.prefix = args.prefix or conf.prefix
    conf.snap_id = args.snap_id
    conf.snap_name = args.snap_name
    conf.from_snap_id = args.from_snap_id
    conf.from_snap_name = args.from_snap_name
    conf.all_objs = args.all_objs

    init_snapshot = args.command != 'create-snapshot'
    env = Env(conf, init_snapshot=init_snapshot)
    workload = SyntheticS3Workload(env)

    if args.command == 'generate':
        snap_ratio = args.auto_snap_ratio or 5
        i = 0
        while True:
            workload.generate_objects()
            workload.flush_meta()

            if args.auto_snap or not args.forever:
                i += 1
                if i % snap_ratio:
                    snap_name='snap-' + random_text(8)

                    create_snapshot(env, snap_name, 'Auto snapshot')

            if not args.forever:
                break

            time.sleep(2)

    elif args.command == 'create-snapshot':
        if not args.snap_name:
            parser.error("--snap-name is required")

        create_snapshot(env, args.snap_name, args.description)
    elif args.command == 'list-snapshots':
        snaps, snaps_by_name = list_snapshots(env)
        print(json.dumps(snaps_by_name, indent=4, default=str))

    elif args.command == 'list-objects':
        try:
            objs = list_objects(env)
        except Exception as e:
            print(e)
            sys.exit(1)

        total_size = 0
        for o in objs:
            print(f"{o['Key']:<28} {o['Size']:>10} {str(o['LastModified'])}")
            # print(o['Key'] + ' ' + str(o['size']) + ' ' str(o['Date']))
            total_size += o['Size']
        print(f'\nTotal: {len(objs)} objects / {sizeof_fmt(total_size)}')

    elif args.command == 'get-meta':
        meta = load_metadata(env)
        print(meta)
    
    elif args.command == 'validate':
        workload.validate()

    elif args.command == 'copy-bucket':
        if not args.dest_bucket:
            parser.error("--dest-bucket is required")

        workload.copy_bucket(args.dest_bucket)

    elif args.command == 'sync-bucket':
        if not args.dest_bucket:
            parser.error("--dest-bucket is required")

        cur_snap_id = None
        incremental = False
        while True:
            cur_snap_id = workload.sync_bucket(args.dest_bucket, args.follow_snapshots, cur_snap_id, incremental)
            if not args.forever:
                break

            incremental = True

            time.sleep(2)


if __name__ == "__main__":
    main()
