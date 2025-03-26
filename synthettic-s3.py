import boto3
import hashlib
import string
import json
from datetime import datetime, UTC

from random import choices, randrange

class WorkloadConfig:
    def __init__(self):
        self.bucket_name = None
        self.prefix = "demo-objects/"
        self.metadata_object_key = self.prefix + "metadata.json"

        self.max_obj_ids = 100
        self.max_objs = 20
        self.obj_max_size = 10000


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

def list_snapshots(env):
    enable_snapshots(env)
    response = env.s3.list_bucket_snapshots(Bucket = env.conf.bucket_name)
    snaps = response['Snapshots']

    snaps_by_name = {}

    for k in snaps:
        snap_id = k['ID']
        snap_name = k['Info']['Name']

        snaps_by_name[snap_name] = snap_id

    return snaps, snaps_by_name


def load_metadata(env):
    print(f"loading {env.conf.bucket_name}/{env.conf.metadata_object_key}")
    try:
        response = env.s3.get_object(Bucket=env.conf.bucket_name, Key=env.conf.metadata_object_key)
        metadata_content = response['Body'].read().decode('utf-8')
        print(json.loads(metadata_content))
        return json.loads(metadata_content)
    except:
        pass

    return { 'objects': {},
            'generated_at': None }


class Env:
    def __init__(self, conf):
        self.conf = conf
        self.s3 = boto3.client("s3")

class SyntheticS3Workload:
    def __init__(self, env):
        self.env = env
        self.conf = env.conf
        env.s3.create_bucket(Bucket=self.conf.bucket_name)
        self.metadata = load_metadata(env)

    def gen_object(self):
        object_size = randrange(self.conf.obj_max_size)
        content = random_text(object_size)
        content_bytes = content.encode('utf-8')
        hash_digest = hashlib.sha256(content_bytes).hexdigest()
        obj_num = randrange(self.conf.max_obj_ids)
        object_key = f"{self.conf.prefix}object_{obj_num + 1}.txt"

        self.env.s3.put_object(Bucket=self.conf.bucket_name, Key=object_key, Body=content_bytes)

        print(f"uploaded: {object_key}\tsize={object_size}\thash={hash_digest}")

        obj_meta = {
            "object_key": object_key,
            "size": object_size,
            "sha256": hash_digest
        }

        return obj_num, obj_meta


    def generate_objects(self):
        num_objs = randrange(self.conf.max_objs) + 1
        for i in range(num_objs):
            obj_num, obj_meta = self.gen_object()

            self.metadata['objects'][obj_num] = obj_meta


    def flush_meta(self):
        self.metadata["generated_at"] = datetime.now(UTC).isoformat() + "Z",
        metadata_json = json.dumps(self.metadata)

        objs_json = json.dumps(self.metadata['objects'])

        hash_digest = hashlib.sha256(objs_json.encode('utf-8')).hexdigest()
        self.env.s3.put_object(Bucket=self.conf.bucket_name, Key=self.conf.metadata_object_key, Body=metadata_json.encode("utf-8"))

        print(f"Uploaded metadata to: s3://{self.conf.bucket_name}/{self.conf.metadata_object_key}")
        print(f"meta hash: {hash_digest}")

import argparse

def main():
    conf = WorkloadConfig()

    parser = argparse.ArgumentParser(description="Script that takes a command and an optional bucket.")
    
    # Positional argument
    parser.add_argument('command', choices=['generate', 'snapshot', 'get-meta', 'verify', 'list-snapshots'], type=str)
    
    # Optional argument
    parser.add_argument('--bucket', '-b', type=str, required=True)
    parser.add_argument('--prefix', type=str)
    parser.add_argument('--snap-name', type=str)
    parser.add_argument('--description', type=str)

    args = parser.parse_args()

    print(f"Command: {args.command}")
    print(f"Bucket: {args.bucket}")

    conf.bucket_name = args.bucket
    conf.prefix = args.prefix or conf.prefix

    env = Env(conf)
    workload = SyntheticS3Workload(env)

    if args.command == 'generate':
        workload.generate_objects()
        workload.flush_meta()
    elif args.command == 'snapshot':
        if not args.snap_name:
            parser.error("--snap-name is required when command is 'snapshot'")

        create_snapshot(env, args.snap_name, args.description)
    elif args.command == 'list-snapshots':
        snaps, snaps_by_name = list_snapshots(env)
        print(json.dumps(snaps_by_name, indent=4, default=str))

    elif args.command == 'get-meta':
        meta = load_metadata(env)
        print(meta)


if __name__ == "__main__":
    main()
