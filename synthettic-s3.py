import boto3
import hashlib
import string
import json
from datetime import datetime, UTC
import argparse
from colorama import Fore, Style, init as colorama_init


from random import choices, randrange

class WorkloadConfig:
    def __init__(self):
        self.bucket_name = None
        self.prefix = "demo-objects/"
        self.metadata_object_key = self.prefix + "metadata.json"

        self.max_obj_ids = 100
        self.max_objs = 20
        self.obj_max_size = 10000


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

def get_object_key(env, obj_id):
    return f"{env.conf.prefix}object_{obj_id}.txt"

def load_metadata(env):
    print(f"loading {env.conf.bucket_name}/{env.conf.metadata_object_key}")
    try:
        response = env.s3.get_object(Bucket=env.conf.bucket_name, Key=env.conf.metadata_object_key)
        metadata_content = response['Body'].read()
        return json.loads(metadata_content)
    except:
        pass

    return { 'objects': {},
            'generated_at': None }

def get_object(env, bucket_name, key):
    response = env.s3.get_object(Bucket=bucket_name, Key=key)
    return response['Body'].read().decode('utf-8')

def put_object(env, bucket_name, key, data):
    env.s3.put_object(Bucket=bucket_name, Key=key, Body=data)

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
        hash_digest = sha256(content_bytes)
        obj_num = randrange(self.conf.max_obj_ids)
        object_key = get_object_key(self.env, obj_num + 1)

        put_object(self.env, self.conf.bucket_name, object_key, content_bytes)

        print(f"uploaded: {object_key}\tsize={object_size}\thash={hash_digest}")

        obj_meta = {
            "object_key": object_key,
            "size": object_size,
            "sha256": hash_digest
        }

        return obj_num + 1, obj_meta


    def generate_objects(self):
        num_objs = randrange(self.conf.max_objs) + 1
        for i in range(num_objs):
            obj_num, obj_meta = self.gen_object()

            self.metadata['objects'][str(obj_num)] = obj_meta


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

            actual_obj_data = get_object(self.env, self.conf.bucket_name, key)
            actual_hash = sha256(actual_obj_data.encode('utf8'))

            if actual_hash == obj_hash:
                result_str = Fore.GREEN + 'OK'
            else:
                result_str = Fore.RED + 'ERROR'
                fail_count += 1
                success = False

            print(f'{obj_id:>3} : expected: {obj_hash} actual: {actual_hash} ... ' + result_str)

        if success:
            print(Fore.GREEN + 'SUCCESS')
        else:
            print(f'{fail_count} objects with unexpected content')
            print(Fore.RED + 'FAIL')

    def copy_bucket(self, dest_bucket):
        self.env.s3.create_bucket(Bucket=dest_bucket)
        for key in self.env.s3.list_objects(Bucket=self.conf.bucket_name)['Contents']:
            k = key['Key']
            if not k.startswith(self.conf.prefix):
                continue
            print(f'copying {self.conf.bucket_name}/{k} -> {dest_bucket}/{k}')

            obj_data = get_object(self.env, self.conf.bucket_name, k)
            put_object(self.env, dest_bucket, k, obj_data)


def main():
    colorama_init(autoreset = True)
    conf = WorkloadConfig()

    parser = argparse.ArgumentParser(description="Script that takes a command and an optional bucket.")
    
    # Positional argument
    parser.add_argument('command', choices=['generate', 'create-snapshot', 'get-meta', 'validate', 'list-snapshots', 'copy-bucket'], type=str)
    
    # Optional argument
    parser.add_argument('--bucket', '-b', type=str, required=True)
    parser.add_argument('--prefix', type=str)
    parser.add_argument('--snap-name', type=str)
    parser.add_argument('--description', type=str)
    parser.add_argument('--dest-bucket', type=str)

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
    elif args.command == 'create-snapshot':
        if not args.snap_name:
            parser.error("--snap-name is required")

        create_snapshot(env, args.snap_name, args.description)
    elif args.command == 'list-snapshots':
        snaps, snaps_by_name = list_snapshots(env)
        print(json.dumps(snaps_by_name, indent=4, default=str))

    elif args.command == 'get-meta':
        meta = load_metadata(env)
        print(meta)
    
    elif args.command == 'validate':
        workload.validate()

    elif args.command == 'copy-bucket':
        if not args.dest_bucket:
            parser.error("--dest-bucket is required")

        workload.copy_bucket(args.dest_bucket)

if __name__ == "__main__":
    main()
