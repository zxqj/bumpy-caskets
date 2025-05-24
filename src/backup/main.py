import os
import sys
import socket
import datetime
import yaml
from sh import borg
from enum import Enum, auto
import io

BORG_REPO = '/mnt/backup/backup'
BORG_PASSPHRASE = 'sarraceniaalabamensis'
EXCLUDES_YAML = '/etc/backup.yaml'

def info(msg):
    print(f"\n{datetime.datetime.now()} {msg}\n", file=sys.stderr)

def load_excludes(yaml_path):
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    return data.get('excludes', [])

class Stage(Enum):
    Create = auto()
    Prune = auto()
    Compact = auto()

def main():
    os.environ['BORG_REPO'] = "/mnt/backup/backup"
    os.environ['BORG_PASSPHRASE'] = "sarraceniaalabamensis"

    excludes = load_excludes(EXCLUDES_YAML)
    hostname = socket.gethostname()
    archive_name = f"{hostname}-{datetime.date.today().isoformat()}"

    fail_stage = Stage.Create
    error_buffer = io.StringIO()
    output_args = {"_out": sys.stdout, "_err": error_buffer}

    info("Starting backup")
    fail_stage = Stage.Create
    try:
        create_args = [
            '--verbose',
            '--filter', 'AME',
            '--list',
            '--stats',
            '--show-rc',
            '--compression', 'lz4',
            '--exclude-caches',
        ]

        for path in excludes:
            create_args += ['--exclude', path]
        create_args += [f"{BORG_REPO}::{archive_name}", '/']

        borg.create(*create_args, **output_args)
        fail_stage = Stage.Prune

        info("Pruning repository")
        prune_args = [
            '--list',
            '--glob-archives', f'{hostname}-*',
            '--show-rc',
            '--keep-daily', '7',
            '--keep-weekly', '4',
            '--keep-monthly', '6',
            BORG_REPO
        ]
        borg.prune(*prune_args, **output_args)
        fail_stage = Stage.Compact

        info("Compacting repository")
        compact_args = [BORG_REPO]
        borg.compact(*compact_args, **output_args)

    except borg.ErrorReturnCode as e:
        # Print the captured standard error
        print(f"Backup failed at stage: {fail_stage.name}")
        print(error_buffer.getvalue())
        print(f"Command failed with exit code: {e.exit_code}")
        sys.exit(e.exit_code)
