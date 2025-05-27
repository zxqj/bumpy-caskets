import os
import sys
import socket
import datetime
import yaml
from sh import borg
from enum import Enum, auto
import io
import click

def info(msg):
    print(f"\n{datetime.datetime.now()} {msg}\n", file=sys.stderr)

def load_excludes(yaml_path):
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    return data.get('excludes', [])

def stream_output(line):
    print(line, end='')

class Stage(Enum):
    Create = auto()
    Prune = auto()
    Compact = auto()

def get_next_archive_name(repo, hostname):
    # List existing archives in the repository
    existing_archives = borg.list(repo, "--short").splitlines()

    # Regex to match archive names of the form {hostname}-{date}-{number}
    pattern = re.compile(rf"^{re.escape(hostname)}-(\d{{4}}-\d{{2}}-\d{{2}})-(\d{{2}})$")
    today = datetime.date.today().isoformat()
    highest_number = 0

    for archive in existing_archives:
        match = pattern.match(archive)
        if match and match.group(1) == today:
            highest_number = max(highest_number, int(match.group(2)))

    # Increment the highest number and format it with zero-padding
    next_number = f"{highest_number + 1:02}"
    return f"{hostname}-{today}-{next_number}"
@click.command()
@click.option('--repo', '-r', default=None, help='Path to the Borg repository.')
@click.option('--password', '-p', default=None, help='Borg repository passphrase.')
@click.option('--excludes-list', multiple=True, default=['res/excludes.common.yaml'], help='Paths to YAML files with exclude patterns.')
def main(repo, password, excludes_list):
    # Check for repo and password in environment variables if not provided
    repo = repo or os.getenv('BORG_REPO')
    password = password or os.getenv('BORG_PASSPHRASE')

    if not repo:
        click.echo("Error: Borg repository (--repo) not provided and BORG_REPO environment variable not set.", err=True)
        sys.exit(1)

    if not password:
        click.echo("Error: Borg passphrase (--password) not provided and BORG_PASSPHRASE environment variable not set.", err=True)
        sys.exit(1)

    os.environ['BORG_REPO'] = repo
    os.environ['BORG_PASSPHRASE'] = password

    # Load excludes from all provided files
    excludes = []
    for exclude_file in excludes_list:
        excludes.extend(load_excludes(exclude_file))

    hostname = socket.gethostname()
    # Replace the archive_name generation in the main function
    archive_name = get_next_archive_name(repo, hostname)

    fail_stage = Stage.Create
    error_buffer = io.StringIO()
    output_args = {"_out": stream_output, "_err": stream_output}

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
        create_args += [f"{repo}::{archive_name}", '/']

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
            repo
        ]
        borg.prune(*prune_args, **output_args)
        fail_stage = Stage.Compact

        info("Compacting repository")
        compact_args = [repo]
        borg.compact(*compact_args, **output_args)

    except borg.ErrorReturnCode as e:
        # Print the captured standard error
        print(f"Backup failed at stage: {fail_stage.name}")
        print(f"Command failed with exit code: {e.exit_code}")
        sys.exit(e.exit_code)
