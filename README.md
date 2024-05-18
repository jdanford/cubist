# cubist

> In Cubist works of art, the subjects are analyzed, broken up, and reassembled in an abstract form — instead
> of depicting objects from a single perspective, the artist depicts the subject from multiple perspectives
> to represent the subject in a greater context.

cubist takes files, splits them up intelligently into blocks, and uploads only the blocks it hasn't seen
before. This means that similar files will share many underlying blocks, greatly minimizing the amount of data
transferred and stored. cubist is roughly similar to Git and rsync, and very similar to
[BorgBackup](https://www.borgbackup.org/), except that it is designed around the features and constraints
of cloud object storage systems like Amazon S3.

See [TECHNICAL.md](./TECHNICAL.md) for more details.

## Setup

cubist requires read/write access to a bucket on S3, or any object storage system with an S3-compatible API.
It uses the official AWS SDK, so authentication is handled through standard methods such as stored
credentials or environment variables, namely `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Using an S3
alternative can be done by setting `AWS_ENDPOINT_URL`.

cubist gets/puts objects with the following keys:

| Object type      | Key                 |
| ---------------- | ------------------- |
| Archive          | `archives/<hash>`   |
| Block            | `blocks/<hash>`     |
| Archive metadata | `metadata/archives` |
| Block metadata   | `metadata/blocks`   |

## Subcommands

If the `--bucket` option is not supplied to a subcommand, it will be read from the environment variable `CUBIST_BUCKET`.

### `backup`

Back up files to an archive

```text
Usage: cubist backup [OPTIONS] <PATHS>...

Arguments:
  <PATHS>...  Files to back up

Options:
  -l, --compression-level <NUM>  Compression level (1-19) [default: 3]
  -s, --target-block-size <NUM>  Target size for blocks [default: 1048576]
  -j, --tasks <NUM>              Number of background tasks to use [default: 8]
  -t, --transient                Undo all changes when finished
  -n, --dry-run                  Show operations that would be performed without actually doing them
  -b, --bucket <BUCKET>          S3 bucket
      --stats <STATS>            Format to use for stats [possible values: basic, json]
      --color <COLOR>            When to use color in output [default: auto] [possible values: auto, always, never]
  -v, --verbose...               Print more output
  -q, --quiet...                 Print less output
  -h, --help                     Print help
  -V, --version                  Print version
```

### `restore`

Restore files from an archive

```text
Usage: cubist restore [OPTIONS] <ARCHIVE> [PATHS]...

Arguments:
  <ARCHIVE>   Archive to restore from
  [PATHS]...  Files to restore (or all files if empty)

Options:
      --order <ORDER>    Archive traversal order [default: depth-first] [possible values: depth-first, breadth-first]
  -j, --tasks <NUM>      Number of background tasks to use [default: 8]
  -n, --dry-run          Show operations that would be performed without actually doing them
  -b, --bucket <BUCKET>  S3 bucket
      --stats <STATS>    Format to use for stats [possible values: basic, json]
      --color <COLOR>    When to use color in output [default: auto] [possible values: auto, always, never]
  -v, --verbose...       Print more output
  -q, --quiet...         Print less output
  -h, --help             Print help
  -V, --version          Print version
```

### `delete`

Delete one or more archives

```text
Usage: cubist delete [OPTIONS] <ARCHIVES>...

Arguments:
  <ARCHIVES>...  Archive(s) to delete

Options:
  -j, --tasks <NUM>      Number of background tasks to use [default: 8]
  -n, --dry-run          Show operations that would be performed without actually doing them
  -b, --bucket <BUCKET>  S3 bucket
      --stats <STATS>    Format to use for stats [possible values: basic, json]
      --color <COLOR>    When to use color in output [default: auto] [possible values: auto, always, never]
  -v, --verbose...       Print more output
  -q, --quiet...         Print less output
  -h, --help             Print help
  -V, --version          Print version
```

### `archives`

List archives

```text
Usage: cubist archives [OPTIONS]

Options:
  -b, --bucket <BUCKET>  S3 bucket
      --stats <STATS>    Format to use for stats [possible values: basic, json]
      --color <COLOR>    When to use color in output [default: auto] [possible values: auto, always, never]
  -v, --verbose...       Print more output
  -q, --quiet...         Print less output
  -h, --help             Print help
  -V, --version          Print version
```

### `cleanup`

Clean up orphaned blocks and archives

```text
Usage: cubist cleanup [OPTIONS]

Options:
      --tasks <NUM>      Number of background tasks to use [default: 8]
  -n, --dry-run          Show operations that would be performed without actually doing them
  -b, --bucket <BUCKET>  S3 bucket
      --stats <STATS>    Format to use for stats [possible values: basic, json]
      --color <COLOR>    When to use color in output [default: auto] [possible values: auto, always, never]
  -v, --verbose...       Print more output
  -q, --quiet...         Print less output
  -h, --help             Print help
  -V, --version          Print version
```
