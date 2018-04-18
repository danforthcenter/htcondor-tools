# Donald Danforth Plant Science Center Data Archiving Tool

## Usage

```
usage: archive [-h] {configure,aws,local} ...

Bioinformatics archiving tools.

positional arguments:
  {configure,aws,local}
    configure           Configure the data archiving tool.
    aws                 AWS data archiving tool. Uploads files to AWS and
                        stores S3 metadata.
    local               Local data archiving tool. Uploads files to a local
                        archive target and stores metadata.

optional arguments:
  -h, --help            show this help message and exit
```

The `archive` tool has three sub programs:

### Configure

The `configure` tool should be run first to create some logging directories
and can be used to store your archiving resource (AWS or a local resource) information in a file so that the information
does not need to be reentered on the command line every time.

### AWS

The `aws` tool archives data to an Amazon Web Services S3 bucket that is configured to migrate data to the Glacier
low-cost archival storage system. 

#### Usage

```
usage: archive aws [-h] --files FILES [--delete] [--bucket BUCKET]

optional arguments:
  -h, --help       show this help message and exit
  --files FILES    Files to be archived.
  --delete         Delete local files if archiving is successful.
  --bucket BUCKET  AWS S3 bucket name. Overrides bucket set in the
                   configuration file.
```

### Local

The `local` tool archives data to any local computer resource that is accessible via SSH/Rsync.

#### Usage

```
usage: archive local [-h] --files FILES [--delete] [--hostname HOSTNAME]
                     [--username USERNAME] [--path PATH]

optional arguments:
  -h, --help           show this help message and exit
  --files FILES        Files to be archived.
  --delete             Delete local files if archiving is successful.
  --hostname HOSTNAME  Local archive target computer/server hostname/IP.
  --username USERNAME  Local archive target computer/server username/login.
  --path PATH          Local archive target computer/server volume path.
```
