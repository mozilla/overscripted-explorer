Steps to setup AWS Server

Server dependencies
    $ sudo yum update
    $ sudo reboot
    $ sudo yum install wget tmux vim git bzip2 java-1.8.0-openjdk

Get miniconda
    $ wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    $ sh Miniconda3-latest-Linux-x86_64.sh

Add github ssh keys to box (won't be necessary when repos public)
    $ git clone git@github.com:mozilla/overscripted-explorer.git

Get data (instructions for AWS do it however)
    $ pip install awscli
    $ mkdir .aws
    $ vim credentials

Set-up ephemeral drive and get data in it
    $ lsblk # to see location of drive
    $ sudo mkfs.ext4 -E nodiscard /dev/nvme0n1
    $ sudo mkdir /mnt/Data
    $ sudo mount -o discard /dev/nvme0n1 /mnt/Data
    $ sudo chown ec2-user:ec2-user /mnt/Data
    $ cd /mnt/Data
    $ aws s3 cp s3://safe-ucosp-2017/safe_dataset/v1/clean.parquet . --recursive

Make sure environment variable `DATA_DIR` is set.

Run server in a tmux session (ahem - this isn't prod!)
    $ cd overscripted-explorer
    $ tmux new -s bokeh
    $ conda env create -f environment.yaml
    $ source activate overscripted_explorer
    $ export DATA_DIR=/mnt/Data
    $ bokeh serve --address 0.0.0.0 text_search --allow-websocket-origin=address-of-server:5006 
