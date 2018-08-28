Steps to setup AWS Server

* RHEL 7
* m5d.large

$ sudo yum update
$ sudo reboot
$ sudo yum install wget tmux vim git bzip2 java-1.8.0-openjdk


$ wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
$ sh Miniconda3-latest-Linux-x86_64.sh

Add github ssh keys to box (won't be necessary when repos public)

$ git clone git@github.com:mozilla/overscripted-explorer.git
$ cd overscripted-explorer
$ tmux new -s bokeh
$ conda env create -f environment.yaml
$ source activate overscripted_explorer


