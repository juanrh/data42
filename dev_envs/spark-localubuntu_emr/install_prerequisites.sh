#!/bin/bash

function prepare_anaconda {
	sudo mkdir -p /opt/anaconda
	sudo chmod -R 777 /opt/anaconda
	echo 'export PATH=/opt/anaconda/anaconda/bin/:${PATH}' > /opt/anaconda/activate_anaconda.sh
	echo "Install Anaconda for Python 2.7 at /opt/anaconda/anaconda downloading the intaller from http://continuum.io/downloads"
	echo "Then use 'source /opt/anaconda/activate_anaconda.sh' to activate anaconda"
}

function prepare_python27 {
	# Install pip and Fabric in the system version of python 2.7
	get_pip_url='https://bootstrap.pypa.io/get-pip.py'
	get_pip_name=$(basename $get_pip_url)
	pushd /tmp
	wget $get_pip_url
	sudo python2.7 $get_pip_name
	rm -f $get_pip_name
	popd
	sudo pip2.7 install Fabric
}

###################
# Main
###################
prepare_anaconda
prepare_python27