#!/bin/bash

function prepare_anaconda {
	sudo mkdir -p /opt/anaconda
	sudo chmod -R 777 /opt/anaconda
	echo 'export PATH=/opt/anaconda/anaconda/bin/:${PATH}' > /opt/anaconda/activate_anaconda.sh
	echo "Install Anaconda for Python 2.7 at /opt/anaconda/anaconda downloading the intaller from http://continuum.io/downloads"
	echo "Then use 'source /opt/anaconda/activate_anaconda.sh' to activate anaconda"
}

function prepare_eclipse_kepler {
    echo 'Download "Eclipse IDE for Java Developers" for Linux 64 bits from http://www.eclipse.org/downloads/'
    echo "We need Scala 2.10 for Spark, and Eclipse only supports one plugin installation for Scala at a time"
    echo "Install Scala IDE 3.0.3 for Eclipse from http://scala-ide.org/download/prev-stable.html using the update site http://download.scala-ide.org/sdk/helium/e38/scala210/stable/site"
}

function prepare_python27 {
	# Install pip and Fabric in the system version of python 2.7
    # Note this Python version is different to Anaconda python
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
prepare_eclipse_kepler
prepare_python27