#!/usr/bin/python2.7

'''
Local devel environment for Spark under XUbuntu 14.10
'''

from fabric.api import local, task, lcd
import os

_install_root = "/opt"
_bashrc = os.path.join(os.environ["HOME"], ".bashrc")
###################
# Utils
###################
def create_public_dir(dir_path):
    '''
    Creates a directory dir_path with 777 permissions
    NOTE: Local execution
    '''
    local("sudo mkdir -p " + dir_path)
    local("sudo chmod -R 777 " + dir_path)

def download_and_uncompress(url):
    '''
    Downloads and uncompresses the tgz file at url, in the current directory
    NOTE: Local execution

    :returns: the name of the file as returned by os.path.basename
    '''
    file_name = os.path.basename(url)
    local("wget " +  url)
    local("tar xzf " + file_name)
    return file_name

def get_child_name_with_prefix(dir_prefix): 
    '''
    NOTE: Local execution

    :retuns: the name of the child directory of the current directory 
    '''
    dir_name = local("ls -d */ | grep ^" + dir_prefix, capture = True)[:-1]
    return dir_name

def append_to_bashrc(line):
   '''
   Appends line to ~/.bashrc. Note line is not quoted, and echo will be used for this, so quote accordingly to use or avoid variable expansion as required
   NOTE: Local execution
   '''
   local("echo {line} >> ".format(line=line) + _bashrc)

###################
# Tasks
###################
_maven3_url = "ftp://mirror.reverse.net/pub/apache/maven/maven-3/3.3.1/binaries/apache-maven-3.3.1-bin.tar.gz"
@task
def install_maven3(): 
    '''
    Installs maven 3
    NOTE: Local execution
    See: https://happilyblogging.wordpress.com/2011/12/13/installing-maven-on-ubuntu-10-10/
    '''
    print "Installing maven 3"
    _maven_root = os.path.join(_install_root, "maven3")
    print _maven_root 
    create_public_dir(_maven_root)
    with lcd(_maven_root):
        mvn_tgz_file = download_and_uncompress(_maven3_url)
        mvn_dir = get_child_name_with_prefix("apache-maven-3")
        mvn_full_path = os.path.join(_maven_root, mvn_dir)
        append_to_bashrc("'export M2_HOME={mvn_full_path}'".format(mvn_full_path=mvn_full_path))
        append_to_bashrc("'export M2=$M2_HOME/bin'")
        append_to_bashrc("'PATH=$M2:$PATH'")
        local("pwd")
        local("echo 'sudo rm -f " + mvn_tgz_file + "'")
        local("sudo rm -f " + mvn_tgz_file)

@task
def install_devenv():
    '''
    Install the development environment
    '''
    print "Installing the development environment"
    install_maven3()
