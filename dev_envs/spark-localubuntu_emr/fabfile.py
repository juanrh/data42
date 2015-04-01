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

def print_title(title):
    print title
    print '-' * len(title)

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
    print_title("Installing maven 3")
    _maven_root = os.path.join(_install_root, "maven3")
    create_public_dir(_maven_root)
    with lcd(_maven_root):
        mvn_tgz_file = download_and_uncompress(_maven3_url)
        mvn_dir = get_child_name_with_prefix("apache-maven-3")
        mvn_full_path = os.path.join(_maven_root, mvn_dir)
        append_to_bashrc("'export M2_HOME={mvn_full_path}'".format(mvn_full_path=mvn_full_path))
        append_to_bashrc("'export M2=$M2_HOME/bin'")
        append_to_bashrc("'PATH=$M2:$PATH'")
        local("sudo rm -f " + mvn_tgz_file)

_java_url = "http://download.oracle.com/otn-pub/java/jdk/7u75-b13/jdk-7u75-linux-x64.tar.gz"
@task
def install_java7():
    '''
    Installs the JDK for Java 7 from Oracle Corporation
    NOTE: Local execution
    NOTE: use the ideas of http://blog.kdecherf.com/2012/04/12/oracle-i-download-your-jdk-by-eating-magic-cookies/ for an automatic install
    '''
    print_title("Installing Oracle's JDK for Java 7")
    _java_root = os.path.join(_install_root, "java7")
    create_public_dir(_java_root)
    with lcd(_java_root):
        local('wget --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie;" ' + _java_url)
        java_tgz_file = os.path.basename(_java_url)
        local("tar xzf " + java_tgz_file)
        java_dir = get_child_name_with_prefix("jdk1.7")
        java_full_path = os.path.join(_java_root, java_dir)
        java_binary = os.path.join(java_full_path, "bin", "java")
        append_to_bashrc("'# Java 7 Oracle JDK'")
        append_to_bashrc("'export JAVA_ORACLE={java_full_path}'".format(java_full_path=java_full_path))
        append_to_bashrc("'export JAVA_HOME=$JAVA_ORACLE'")
        append_to_bashrc("'PATH=${JAVA_ORACLE}/bin:$PATH'")
        local("echo sudo update-alternatives --install /usr/bin/java java {java_binary} 1".format(java_binary=java_binary))
        local("echo sudo update-alternatives --set java {java_binary}".format(java_binary=java_binary))
        local("sudo rm -f " + java_tgz_file)
    local('java -version')

@task
def install_devenv():
    '''
    Install the development environment
    '''
    print "Installing the development environment"
    install_maven3()
    install_java7()
