# Spark Development Environment for XUbuntu 14.10
This environment is composed on the following elements:
 * the main development will be performed in a VirtualBox VM running XUbuntu 14.10
    - the local versions of some services, like HBase and Kafka, will run in XUbuntu, as these services are difficul to run in windows
    - due to problems communicating the Windows host with the guest XUbuntu, finally development with Eclipse and Anaconda Spyder will also be performed in the VM. Anyway a Linux environment is more confortable for developing

 * a EMR cluster launched by some Fabric scripts from the XUbuntu VM will be used to run the jobs in a cluster, for the final acceptance and performance tests

## Components and versions:
    * Spark 1.3
    * Scala 2.10: required by Spark
    * Python 2.7: required by Spark
    * Apache Kafka kafka_2.10_0.8.1.1: required by Spark
 
## Network Setup for VirtualBox
First of all install the ssh server (package openssh-server) in the VM. Then following http://stackoverflow.com/questions/5906441/how-to-ssh-to-a-virtualbox-guest-externally-through-a-host I take the option of using the NAT network configuration for VirtualBox, and then in Configuration -> Networking -> Port forwarding we add a new rule with protocol = TCP, host port = 3022 and guess port = 22. This forwards petitions to host:3022 to the VM:22. Hence we can connect to the VM by ssh from the Windows host with:

```bash
C:\Users\bc24u_000\Sistemas>ssh -p 3022 juanrh@localhost
The authenticity of host '[localhost]:3022 ([127.0.0.1]:3022)' can't be established.
RSA key fingerprint is 9e:f2:af:6b:75:68:18:0b:93:ab:fd:c2:24:cb:e1:d8.
Are you sure you want to continue connecting (yes/no)? yes
Warning: Permanently added '[localhost]:3022' (RSA) to the list of known hosts.
juanrh@localhost's password:
Welcome to Ubuntu 14.10 (GNU/Linux 3.16.0-33-generic x86_64)
```

This is enough as most of the time we'll use the VM directly, and ssh is enough for moving data from the host to the guest, and viceversa

## Scripts for the development environment
Fabric is used for automation
 * Run `install_prerequisites.sh` to setup Fabric
 * Run `install_devenv.sh` to install the development environment

 For now all the Fabric tasks are defined in the single file `fabfile.py`

## Scala Environment for Spark
### Version of Scala
Spark is compiled for Scala 2.10, but 1) Scala IDE 4 is distributed with Scala 2.11, and 2) Scala IDE can only
have a single version of Scala installed (http://scala-ide.org/docs/current-user-doc/faq/index.html "Currently,
it is not possible to install more than one Scala IDE plugin within the same Eclipse installation"), and 3)
we cannot run a binary for Scala 2.10 in the Scala 2.11 runtime (https://groups.google.com/forum/#!topic/scala-user/mUZSYgCLigA).
Hence we should use Scala IDE 3.0.3 (http://scala-ide.org/download/prev-stable.html careful, Scala IDE 3.0.4 is
for Scala 2.11) which is the last version of the plugin for Scala 2.10. This works for Eclipse Kepler

### Configure Scala IDE
- Import project as a Maven project
- FIX for Scala IDE 4: this is not needed for Scala IDE 3.0.3, but for Scala IDE 4 we cannot execute the
code because the runtime is Scala 2.11. In project properties -> Scala Compiler select "use project settings" and "Scala Installation" to
"Fixed Scala Installation: 2.10.4 (built-in)" 

----

TODO below 

## Installing Zookeeper in local mode
See C:\Users\bc24u_000\Dropbox\Programacion\BigData\Zookeeper\zookeeper_local_install.txt

In http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.1.2-Win/bk_releasenotes_HDP-Win/content/ch_relnotes-hdp-2.1.1-product.html we can see HDP 2.1 uses zookeeper 3.4.5. I have only managed to find zookeeper 3.4-6 at http://zookeeper.apache.org/releases.html#download, marked as the stable version of zookeeper. Now we'll use the quickstart guide at http://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html#sc_InstallingSingleMode

* Download and configure Zookeeper: from `~/Sistemas`
```bash
wget http://ftp.cixug.es/apache/zookeeper/stable/zookeeper-3.4.6.tar.gz
tar xzf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6/
# Simple zookeeper config
echo 'tickTime=2000' >> conf/zoo.cfg
echo 'dataDir=/var/lib/zookeeper' >> conf/zoo.cfg
echo 'clientPort=2181' >> conf/zoo.cfg
sudo mkdir -p /var/lib/zookeeper
sudo chmod -R 777 /var/lib/zookeeper
bin/zkServer.sh start
```

Now test the installation with

```bash
bin/zkCli.sh
ls /
```
 

## Installing Kafka in local mode
TODO: update instructions for kafka_2.10_0.8.1.1

We download kafka_2.10-0.8.2.1.tgz from https://kafka.apache.org both in windows and in the virtual machine. Then following https://kafka.apache.org/documentation.html#quickstart

    1. From the VM, start the zookeeper server included with kafka, and the kafka server

```bash
cd ~/Sistemas/kafka_2.10-0.8.2.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```
and in another terminal

```bash
cd ~/Sistemas/kafka_2.10-0.8.2.1
bin/kafka-server-start.sh config/server.properties
```

See Dropbox\Programacion\BigData\fabric_clusters for a init.d script for Kafka

    2. In windows, for testing the connection from windows we need 1) a windows console (e.g. the github bash shell won't work, but Console2 with MinGW is ok); 2) using the bat scripts in the bin/windows directory of the kafka installation, instead of the scripts in bin. So from C:\Users\bc24u_000\Sistemas\Kafka\kafka_2.10-0.8.2.1>

```bat
set VM_HOST=192.168.0.203
bin\windows\kafka-topics.bat --list --zookeeper %VM_HOST%:2181
bin\windows\kafka-topics.bat --create --zookeeper %VM_HOST%:2181 --replication-factor 1 --partitions 1 --topic test
bin\windows\kafka-topics.bat --list --zookeeper %VM_HOST%:2181
```
   
    3. Open a console consumer in the VM, and a console producer in Windows, and send some messages:

    In the VM:
```bash
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
```
   
    In Windows:
```bat
set VM_HOST=192.168.0.203
bin\windows\kafka-console-producer.bat --broker-list %VM_HOST%:9092 --topic test
```

The windows part fails. It might be just for the scripts, we'll see soon if there is some problem to access Kafka from Spark. If we launch a consumer and producer from the VM then everything works ok

```bash
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

**NOTE**: the first time the console producer connects to a topic, if the topic was not created previously some exception related to "fetching metadata from the leader" might be raised. When using the Scala / Java API, topics are created automatically when the first message is sent

With this setting local Spark from windows is not able to connect to Kafka. Also tried with NAT configuration in VirtualBox with port forwarding for ports 9092 and 2181 and it doesn't work either ==> LEAVING THIS APPROACH, and switching to local devel in XUbuntu

### Deleting a Kafka topic
Deleting a topic is not recommended, this is more for development purposes, in production the idea is letting the TTL for the topic arrive to Kafka.
For Kafka 0.8.1.1 we can use http://stackoverflow.com/questions/24287900/delete-topic-in-kafka-0-8-1-1 we can do it:

 * From the command line utils that come with Kafka:
```bash
bin/kafka-run-class.sh kafka.admin.DeleteTopicCommand --zookeeper localhost:2181 --topic test
```
 
 * With the Java / Scala API
```java
ZkClient zkClient = new ZkClient("localhost:2181", 10000);
zkClient.deleteRecursive(ZkUtils.getTopicPath("test2"));
```
 
### Purging a Kafka Installation
For development, as seen in http://stackoverflow.com/questions/16284399/purge-kafka-queue we can:
 * Stop the Apache Kafka daemon
 * Delete the topic data folder: rm -rf /tmp/kafka-logs/MyTopic-0
 * Delete the topic metadata: zkCli.sh then rmr /brokers/MyTopic
 * Start the Apache Kafka daemon

