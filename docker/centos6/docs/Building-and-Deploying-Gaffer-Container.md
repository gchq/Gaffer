# Quick start guide
1. Ensure docker is running, as root run:

`
service docker start
`

2. Change to centos6 folder: Gaffer/docker/centos6

3. Run setup script as root:

`
./setup.sh
`

4. Run buildAndRun script as your user (note this will log you into the accumulo container):

`
./buildAndRun.sh
`

5. As hduser in the accumulo container run the start accumulo script:

`
~/start-accumulo.sh
`

6. To disconnect from accumulo container:

`
Ctrl-p  Ctrl-q
`

7. To reattach to accumulo container run the attach script as your user:

`
./attach.sh
`

8. To restart accumulo, run the stop and start scripts as your user:

`
./stop.sh
./start.sh
`

# Detailed guide

## Pre-requisites

1. The installation of Docker on the host - access to the internet is required for this step 
2. Docker requires a 64-bit installation regardless of the CentOS version. The kernel must be 3.10 at minimum,
   which CentOS 7 runs. Installation needs to be run as root.

   Instructions are available on the Docker GitHub  
   (see link [DockerGitHub](https://docs.docker.com/engine/installation/linux/centos/) )

3. X11 package on host machine

4. Deployment of the Gaffer Dockerfile tar files if gaffer-docker is not available via GitHub
   ( gaffer-docker-base.tgz, gaffer-docker-gaffer.tgz,  gaffer-docker-hadoop.tgz gaffer-docker-accumulo.tgz )

5. Swappiness needs to be set to 0 and IPV6 needs to be disabled on the Docker Host/Client

# Deployment of Gaffer Dockerfile tar files
 This step is only required if gaffer-docker is not available on GitHub

 * Create a directory on the docker host machine called gaffer-docker

   ```$ mkdir /gaffer-docker```

   ```$ chown <name of user with privileges to run docker>:< user group> /gaffer-docker```

   ```$ mkdir /gaffer-docker/centos6```

   ```$ chown <name of user with privileges to run docker>:< user group> /gaffer-docker/centos6```
   
6. Copy the tar files to /gaffer-docker/centos6 and extract files to this directory
   
   ```$ chown <name of user with privileges to run docker>:< user group> *.tgz```

   ```$ tar xvzf  gaffer-docker-base.tgz```

   ```$ tar xvzf  gaffer-docker-gaffer.tgz```

   ```$ tar xvzf  gaffer-docker-hadoop.tgz```

   ```$ tar xvzf  gaffer-docker-accumulo.tgz```
  
 * The directory structure below should be created on the server
 
   gaffer-docker
                base
                gaffer
                hadoop
                accumulo
  
## Building Gaffer Docker Containers 
   
   
7. Create the base image
   
   ```$ cd /gaffer-docker/centos6/base```

   ```$ docker build --rm -t gaffer-docker/centos6:base -f Dockerfile.centos6 .```

   To test run the command below:
   
   ```$ docker run -it --name gaffer-base gaffer-docker/centos6:base```
   
   Stop the container using the command below:
   
  ```$ docker stop gaffer-base```

8. Create the gaffer data volume image
   
   ```$ cd /gaffer-docker/centos6/gaffer```

   ```$ docker build --rm -t gaffer-docker/centos6:gaffer -f Dockerfile.centos6.gaffer .```

   To test run the command below:
   
  ```$ docker run -it --name gaffer-data-volume gaffer-docker/centos6:gaffer```
   
   Stop the container using the command below:
   
   ```$ docker stop gaffer-data-volume```
   
9. Create the hadoop image
   
   ```$ cd /gaffer-docker/centos6/hadoop```

   ```$ docker build --rm -t gaffer-docker/centos6:hadoop -f Dockerfile.hadoop.centos6 .```

   To test run the command below:
   
   ```$ docker run -it -h localhost -u hduser --name gaffer-hadoop -e DISPLAY=$DISPLAY \```

   ```-v /tmp/.X11-unix:/tmp/.X11-unix gaffer-docker/centos6:hadoop```

   | Item                            | Description                                | Value  |
   |---------------------------------|--------------------------------------------|--------|
   | Hadoop  Process User            | User used to run hadoop processes          | hduser |
   | Hadoop Process User Password    | Password                                   | admin  |
   
   Start hadoop processes
   
   ```$ /home/hduser/start-hadoop.sh```

   Check status of hadoop using the webUI
   
   ```$ firefox &```

   Enter the URL ```localhost:50070```in the browser

   Stop hadoop processes

   ```$ /home/hduser/stop-hadoop.sh```
   
   Stop the container using the command below:
   
   ```$ docker stop gaffer-hadoop```
   
10. Create the Accumulo image
   
   **Pre-requisites**
   
   swappiness and IPV6 needs to be disabled on the Docker client/host

   Start the gaffer-data-volume container in detahced mode by running the commands below

   ```$ docker rm gaffer-data-volume```

   ```$ docker run -it -d --name gaffer-data-volume gaffer-docker/centos6:gaffer```

   ```$ cd /gaffer-docker/centos6/accumulo```

   ```$ docker build --rm -t gaffer-docker/centos6:accumulo  \ ```

   ```-f Dockerfile.accumulo.centos6 .```

   To test run the command below:
   
   ```$ docker run -it -h localhost -u hduser --name gaffer-accumulo  --volumes-from gaffer-data-volume \```

   ```-e DISPLAY=$DISPLAY \```

  ```-v /tmp/.X11-unix:/tmp/.X11-unix gaffer-docker/centos6:accumulo```

   | Item                            | Description                                | Value  |
   |---------------------------------|--------------------------------------------|--------|
   | Accumulo Process User           | User used to run accumulo/hadoop processes | hduser |
   | Accumulo Process User Password  | Password                                   | admin  |
   | Root                            | Privileged user                            | admin  |
   | Accumulo Instance Name          | Name of accumulo instance                  | Gaffer |
   | Accumulo Instance User          | Name of user used to initialise instance   | root   |
   | Accumulo Instance User password | Password for accumulo instance user        | admin  |

## Start accumulo services
  
  
11. Connect to the gaffer-accumulo container as the hduser and run the command below:
  
    ```$ /home/hduser/start-accumulo.sh```
  
    Test Accumulo services have started
  
    ```$ firefox &```
  
    Enter the following URL in the browser to display the Accumulo monitor web UI
  
    ```localhost:50095```
  
  
 ##  Test Gaffer 
 Run the following script to copy the gaffer example jar files to the accumulo lib directory
  
  ```$ /home/hduser/gaffer2-setup.sh```

  Output similar to below will be displayed
  > accumulo-store-iterators-0.3.3.jar
  > common-util-0.3.3.jar
  > data-0.3.3.jar
  > example-0.3.3.jar
  > function-0.3.3.jar
  > graph-0.3.3.jar
  > jackson-annotations-2.6.2.jar
  > jackson-core-2.6.2.jar
  > jackson-databind-2.6.2.jar
  > simple-operation-library-0.3.3.jar
  > operation-0.3.3.jar
  > serialisation-0.3.3.jar
  > simple-function-library-0.3.3.jar
  > simple-serialisation-library-0.3.3.jar
  > store-0.3.3.jar
  > Gaffer setup completed

 Create user and table with relevant authorizations to run the film LoadAndQuery example

  ```$ /opt/accumulo/bin/accumulo shell -u root -p admin```

  ```$ root@Gaffer> createuser user01```

  ```$ Enter new password for 'user01': password```

  ```$ Please confirm new password for 'user01': password```

  ```$ root@Gaffer> grant -s System.CREATE_TABLE -u user01```

  ```$ root@Gaffer> user user01```

  ```$ Enter password for user user01: password```

  ```$ user01@Gaffer> createtable table1```

  ```$ user01@Gaffer table1> user root```

  ```$ Enter password for user root: admin```

  ```$ root@Gaffer table1> setauths -s U,PG,_12A,_15,_18 -u user01```

  ```$ root@Gaffer table1> exit```

  Run LoadAndQuery example

  ```$ /opt/accumulo/bin/accumulo gaffer.example.films.analytic.LoadAndQuery```
  
  Output similar to below should be displayed:

> [hduser@localhost : /opt/accumulo/bin]$ ./accumulo gaffer.example.films.analytic.LoadAndQuery
> 2016-05-18 12:20:07,756 [client.ClientConfiguration] WARN : Found no client.conf in default paths. Using default client configuration values.
> 2016-05-18 12:20:08,994 [analytic.LoadAndQuery] INFO : Results from query:
> Entity{vertex=filmA, group='review', properties={starRating=<java.lang.Float>2.5, count=<java.lang.Integer>2, userId=<java.lang.String>user01,user03, rating=<java.lang.Long>100}} 

  ## Stop Accumulo Services
 
  ```$ /home/hduser/stop-accumulo.sh```
  
  
  ## Stop the Accumulo container
  
 ```$ docker stop gaffer-accumulo```
