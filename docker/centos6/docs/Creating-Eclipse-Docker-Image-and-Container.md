##  Creating Eclipse Docker Image and Container

### Pre-requisites

1. Familiarity with the Gaffer framework

2. A pre-built  Gaffer Dev Docker Image 

The tar file must be copied to the gaffer_docker/centos6 directory on the docker host:

3. The file gaffer-docker-eclipse.tgz copied to the /tmp directory on the docker host


4. The tar file extracted to the /gaffer_docker/centos6 directory on the  docker host
   Commands similar to the ones below can be used to do this

```$ cp /tmp/gaffer-docker-eclipse.tgz /gaffer_docker/centos6/.```

```$ cd /gaffer_docker/centos6 && tar xvfz  gaffer-docker-eclipse.tgz```

## Build Eclipse Docker installer image with Gaffer

1. Run the following command on the docker host

```$ cd /gaffer-docker/centos6/eclipse```

```$ docker build --rm  -t gaffer-docker/centos6:eclipse_installer_server -f Dockerfile.centos6.eclipse .```

2. Test the container by running the following command

```$ docker run -it --name eclipse -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix gaffer-docker/centos6:eclipse_installer_server```

3. Install Eclipse
Ensure installation directory is set to /opt/eclipse
Ensure workspace is set to /home/hduser/workspace

## Save state of container
1. Open another terminal session to save the container state ( i.e with eclipse installed ) by using commands 
   similar to the one below

```$ docker ps```

```$ docker commit eclipse gaffer-docker/centos6:eclipse_server```

**Exit from the 'eclipse' container**

## Create a container from the 'gaffer-docker/centos6:eclipse_server' image

Run the commands below:

```$ docker run -it --name eclipse_gafferdev_server -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix gaffer-docker/centos6:eclipse_server```









