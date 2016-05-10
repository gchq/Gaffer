##  Creating Gaffer Dev Docker Image and Container

### Pre-requisites

1. Familiarity with the Gaffer framework

2. The existence of a directory called  /gaffer_docker/centos6 on the docker host.
   (Create the directory if it does not already exist)

The following tar files must be copied to the gaffer_docker/centos6 directory on the docker host:

3. The files should initially be copied to the /tmp directory on the docker host

> _gaffer-docker-gaffer.tgz_

> _gaffer-docker-base.tgz_

4. The contents of the *.tgz files should be copied to the /tmp directory on the docker host
   initially and then extracted to the /gaffer_docker/centos6 directory.

   Commands similar to the ones below can be used to do this

  ```$ cp /tmp/gaffer-docker-base.tgz /gaffer_docker/centos6/.```

  ```$ cd /gaffer_docker/centos6 && tar xvfz  gaffer-docker-base.tgz```

 ```$ cp /tmp/gaffer-docker-gaffer.tgz /gaffer_docker/centos6/.```

 ```$ cd /gaffer_docker/centos6 && tar xvf  gaffer-docker-gaffer.tgz```

The following commands will create a gaffer data container that retrieves a specified Gaffer branch from
github , so developers can amend and explore the gaffer source code.

##  Building Gaffer Dev docker image

1. Run the following command on the docker host

```$ cd /gaffer-docker/centos6```

```$ docker build --build-arg repo_branch=<git branch> --rm -t gaffer-docker/centos6:gafferdevel -f Dockerfile.centos6.gaffer.devel .```






