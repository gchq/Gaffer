**Special mention to the developers at the Docker Github site**

# Pre-requisites

1. Access to the internet is required
2. Access to the root password or user with root privileges
3. A machine with Centos/Redhat & installed

## Install with the script
1. Log into your machine as a user with sudo or root privileges.

2. Make sure your existing yum packages are up-to-date.

```$ sudo yum update```
Run the Docker installation script.

```$ curl -fsSL https://get.docker.com/ | sh```
This script adds the docker.repo repository and installs Docker.

## Start the Docker daemon.

```$ sudo service docker start```
Verify docker is installed correctly by running a test image in a container.

```$ sudo docker run hello-world```

## Create a docker group
The docker daemon binds to a Unix socket instead of a TCP port. By default that Unix socket is owned by the user root and other users can access it with sudo. For this reason, docker daemon always runs as the root user.

To avoid having to use sudo when you use the docker command, create a Unix group called docker and add users to it. When the docker daemon starts, it makes the ownership of the Unix socket read/writable by the docker group.

Warning: The docker group is equivalent to the root user; For details on how this impacts security in your system, see Docker Daemon Attack Surface for details.

## To create the docker group and add your user:

1. Log into Centos as a user with sudo privileges.

2. Create the docker group and add your user.

```sudo usermod -aG docker your_username```

Log out and log back in.

This ensures your user is running with the correct permissions.

## Verify your work by running docker without sudo.

```$ docker run hello-world```
3. Start the docker daemon at boot
To ensure Docker starts when you boot your system, do the following:

```$ sudo chkconfig docker on```
If you need to add an HTTP Proxy, set a different directory or partition for the Docker runtime files, or make other customizations, read our Systemd article to learn how to customize your Systemd Docker daemon options.

## Uninstall
4. You can uninstall the Docker software with yum.

List the package you have installed.

```$ yum list installed | grep docker```
```yum list installed | grep docker```
```docker-engine.x86_64   1.7.1-1.el7 @/docker-engine-1.7.1-1.el7.x86_64.rpm```

5. Remove the package.

```$ sudo yum -y remove docker-engine.x86_64```
This command does not remove images, containers, volumes, or user-created configuration files on your host.

# Removing Images
6.To delete all images, containers, and volumes, run the following command:

```$ rm -rf /var/lib/docker```
Locate and delete any user-created configuration files.
