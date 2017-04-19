# Welcome to the gaffer-docker wiki!

## Background 
Gaffer can uses a NOSQL database to store the data it uses to produce graphs. Docker is used to package Gaffer and it's dependencies into a standardised unit for ease of use and to enhance the Gaffer user experience.

## Approach used to Dockerise Gaffer

Gaffer is packaged using docker by adding dependencies using a hierarchy of docker images. This reduces the size of the docker image and adheres to guides lines on the Docker GitHub for creating Docker images.

A base image  is created initially consisting of a base OS with the additional packages required for the installation of Gaffer, Hadoop and Accumulo. 

The gaffer data volume is based on the base image and is used to create the Gaffer data volume container

The hadoop image is based on the base image and is used to create a hadoop container.


The accumulo image is based on the hadoop image and is used to create an Accumulo container. The Accumulo container accesses the Gaffer data volume container (see section  Building and Deploying Gaffer Docker: [[Building-and-Deploying-Gaffer-Container]] for further details)

## Structure

| Image Name                      | Usage                                                                       |
|---------------------------------|-----------------------------------------------------------------------------|
| gaffer-docker/centos6:accumulo  |( Dockerfile is used to create an Accumulo (1.6.4), zookeeper (3.3.5) on     
|                                 |Apache Hadoop 2.6.0 image. Apache Hadoop 2.6.0 is inherited from             
|                                 | the gaffer-docker/centos6:hadoop image . The image is used to create an      
|                                 |Accumulo docker container ).                                                  
|gaffer-docker/centos6:hadoop     |( Dockerfile is used to create the an Apache Hadoop 2.6.0 image. The linux   
|                                 |dependencies packages required for the installation of Apache Hadoop 2.6.0   
|                                 |and firefox are inherited from the gaffer-docker/centos6:base image ). The   
|                                 |image is used to create a Hadoop docker container ).                                                      
|gaffer-docker/centos6:gaffer     |( Dockerfile is used to create a data volume image containing                
|                                 |the GAFFER jar files and example jar files used to test gaffer.             
|                                 |The linux dependencies required for the generation of the jar                
|                                 |files from the Gaffer source code is inherited from the                      
|                                 |gaffer-docker/centos6:base image.  The image is used to create the gaffer    
|                                 |data volume container )                                                                                   
|gaffer-docker/centos6:base       |( Image is used to create a container with the base centos6.7 image, obtained
|                                 |from saved copy of official Docker centos 6 image )