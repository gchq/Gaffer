# Integrating Eclipse and Gaffer using plugin

## Pre-requisites

1. A Gaffer Dev container must be running

2. The latest JEE version of eclipse must be installed
   Latest version is available from the eclipse download site
  [Eclipse Downloads](http://www.eclipse.org/downloads/download.php? file=/technology/epp/downloads/release/mars/2/eclipse-jee-mars-2-linux-gtk-x86_64.tar.gz)

3. Familiarity with using Docker Tooling plugin
See link below for a more detailed guid
[Docker Linux Tool Guide](https://wiki.eclipse.org/Linux_Tools_Project/Docker_Tooling/User_Guide)

## Configuring Eclipse
1. Add the Docker Linux Tool plugin to Eclipse. The tool is available from the link below:
[Eclipse Docker Tooling](http://download.eclipse.org/linuxtools/update-docker-1.2/) 



2. Restart Eclipse

## Setting up eclipse to interact with Docker

1. Select the 'Other' perspective and select the tools under the 'Docker folder to the Docker Tool  Explorer, Docker Container and Docker Images within Eclipse

2. Select 'Run --> External Tool Configuration --> Perspective'.

3. Select 'Run Docker Image' as the Application Type/Launcher.

4. Select 'Docker Tooling' as the mode.



3. Select 'Docker Tooling'


