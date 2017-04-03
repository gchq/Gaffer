Copyright 2016 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Example REST
============
This module provides an example implementation of a Gaffer REST Interface.

By default it will use the Gaffer MockAccumulo store.

To use a different store type you can set this system property when running the maven build.

```
-Dstore.type=map
```

There are two options for building and then running it:

Option 1 - Deployable war file
==============================

If you wish to deploy the war file to a container of your choice, then use this option.

To build the war file along with all its dependencies then run the following command from the parent directory:
'mvn clean install -Pquick'

To deploy it to a server of your choice, take target/example-rest-[version].war and deploy as per the usual deployment process for your server.

In order for the application to function, it needs a number of system properties to be set up on the server:
e.g.
gaffer.schemas=${SOME PATH}/schema
gaffer.storeProperties=${SOME PATH}/accumulo/store.properties

The above example for 'gaffer.schemas' assumes that your schemas are in separate files, but if they were in a combined file, then you would just provide the path to one file.
The 'gaffer.storeProperties' property points to a file that specifies the connection details of your Gaffer Store implementation. In this case, the implementation is an in-memory MockAccumuloStore.

Either way, the relevant files need to exist in the specified path on the appropriate machine. Examples can be copied from src/main/resources/. These examples illustrate a basic schema and use the MockAccumuloStore as the Gaffer Store Implementation.
Alternatively, there are further examples in the 'examples' project, or you could create your own.

The above System Properties need to be configured on your server. An example of doing this in Tomcat would be to add the lines above to the end of ${CATALINA_HOME}/conf/catalina.properties and then to ensure that the files are resolvable via the configured paths.


Option 2 - Build using the standalone profile
=============================================

The application can be built and then run as a basic executable standalone war file from Maven. When run in this format, the default schemas represent the Film/Viewings example and the store used is a MockAccumuloStore.

To build it and its dependencies, use the following command from the parent directory:

'mvn clean install -Pquick -Pstandalone'
This uses the 'standalone' profile to run jetty with the example-rest project after it and its dependencies have been built.

This should launch an embedded jetty container, which can then be accessed via your browser pointing to the following url:
http://localhost:8080/rest/

If you need to change the start-up port or customise anything else jetty related, you can change the class example.rest.launcher.Main accordingly and rebuild.

As a default, there are a number of system properties that are configured in the pom.xml file. The most important of these are the locations of the folder containing the schema
files and the data store .properties file. As a default, these point to the same files that are to be found in src/main/resources. These can be changed and the project rebuilt using the previous maven command

```xml
<systemProperties>
  <systemProperty>
      <name>gaffer.rest-api.basePath</name>
      <value>rest/v1</value>
  </systemProperty>
  <systemProperty>
      <name>gaffer.schemas</name>
      <!-- this needs to point to your Gaffer schema files or folder-->
      <value>${project.build.outputDirectory}/example-schema</value>
  </systemProperty>
  <systemProperty>
      <name>gaffer.storeProperties</name>
       <!-- this needs to point your data store properties file-->
      <value>${project.build.outputDirectory}/accumulo/store.properties</value>
  </systemProperty>
</systemProperties>
```


