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

This page has been copied from the REST API module README. To make any changes please update that README and this page will be automatically updated when the next release is done.


REST API
==========
This module contains a Gaffer REST API.

The core REST API is within the core-rest module. 
Each Gaffer Store then has a module where it extends the core-rest and adds in the dependency for the Gaffer Store.

So if you want to use the Accumulo Store REST API, you can use the accumulo-rest war.

For an example of using the REST API please see the [example/road-traffic](https://gchq.github.io/gaffer-doc/components/example/road-traffic.html) module.

## How to modify the REST API for your project

If you wish to make changes or additions to the REST API for your project then this can be easily achieved.
You will need to create a new maven module to build your REST API.
In your pom you should configure the maven-dependency-plugin to download the core gaffer rest api war and extract it.
When maven builds your module it will unpack the core war, add your files and repackage the war.
If you wish to override a file in the core war then you can do this by including your own file with exactly the same name and path.

Example maven-dependency-plugin configuration:
```xml
<build>
    <sourceDirectory>src/main/java</sourceDirectory>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-dependency-plugin</artifactId>
            <version>2.10</version>
            <dependencies>
                <dependency>
                    <groupId>uk.gov.gchq.gaffer</groupId>
                    <artifactId>core-rest</artifactId> <!-- Or your chosen store, e.g 'accumulo-rest' -->
                    <version>${gaffer.version}</version>
                    <type>war</type>
                </dependency>
            </dependencies>
            <executions>
                <execution>
                    <id>unpack</id>
                    <phase>compile</phase>
                    <goals>
                        <goal>unpack</goal>
                    </goals>
                    <configuration>
                        <artifactItems>
                            <artifactItem>
                                <groupId>uk.gov.gchq.gaffer</groupId>
                                <artifactId>core-rest</artifactId> <!-- Or your chosen store, e.g 'accumulo-rest' -->
                                <version>${gaffer.version}</version>
                                <type>war</type>
                                <overWrite>false</overWrite>
                                <outputDirectory>
                                    ${project.build.directory}/${project.artifactId}-${project.version}
                                </outputDirectory>
                            </artifactItem>
                        </artifactItems>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

So, if you want to change the CSS for the rest api you can override the custom.css file:
```
<module>/src/main/webapp/css/custom.css
```

There are also various system properties you can use to configure to customise the Swagger UI.
For example:
```
gaffer.properties.app.title=Road Traffic Example
gaffer.properties.app.description=Example using road traffic data
gaffer.properties.app.banner.description=DEMO
gaffer.properties.app.banner.colour=#1b75bb
```