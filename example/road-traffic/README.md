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

This page has been copied from the Road Traffic module README. To make any changes please update that README and this page will be automatically updated when the next release is done.


Road Traffic Example
=============

## Deployment
Assuming you have Java 8, Maven and Git installed, you can build and run the latest version of the road traffic demo by doing the following:

```bash
# Clone the Gaffer repository, to reduce the amount you need to download this will only clone the master branch with a depth of 1 so there won't be any history.
git clone --depth 1 --branch master https://github.com/gchq/Gaffer.git
cd Gaffer

# This will download several maven dependencies such as tomcat.
# Using -pl we tell maven only to build the demo module and just download the other Gaffer binaries from maven.
# The -Proad-traffic-demo is a profile that will automatically startup a standalone instance of tomcat with the REST API and UI deployed.
mvn install -Pquick -Proad-traffic-demo -pl :road-traffic-demo
```

If you wish to build all of Gaffer first then just remove the "-pl :road-traffic-demo" part.

The rest api will be deployed to localhost:8080/rest.

The sample data used is taken from the Department for Transport [GB Road Traffic Counts](http://data.dft.gov.uk/gb-traffic-matrix/Raw_count_data_major_roads.zip), which is licensed under the [Open Government Licence](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

This data contains information about UK roads, their locations and hourly traffic flow between adjacent road junctions.

We've modelled the road use data as a simple Gaffer graph to demonstrate how Gaffer lets users explore and summarise data.

There are edges representing:

 - Region to Location: e.g South West - Bristol, South West - Devon etc.
 - Location to Roads: e.g. Bristol - M32 etc
 - Roads and their Junctions: e.g. M32 - M32:1, M32 - M32:2, etc.
 - Junctions and their locations: e.g. M32:2 - 361150,175250, etc.
 - Traffic counts between junctions during specific hours: e.g. M32:1 - M32:2 etc.

We can start with a UK region, such as the South West, and find the locations within that region. Then pick one or more of those locations, find the roads there and list their junctions. Then between any pair of adjacent junctions, we can summarise the vehicle counts over a time range of our choosing.

There will be multiple edges representing the traffic counts between the same two junctions: one for each hour of observation recorded in the data. Each of the RoadUse edges has properties attached to it representing the start of the hour during which the traffic was counted, the end of the hour, the total vehicle count for the hour and a map of vehicle type to count for the hour.

There are some in-depth examples based around the Java API here: [Getting Started](https://gchq.github.io/gaffer-doc/summaries/getting-started.html).
