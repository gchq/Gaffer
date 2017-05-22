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

Road Use Demo
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
mvn install -Pquick -Proad-traffic-demo -pl example/road-traffic/road-traffic-demo
```

If you wish to build all of Gaffer first then just remove the "-pl example/road-traffic/road-traffic-demo" part.

The rest api will be deployed to localhost:8080/rest and the ui will be deployed to localhost:8080/ui.

The sample data used is taken from the Department for Transport [GB Road Traffic Counts](http://data.dft.gov.uk/gb-traffic-matrix/Raw_count_data_major_roads.zip), which is licensed under the [Open Government Licence](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

This data contains information about UK roads, their locations and hourly traffic flow between adjacent road junctions.

## Walkthrough

We've modelled the road use data as a simple Gaffer graph to demonstrate how Gaffer lets users explore and summarise data.

There are edges representing:

 - Region to Location: e.g South West - Bristol, South West - Devon etc.
 - Location to Roads: e.g. Bristol - M32 etc
 - Roads and their Junctions: e.g. M32 - M32:1, M32 - M32:2, etc.
 - Junctions and their locations: e.g. M32:2 - 361150,175250, etc.
 - Traffic counts between junctions during specific hours: e.g. M32:1 - M32:2 etc.

We can start with a uk region, such as the South West, and find the locations within that region. Then pick one or more of those locations, find the roads there and list their junctions. Then between any pair of adjacent junctions, we can summarise the vehicle counts over a time range of our choosing. 

There will be multiple edges representing the traffic counts between the same two junctions: one for each hour of observation recorded in the data. Each of the RoadUse edges has properties attached to it representing the start of the hour during which the traffic was counted, the end of the hour, the total vehicle count for the hour and a map of vehicle type to count for the hour.

For example:

```json
 {
    "group": "RoadUse",
    "source": "M32:1",
    "destination": "M32:M4 (19)",
    "directed": true,
    "class": "uk.gov.gchq.gaffer.data.element.Edge",
    "properties": {
      "countByVehicleType": {
        "uk.gov.gchq.gaffer.types.simple.FreqMap": {
          "HGVR3": 2004,
          "BUS": 1375,
          "HGVR4": 1803,
          "AMV": 407034,
          "HGVR2": 11369,
          "HGVA3": 1277,
          "PC": 1,
          "HGVA5": 5964,
          "HGVA6": 4817,
          "CAR": 320028,
          "HGV": 27234,
          "WMV2": 3085,
          "LGV": 55312
        }
      },
      "startDate": {
        "java.util.Date": 1034316000000
      },
      "endDate": {
        "java.util.Date": 1431540000000
      },
      "count": {
        "java.lang.Long": 841303
      }
    }
  }
```

Gaffer allows us to query the set of RoadUse edges between two junctions across a time range that we choose. It will return a single RoadUse edge representing the sum of the vehicle counts over the time period we have queried for.   

The following steps will take you through a simple exploration and summarisation of the road use data.

##### Navigate to the UI (the zoom is a bit temperamental in Safari):
```
localhost:8080/ui
```

##### Add a seed as the starting point for your query:
- click 'Add Seed'
- vertex: ```region```
- value: ```South West```
- click 'Add'


##### Build and execute a query to find all locations within the South West region:
- click 'Build query'
- click on the 'South West' vertex (displayed as a grey circle in the top left hand corner of the page) 
- click 'Next'
- select 'RegionContainsLocation'
- click 'Next'
- click 'Next'
- click 'Execute'

The South West vertex will still be selected - click on an empty part of the graph to deselect it.
Move the graph around by clicking and dragging the cursor.
Scroll to zoom in/out.

##### Build and execute a query to find all roads within Bristol:
- click 'Build query'
- click on the 'Bristol, City of' vertex
- click 'Next'
- select 'LocationContainsRoad'
- click 'Next'
- click 'Next'
- click 'Execute'


#### Build and execute a query to find all junctions on the M32:
- click 'Build query'
- click on the 'M32' vertex
- click 'Next'
- select 'RoadHasJunction'
- click 'Next'
- click 'Next'
- click 'Execute'


#### Build and execute a query to find the road use between junctions M32:1 and M32:M4 between 6AM and 7AM on 5/3/2005:
- click 'Build query'
- click on the 'M32:1' vertex
- click 'Next'
- select 'RoadUse'
- click 'Next'
- This time we are going to add a filter to the start and end times
- click 'Add filter'
- Enter the following startDate filter:
```
property: startDate
function: uk.gov.gchq.koryphe.impl.predicate.IsMoreThan
orEqualTo: true
value: {"java.util.Date": 1115100000000}
```
- click 'Add filter'
- Enter the following endDate filter:
```
property: endDate
function: uk.gov.gchq.koryphe.impl.predicate.IsLessThan
orEqualTo: true
value: {"java.util.Date": 1115103600000}
```
- click 'Next'
- click 'Execute'

If you find the 'RoadUse' edge in the graph and click on it, you will see the following information in the pop-up:

```
M32:1 to M32:M4 (19)
RoadUse	
countByVehicleType: {"uk.gov.gchq.gaffer.types.FreqMap":{"HGVR3":44,"BUS":10,"HGVR4":28,"AMV":6993,"HGVR2":184,"HGVA3":19,"PC":0,"HGVA5":99,"HGVA6":40,"CAR":5480,"HGV":414,"WMV2":44,"LGV":1045}}

startDate: {"java.util.Date":1115100000000}

endDate: {"java.util.Date":1115103600000}

count: {"java.lang.Long":14400}
```

This shows the vehicle types and their counts between these two junctions for the time period described by the filters.

Alternatively, if you click the 'Table' tab at the top of the UI you will see a table with 'Entity' and 'Edge' tabs.

Click the 'Edge' tab and you will see a table listing all of the edges displayed in the Graph based on the queries run so far.

Clicking the 'Raw' tab at the top of the UI displays the Json constructed and handed to Gaffer to run the queries.


#### Now we will repeat the previous query but with a bigger time window - this time between 6AM and 8AM on 5/3/2005:
- click 'Build query'
- click on the 'M32:1' vertex
- click 'Next'
- select 'RoadUse'
- click 'Next'
- This time we are going to add a filter to the start and end times
- click 'Add filter'
- Enter the following startDate filter:
```
property: startDate
function: uk.gov.gchq.koryphe.impl.predicate.IsMoreThan
orEqualTo: true
value: {"java.util.Date": 1115100000000}
```
- click 'Add filter'
- Enter the following endDate filter:
```
property: endDate
function: uk.gov.gchq.koryphe.impl.predicate.IsLessThan
orEqualTo: true
value: {"java.util.Date": 1115107200000}
```
- click 'Next'
- click 'Execute'

Now if you click on the 'RoadUse' edge, or visit the 'Edges' Table view, you'll see that two 'RoadUse' summaries are displayed:

```
M32:1 to M32:M4 (19)
RoadUse	
countByVehicleType: {"uk.gov.gchq.gaffer.types.simple.FreqMap":{"HGVR3":44,"BUS":10,"HGVR4":28,"AMV":6993,"HGVR2":184,"HGVA3":19,"PC":0,"HGVA5":99,"HGVA6":40,"CAR":5480,"HGV":414,"WMV2":44,"LGV":1045}}

startDate: {"java.util.Date":1115100000000}

endDate: {"java.util.Date":1115103600000}

count: {"java.lang.Long":14400}

RoadUse	
countByVehicleType: {"uk.gov.gchq.gaffer.types.simple.FreqMap":{"HGVR3":68,"BUS":28,"HGVR4":50,"AMV":13640,"HGVR2":370,"HGVA3":35,"PC":0,"HGVA5":204,"HGVA6":96,"CAR":10924,"HGV":823,"WMV2":95,"LGV":1770}}

startDate: {"java.util.Date":1115100000000}

endDate: {"java.util.Date":1115107200000}

count: {"java.lang.Long":28103}
```
The top one is from the first query.

The next is a summary over the two hours we specified in our second query. (You can verify this by querying again for second hour separately and then adding counts from the two single-hour summaries together).

In this example we have summarised the vehicle counts by adding them together. Gaffer users are free to define the objects that represent the properties on an edge and the functions used to summarise them and so supports things that are much more complex than adding integers together.

There are some in-depth examples based around the Java API here: [Getting Started](https://github.com/gchq/Gaffer/wiki/Getting-Started).
