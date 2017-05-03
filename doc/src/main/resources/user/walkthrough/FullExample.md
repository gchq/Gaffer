${HEADER}

${CODE_LINK}

Finally this example introduces the full Road Traffic schema. This uses the sample data taken from the Department for Transport [GB Road Traffic Counts](http://data.dft.gov.uk/gb-traffic-matrix/Raw_count_data_major_roads.zip), which is licensed under the [Open Government Licence](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

The data is now in a slightly different format. Each row now represents multiple vehicles of different types travelling between 2 junctions. We also have a bit of extra information in the data file. This has allow us to create some extra edges: RegionContainsLocation, LocationContainsRoad and JunctionLocatedAt.

As we now have multiple roads in our sample data, we will include the name of the road in the junction name, e.g. M5:23 represents junction 23 on the M5.

We have also add in a frequency map for the counts of each vehicle time. This will allow us to perform queries such as to find out which roads have a large number of buses. 
Here are the updated schema files:

##### Data Schema
${DATA_SCHEMA_JSON}


##### Data Types
${DATA_TYPES_JSON}


##### Store Types
${STORE_TYPES_JSON}


#### Example queries
Now we have a the full schema we can load in our ${ROAD_TRAFFIC_SAMPLE_DATA_LINK} data and run more complex queries.

##### Get the road use of M32 junction 1.
${GET_SNIPPET}

This can also be written in JSON for performing the query via the REST API.

```json
${GET_JSON}
```

When executed on the graph, the result is:

```
${RESULT}
```
