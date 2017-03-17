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

# accumulo-store

## Running integration tests

Update the following store properties files to point to the location of the Accumulo store to test against:
- [src/test/resources/store.properties](src/test/resources/store.properties)
- [src/test/resources/accumuloStoreClassicKeys.properties](src/test/resources/accumuloStoreClassicKeys.properties)

Ensure that one of the following classes is being used for the `gaffer.store.class` property:
- uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore
- uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore

Ensure that the Accumulo user specified by the `accumulo.user` property has the `System.CREATE_TABLE` permission and the following scan authorisations:

| Authorisation     | Required by |
| ----------------- | ----------- |
| vis1              | [VisibilityIT](../../integration-test/src/test/java/uk/gov/gchq/gaffer/integration/impl/VisibilityIT.java) |
| vis2              | [VisibilityIT](../../integration-test/src/test/java/uk/gov/gchq/gaffer/integration/impl/VisibilityIT.java) |
| publicVisibility  | [AccumuloAggregationIT](src/test/java/uk/gov/gchq/gaffer/accumulostore/integration/AccumuloAggregationIT.java) |
| privateVisibility | [AccumuloAggregationIT](src/test/java/uk/gov/gchq/gaffer/accumulostore/integration/AccumuloAggregationIT.java) |

Run the integration tests:

```
mvn verify
```

## Accumulo 1.8.0 Support

Gaffer can be compiled with support for Accumulo 1.8.0. Clear your Maven repository of any Gaffer artefacts and compile Gaffer with the Accumulo-1.8 profile:

```
mvn clean install -Paccumulo-1.8
```