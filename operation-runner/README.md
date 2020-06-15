Copyright 2020 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

# Operation Runner

The Operation Runner is a command line tool allowing Operations to be executed against a graph:

```bash
java -jar operation-runner-${GAFFER_VERSION}-with-dependencies.jar \
    --store-properties <path-to-store-properties> \
    --schema <path-to-schema> \
    --operation-chain <path-to-json-serialised-operation> \
    --user <optional-path-to-json-serialised-user> \
    --graph-id graphId
```

## Running Operations against the Accumulo store

Use the `accumulo -add <jar>` command to configure the classpath with the Accumulo dependencies required for execution:
```bash
${ACCUMULO_HOME}/bin/accumulo -add operation-runner-${GAFFER_VERSION}-with-dependencies.jar \
    uk.gov.gchq.gaffer.operation.runner.OperationRunner \
    --store-properties <path-to-Accumulo-store-properties> \
    --schema <path-to-schema> \
    --operation-chain <path-to-json-serialised-AddElementsFromHdfs-operation> \
    --user <optional-path-to-json-serialised-user> \
    --graph-id graphId
```
## Running Operations containing Map Reduce stages

Operations which execute Map Reduce stages require additional configuration to ensure the Hadoop and Map Reduce classpaths are correctly configured.

### Accumulo store Map Reduce Operations

The following example shows how to run the AddElementsFromHdfs operation against an Accumulo store:
```bash
${ACCUMULO_HOME}/bin/tool.sh operation-runner-${GAFFER_VERSION}-with-dependencies.jar \
    uk.gov.gchq.gaffer.operation.runner.OperationRunner \
    --store-properties <path-to-Accumulo-store-properties> \
    --schema <path-to-schema> \
    --operation-chain <path-to-json-serialised-AddElementsFromHdfs-operation> \
    --user <optional-path-to-json-serialised-user> \
    --graph-id graphId
```
The Accumulo tool.sh script automates adding the additional Accumulo dependencies required to the classpath.
If your operation requires additional external dependencies these can be configured using the `-libjars` option and a comma separated String containing the paths to the external dependencies:
```bash
${ACCUMULO_HOME}/bin/tool.sh operation-runner-${GAFFER_VERSION}-with-dependencies.jar \
    uk.gov.gchq.gaffer.operation.runner.OperationRunner \
    -libjars <path-libjar1>,<path-libjar2>...,
    --store-properties <path-to-Accumulo-store-properties> \
    --schema <path-to-schema> \
    --operation-chain <path-to-json-serialised-AddElementsFromHdfs-operation> \
    --user <optional-path-to-json-serialised-user> \
    --graph-id graphId
```

### HBase store Map Reduce Operations

The following example shows how to run the AddElementsFromHdfs operation against a HBase store:
```bash
export HADOOP_CLASSPATH="$(${HBASE_HOME}/bin/hbase classpath)"
${HADOOP_HOME}/bin/hadoop jar operation-runner-${GAFFER_VERSION}-with-dependencies.jar \
    uk.gov.gchq.gaffer.operation.runner.OperationRunner \
    --store-properties <path-to-HBase-store-properties> \
    --schema <path-to-schema> \
    --operation-chain <path-to-json-serialised-AddElementsFromHdfs-operation> \
    --user <optional-path-to-json-serialised-user> \
    --graph-id graphId
```
If your operation requires additional external dependencies then append these to the HADOOP_CLASSPATH and use the `-libjars` operation as shown in the Accumulo example to configure the MR job.