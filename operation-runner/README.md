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

Operation Runner
================

The Operation Runner is a command line tool allowing Operations to be executed against a graph:

```bash
java -jar operation-runner-<version>-with-dependencies.jar \
    --store-properties <path-to-store-properties> \
    --schema <path-to-schema> \
    --operation-chain <path-to-json-serialised-operation> \
    --user <optional-path-to-json-serialised-user> \
    graphId
```

To add additional classpath entries:

```bash
java -cp operation-runner-<version>-with-dependencies.jar \
    uk.gov.gchq.gaffer.operation.runner.OperationRunner \
    --store-properties <path-to-store-properties> \
    --schema <path-to-schema> \
    --operation-chain <path-to-json-serialised-operation> \
    --user <optional-path-to-json-serialised-user> \
    graphId
```
