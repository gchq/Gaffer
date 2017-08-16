Copyright 2017 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Flink Library
============
This module contains various libraries for Apache Flink.

In order to make use of the flink libraries you will need to include this library as a dependency:
```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>flink-library</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```

You will then need to register the flink operations and their handlers with your store.
You just need to add the following to your store properties file.
```
gaffer.store.operation.declarations=FlinkOperationDeclarations.json
```


## FAQs
Here are some frequently asked questions.

#### I am getting errors when running the Flink operations on a cluster
This could be to do with the way the Gaffer Store class is serialised and distributed
around the cluster. To distribute the job, Flink requires all of the components of
the job to be Serializable. The Gaffer Store class is not Serializable so instead
we just Serialialize the graphId, Schema and Properties. Then when we require an
instance of the Store class again we recreate it again with these parts.
This means that any files that are referenced in your StoreProperties must be 
available on all your nodes in your cluster.
