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

# Map-store

TODO - Finish README
TODO - Move logic from MapStore class into an implementation class and into the handlers
TODO - More testing of aggregation logic
TODO - Need to duplicate properties before returning them
TODO - Create factory so that HashMap, ConcurrentHashMap or MapDB can be used

The Map-store is a simple in-memory store. Any class that implements Java's Map interface can be used to store the data. Data stored in this store is not persistent, i.e. when the JVM is shut down the data will disappear.

It is designed to support aggregation of properties efficiently. Optionally

Some examples of how this can be used are:

- As an in-memory cache of some data that has been retrieved from a larger store.
- The aggregation of graph data within a streaming process.
- To demonstrate some of Gaffer's APIs.
