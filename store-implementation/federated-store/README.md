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


Federated Store
============

This store is experimental, the API is unstable and may require breaking changes.

The `FederatedStore` is simply a Gaffer store which forwards operations to a
collection of sub-graphs and returns a single response as though it was a single graph.

Please see [FederatedStore Walkthrough](https://gchq.github.io/gaffer-doc/getting-started/dev-guide.html#federatedstore) for more details on how to set up and use this store.