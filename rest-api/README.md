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


REST API
==========
This module contains a Gaffer REST API.

The core REST API is within the core-rest module. 
Each Gaffer Store then has a module where it extends the core-rest and adds in the dependency for the Gaffer Store.

So if you want to use the Accumulo Store REST API, just add the accumulo-rest war.

For an example of using the REST API please see the example/road-traffic module.