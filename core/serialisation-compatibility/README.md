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

Serialisation Compatibility
======

There is a version compatibility issue with Hadoop and Spark.

Hadoop depends on Jackson 2.3.1 and Spark depends on 2.6.5.

The Gaffer libraries compile against Jackson 2.6.5 (as 2.3.1 is a bit old), however
 this compatibility module ensures the code will also run against Jackson 2.3.1.