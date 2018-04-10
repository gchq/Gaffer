Copyright 2017-2018 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Time Library
============

This library contains classes that represent concepts relating to time. For example, there is a class (RBMBackedTimestampSet) that can be used to represent a set of timestamps. Internally this stores its state in a Roaring Bitmap for efficiency reasons. There is also a class that stores up to a maximum number N of timestamps. If more than N timestamps are added then a uniform random sample of the timestamps, of size at most N, is stored. Serialisers and aggregators for the above classes are provided.

To use this library, you will need to include the following dependency:

```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>time-library</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```
