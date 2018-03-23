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


Bitmap Library
============
This module contains various libraries for Bitmaps.

In order to make use of the bitmap libraries you will need to include this library as a dependency:
```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>bitmap-library</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```

You will then need to register the BitmapJsonModules using the store or system
property: gaffer.serialiser.json.modules. This property takes a CSV of classes
so you can use multiple json modules.
```
gaffer.serialiser.json.modules=uk.gov.gchq.gaffer.bitmap.serialisation.json.BitmapJsonModules
```
