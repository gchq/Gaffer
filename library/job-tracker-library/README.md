Copyright 2016-2017 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Job Tracker Library
===================
This module contains job tracker implementations.

By default the Job Tracker is disabled. To enable the job tracker you will need to add a dependency on the
job tracker implementation you want e.g. job-tracker-jcs:

```
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>job-tracker-jcs</artifactId>
    <version>${gaffer.version}</version>
</dependency>
```

You will then need to register the job tracker in your store.properties file.
```
gaffer.store.job.tracker.class=uk.gov.gchq.gaffer.jobtracker.JcsJobTracker
```

you can optionally provide a config file (e.g a cache.ccf file):
```
gaffer.store.job.tracker.config.path=/path/to/config/file
```