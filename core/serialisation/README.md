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

This page has been copied from the Serialisation module README. To make any changes please update that README and this page will be automatically updated when the next release is done.


Serialisation
=============

A simple module containing the logic for converting an Object into serialised
objects (normally a byte array).

The main interface is Serialisation. We have provided a small set of serialisers
for commonly used objects. The serialisers we have been designed to optimise
speed and size. Serialisers can take arguments which may be mandatory depending on
the serialiser used.


It is important to choose your serialisers wisely as once your data is persisted
using a chosen serialiser, there is no easy way of migrating your data into
a different format.
