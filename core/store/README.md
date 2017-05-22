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


Store
=============

This Store module defines the API for Store implementations. The abstract Store class handles Operations by delegating the Operations to their registered handlers.

Store implementations need to define a set of StoreTraits. These traits tells Gaffer the abilities the Store has. For example the ability to aggregate or filter elements.

When implementing a Store, the main task is to write handlers for the operations your Store chooses to support. This can be tricky, but there is a Store Integration test suite that should be used by all Store implementations to validate these operation handlers. When writing these handlers you should implement OperationHandler or OutputOperationHandler depending on whether the operation has an output.

In addition to OperationHandlers the other large part of this module is the Schema. The Schema is what defines what is in the Graph and how it should be persisted, compacted/summarised and validated.