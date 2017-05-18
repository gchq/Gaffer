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


Operations
============

This module contains the Operation interfaces and core Operation implementations.

It is assumed that all Gaffer Graphs will be able to handle these core Operations.

An Operation defines an operation to be processed on a graph. The Operation class
contains the configuration required to tell Gaffer how to carry out the Operation.
For example, the AddElements Operation contains the elements to be added. The
GetElements Operation contains the seeds to use to find elements in the Graph and
the filters to apply to the query. The Operation classes themselves should not contain
the logic to carry out the operation, just the configuration.

For each Operation the Gaffer Store will have an Operation Handler, where the 
 processing logic is contained. This means operations can be handled differently
 in each Store.

Operations can be chained together to form an OperationChain. When an OperationChain
is executed on a Gaffer Graph the output of one operation is passed to the input of the next.
An OperationChain Builder is provided to help with constructing a valid operation
chain - it ensure the output type of an operation matches the input type of the next.

## How to write an Operation

Operations should be written to be as generic as possible to allow them to be applied to different graph/stores.

Operations must be JSON serialisable in order to make REST API calls. I.e. there
is a public constructor and all the fields have getters and setters.

Operation implementations need to implement the Operation interface and any of 
extra interfaces they they wish to make use of. For example an Operation that
takes a single input value should implement the Input interface.

Here is a list of some of the common interfaces:
- uk.gov.gchq.gaffer.operation.io.Input
- uk.gov.gchq.gaffer.operation.io.Output
- uk.gov.gchq.gaffer.operation.io.InputOutput - Use this instead of Input and Output if your operation takes both input and output.
- uk.gov.gchq.gaffer.operation.io.MultiInput - Use this in addition if you operation takes multiple inputs. This will help with json  serialisation
- uk.gov.gchq.gaffer.operation.SeedMatching
- uk.gov.gchq.gaffer.operation.Validatable
- uk.gov.gchq.gaffer.operation.graph.OperationView
- uk.gov.gchq.gaffer.operation.graph.GraphFilters
- uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters
- uk.gov.gchq.gaffer.operation.Options

Each Operation implementation should have a corresponding unit test class
that extends the OperationTest class.

Implementations should override the close method and ensure all closeable fields are closed.

All implementations should also have a static inner Builder class that implements
the required builders. For example:

```java
public static class Builder extends Operation.BaseBuilder<GetElements, Builder>
        implements InputOutput.Builder<GetElements, Iterable<? extends ElementId>, CloseableIterable<? extends Element>, Builder>,
        MultiInput.Builder<GetElements, ElementId, Builder>,
        SeededGraphFilters.Builder<GetElements, Builder>,
        SeedMatching.Builder<GetElements, Builder>,
        Options.Builder<GetElements, Builder> {
    public Builder() {
            super(new GetElements());
    }
}
```