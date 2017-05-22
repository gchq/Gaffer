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


Operations
============

This module contains the `Operation` interfaces and core operation implementations.

It is assumed that all Gaffer graphs will be able to handle these core operations.

An `Operation` implementation defines an operation to be processed on a
graph, or on a set of results which are returned by another operation. An
`Operation` class contains the configuration required to tell Gaffer how
to carry out the operation. For example, the `AddElements` operation contains
the elements to be added. The `GetElements` operation contains the seeds
to use to find elements in the graph and the filters to apply to the query.
The operation classes themselves should not contain the logic to carry out
the operation (as this may vary between the different supported store types),
just the configuration.

For each operation, each Gaffer store will have an `OperationHandler`, where
the processing logic is contained. This enables operations to be handled
differently in each store.

Operations can be chained together to form an `OperationChain`. When an
operation chain is executed on a Gaffer graph the output of one operation
is passed to the input of the next.

An `OperationChain.Builder` is provided to help with constructing a valid
operation chain - it ensures the output type of an operation matches the
input type of the next.

## How to write an Operation

Operations should be written to be as generic as possible to allow them
to be applied to different graphs/stores.

Operations must be JSON serialisable in order to be used via the REST API
- i.e. there must be a public constructor and all the fields should have
getters and setters.

Operation implementations need to implement the `Operation` interface and
the extra interfaces they they wish to make use of. For example an operation
that takes a single input value should implement the `Input` interface.

Here is a list of some of the common interfaces:
- uk.gov.gchq.gaffer.operation.io.Input
- uk.gov.gchq.gaffer.operation.io.Output
- uk.gov.gchq.gaffer.operation.io.InputOutput - Use this instead of Input
and Output if your operation takes both input and output.
- uk.gov.gchq.gaffer.operation.io.MultiInput - Use this in addition if you
operation takes multiple inputs. This will help with JSON serialisation
- uk.gov.gchq.gaffer.operation.SeedMatching
- uk.gov.gchq.gaffer.operation.Validatable
- uk.gov.gchq.gaffer.operation.graph.OperationView
- uk.gov.gchq.gaffer.operation.graph.GraphFilters
- uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters
- uk.gov.gchq.gaffer.operation.Options

Each operation implementation should have a corresponding unit test class
that extends the `OperationTest` class.

Operation implementations should override the close method and ensure all
closeable fields are closed.

All implementations should also have a static inner `Builder` class that
implements the required builders. For example:

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