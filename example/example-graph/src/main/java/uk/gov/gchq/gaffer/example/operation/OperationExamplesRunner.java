/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.example.operation;

import uk.gov.gchq.gaffer.example.util.Example;
import uk.gov.gchq.gaffer.example.util.ExamplesRunner;
import uk.gov.gchq.gaffer.operation.Operation;

/**
 * This runner will run all operation examples.
 */
public class OperationExamplesRunner extends ExamplesRunner {
    public static void main(final String[] args) throws Exception {
        new OperationExamplesRunner().run();
    }

    public void run() throws Exception {
        run(OperationExample.class, Operation.class, "operation");
    }

    @Override
    protected void printTableOfContents(final Class<? extends Example> exampleParentClass) throws InstantiationException, IllegalAccessException {
        super.printTableOfContents(exampleParentClass);
        log("In addition to these core operations, stores can implement their own specific operations. See [Accumulo specific operation examples](https://github.com/gchq/Gaffer/wiki/Accumulo-specific-operation-examples).\n");
    }
}
