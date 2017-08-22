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
package uk.gov.gchq.gaffer.doc.operation;

import uk.gov.gchq.gaffer.doc.operation.generator.ElementGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;

public class AddElementsFromSocketExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new AddElementsFromSocketExample().run();
    }

    public AddElementsFromSocketExample() {
        super(AddElementsFromSocket.class, "This is not a core operation. To enable it to be handled by Apache Flink, see [flink-library/README.md](https://github.com/gchq/Gaffer/blob/master/library/flink-library/README.md)");
    }

    @Override
    public void runExamples() {
        addElementsFromSocket();
    }

    public void addElementsFromSocket() {
        // ---------------------------------------------------------
        final AddElementsFromSocket op = new AddElementsFromSocket.Builder()
                .hostname("localhost")
                .port(8080)
                .delimiter("\n")
                .generator(ElementGenerator.class)
                .parallelism(1)
                .validate(true)
                .skipInvalidElements(false)
                .build();
        // ---------------------------------------------------------

        showExample(op, null);
    }
}
