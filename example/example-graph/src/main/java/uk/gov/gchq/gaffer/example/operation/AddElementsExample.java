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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.user.User;

public class AddElementsExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new AddElementsExample().run();
    }

    public AddElementsExample() {
        super(AddElements.class);
    }

    @Override
    public void runExamples() {
        try {
            addElements();
        } catch (OperationException e) {
            throw new RuntimeException(e);
        }
    }

    public void addElements() throws OperationException {
        log("#### " + getMethodNameAsSentence(0) + "\n");
        printGraph();

        final AddElements operation = new AddElements.Builder()
                .elements(new Entity.Builder()
                                .group("entity")
                                .vertex(6)
                                .property("count", 1)
                                .build(),
                        new Edge.Builder()
                                .group("edge")
                                .source(5).dest(6).directed(true)
                                .property("count", 1)
                                .build())
                .build();
        printJava("new AddElements.Builder()\n"
                + "                .elements(new Entity.Builder()\n"
                + "                                .group(\"entity\")\n"
                + "                                .vertex(6)\n"
                + "                                .property(\"count\", 1)\n"
                + "                                .build(),\n"
                + "                        new Edge.Builder()\n"
                + "                                .group(\"edge\")\n"
                + "                                .source(5).dest(6).directed(true)\n"
                + "                                .property(\"count\", 1)\n"
                + "                                .build())\n"
                + "                .build();");
        printAsJson(operation);
        printOperationClass(operation);

        getGraph().execute(operation, new User("user01"));

        log("Updated graph:");
        log("```");
        log("    --> 4 <--");
        log("  /     ^     \\");
        log(" /      |      \\");
        log("1  -->  2  -->  3");
        log("         \\");
        log("           -->  5  -->  6");
        log("```");
    }
}
