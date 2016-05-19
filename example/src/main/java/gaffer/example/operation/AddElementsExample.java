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
package gaffer.example.operation;

import gaffer.data.element.Edge;
import gaffer.data.element.Entity;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.impl.add.AddElements;
import gaffer.user.User;

public class AddElementsExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new AddElementsExample().run();
    }

    public AddElementsExample() {
        super(AddElements.class);
    }

    public void runExamples(final Graph graph) throws OperationException {
        addElements(graph);
    }

    public void addElements(final Graph graph) throws OperationException {
        System.out.println(getMethodNameAsSentence() + "\n");

        final AddElements operation = new AddElements.Builder()
                .elements(new Entity.Builder()
                                .group("entity")
                                .vertex(6)
                                .property(COUNT, 1)
                                .build(),
                        new Edge.Builder()
                                .group("edge")
                                .source(5).dest(6).directed(true)
                                .property(COUNT, 1)
                                .build())
                .build();
        final String operationJson = getOperationJson(operation);

        graph.execute(operation, new User("user01"));
        System.out.println("Elements added. Updated graph:\n"
                + "    ---- 4\n"
                + "  /    /  \\\n"
                + " 1 -- 2 -- 3\n"
                + "       \\\n"
                + "        5 -- 6\n");

        outputJson(operationJson);
    }
}
