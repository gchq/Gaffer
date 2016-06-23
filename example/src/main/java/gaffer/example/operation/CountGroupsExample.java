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

import gaffer.data.GroupCounts;
import gaffer.graph.Graph;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.impl.CountGroups;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.operation.impl.get.GetRelatedEntities;

public class CountGroupsExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new CountGroupsExample().run();
    }

    public CountGroupsExample() {
        super(GetRelatedEntities.class);
    }

    @Override
    public void runExamples(final Graph graph) throws OperationException {
        countAllElementGroups(graph);
        countAllElementGroupsWithLimit(graph);
    }

    public GroupCounts countAllElementGroups(final Graph graph) throws OperationException {
        final String opJava = "new OperationChain.Builder()\n"
                + "                .first(new GetAllElements<>())\n"
                + "                .then(new CountGroups())\n"
                + "                .build()";
        return runAndPrintOperationChainResult(new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new CountGroups())
                .build(), graph, opJava);
    }

    public GroupCounts countAllElementGroupsWithLimit(final Graph graph) throws OperationException {
        final String opJava = "new OperationChain.Builder()\n"
                + "                .first(new GetAllElements<>())\n"
                + "                .then(new CountGroups(5))\n"
                + "                .build()";
        return runAndPrintOperationChainResult(new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new CountGroups(5))
                .build(), graph, opJava);
    }
}
