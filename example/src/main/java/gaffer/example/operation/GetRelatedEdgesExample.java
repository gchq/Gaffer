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
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.graph.Graph;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetRelatedEdges;

public class GetRelatedEdgesExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new GetRelatedEdgesExample().run();
    }

    public GetRelatedEdgesExample() {
        super(GetRelatedEdges.class);
    }

    public void runExamples(final Graph graph) throws OperationException {
        getAllEdgesThatAreConnectedToVertex2(graph);
        getAllOutboundEdgesThatAreConnectedToVertex2(graph);
        getAllOutboundEdgesThatAreConnectedToVertex2WithCountGreaterThan1(graph);
    }

    public Iterable<Edge> getAllEdgesThatAreConnectedToVertex2(final Graph graph) throws OperationException {
        final String opJava = "new GetRelatedEdges.Builder<EntitySeed>()\n"
                + "                .addSeed(new EntitySeed(2))\n"
                + "                .build()";
        return runAndPrintOperation(new GetRelatedEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed(2))
                .build(), graph, opJava);
    }

    public Iterable<Edge> getAllOutboundEdgesThatAreConnectedToVertex2(final Graph graph) throws OperationException {
        final String opJava = "new GetRelatedEdges.Builder<EntitySeed>()\n"
                + "                .addSeed(new EntitySeed(2))\n"
                + "                .inOutType(IncludeIncomingOutgoingType.OUTGOING)\n"
                + "                .build();";
        return runAndPrintOperation(new GetRelatedEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed(2))
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .build(), graph, opJava);
    }

    public Iterable<Edge> getAllOutboundEdgesThatAreConnectedToVertex2WithCountGreaterThan1(final Graph graph) throws OperationException {
        final String opJava = "new GetRelatedEdges.Builder<EntitySeed>()\n"
                + "                .addSeed(new EntitySeed(2))\n"
                + "                .inOutType(IncludeIncomingOutgoingType.OUTGOING)\n"
                + "                .view(new View.Builder()\n"
                + "                        .edge(\"edge\", new ViewElementDefinition.Builder()\n"
                + "                                .filter(new ElementFilter.Builder()\n"
                + "                                        .select(\"count\")\n"
                + "                                        .execute(new IsMoreThan(1))\n"
                + "                                        .build())\n"
                + "                                .build())\n"
                + "                        .build())\n"
                + "                .build();";
        return runAndPrintOperation(new GetRelatedEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed(2))
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .view(new View.Builder()
                        .edge("edge", new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .build())
                .build(), graph, opJava);
    }

}
