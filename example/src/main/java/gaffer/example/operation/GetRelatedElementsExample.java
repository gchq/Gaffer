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

import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetRelatedElements;

public class GetRelatedElementsExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new GetRelatedElementsExample().run();
    }

    public GetRelatedElementsExample() {
        super(GetRelatedElements.class);
    }

    public void runExamples(final Graph graph) throws OperationException {
        getEntitiesAndEdgesThatAreRelatedToVertex2(graph);
        getAllEntitiesAndEdgesThatAreRelatedToEdge1to2(graph);
        getAllEntitiesAndEdgesThatAreRelatedToEdge1to2WithCountGreaterThan1(graph);
    }

    public Iterable<Element> getEntitiesAndEdgesThatAreRelatedToVertex2(final Graph graph) throws OperationException {
        final String opJava = "new GetRelatedElements.Builder<EntitySeed, Element>()\n"
                + "                .addSeed(new EntitySeed(2))\n"
                + "                .build();";
        return runAndPrintOperation(new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed(2))
                .build(), graph, opJava);
    }

    public Iterable<Element> getAllEntitiesAndEdgesThatAreRelatedToEdge1to2(final Graph graph) throws OperationException {
        final String opJava = "new GetRelatedElements.Builder<EdgeSeed, Element>()\n"
                + "                .addSeed(new EdgeSeed(1, 2, true))\n"
                + "                .build();";
        return runAndPrintOperation(new GetRelatedElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed(1, 2, true))
                .build(), graph, opJava);
    }

    public Iterable<Element> getAllEntitiesAndEdgesThatAreRelatedToEdge1to2WithCountGreaterThan1(final Graph graph) throws OperationException {
        final String opJava = "new GetRelatedElements.Builder<EdgeSeed, Element>()\n"
                + "                .addSeed(new EdgeSeed(1, 2, true))\n"
                + "                .view(new View.Builder()\n"
                + "                        .entity(\"entity\", new ViewElementDefinition.Builder()\n"
                + "                                .filter(new ElementFilter.Builder()\n"
                + "                                        .select(\"count\")\n"
                + "                                        .execute(new IsMoreThan(1))\n"
                + "                                        .build())\n"
                + "                                .build())\n"
                + "                        .edge(\"edge\", new ViewElementDefinition.Builder()\n"
                + "                                .filter(new ElementFilter.Builder()\n"
                + "                                        .select(\"count\")\n"
                + "                                        .execute(new IsMoreThan(1))\n"
                + "                                        .build())\n"
                + "                                .build())\n"
                + "                        .build())\n"
                + "                .build();";
        return runAndPrintOperation(new GetRelatedElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed(1, 2, true))
                .view(new View.Builder()
                        .entity("entity", new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
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
