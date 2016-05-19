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

import gaffer.data.element.Entity;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.impl.get.GetRelatedEntities;

public class GetRelatedEntitiesExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new GetRelatedEntitiesExample().run();
    }

    public GetRelatedEntitiesExample() {
        super(GetRelatedEntities.class);
    }

    @Override
    public void runExamples(final Graph graph) throws OperationException {
        getAllEntitiesThatAreConnectedToEdge1to2(graph);
        getAllEntitiesThatAreConnectedToEdge1to2WithCountGreaterThan1(graph);
    }

    public Iterable<Entity> getAllEntitiesThatAreConnectedToEdge1to2(final Graph graph) throws OperationException {
        final String opJava = "new GetRelatedEntities.Builder<EdgeSeed>()\n"
                + "                .addSeed(new EdgeSeed(1, 2, true))\n"
                + "                .build()";
        return runAndPrintOperation(new GetRelatedEntities.Builder<EdgeSeed>()
                .addSeed(new EdgeSeed(1, 2, true))
                .build(), graph, opJava);
    }

    public Iterable<Entity> getAllEntitiesThatAreConnectedToEdge1to2WithCountGreaterThan1(final Graph graph) throws OperationException {
        final String opJava = "new GetRelatedEntities.Builder<EdgeSeed>()\n"
                + "                .addSeed(new EdgeSeed(1, 2, true))\n"
                + "                .view(new View.Builder()\n"
                + "                        .entity(\"entity\", new ViewElementDefinition.Builder()\n"
                + "                                .filter(new ElementFilter.Builder()\n"
                + "                                        .select(\"count\")\n"
                + "                                        .execute(new IsMoreThan(1))\n"
                + "                                        .build())\n"
                + "                                .build())\n"
                + "                        .build())\n"
                + "                .build();";
        return runAndPrintOperation(new GetRelatedEntities.Builder<EdgeSeed>()
                .addSeed(new EdgeSeed(1, 2, true))
                .view(new View.Builder()
                        .entity("entity", new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .build())
                .build(), graph, opJava);
    }
}
