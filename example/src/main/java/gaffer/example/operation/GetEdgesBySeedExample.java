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
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.impl.get.GetEdgesBySeed;

public class GetEdgesBySeedExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new GetEdgesBySeedExample().run();
    }

    public GetEdgesBySeedExample() {
        super(GetEdgesBySeed.class);
    }

    public void runExamples(final Graph graph) throws OperationException {
        getEdgesByEdgeSeeds1to2and2to3(graph);
        getEdgesByEdgeSeeds1to2and2to3WithCountGreaterThan2(graph);
    }

    public Iterable<Edge> getEdgesByEdgeSeeds1to2and2to3(final Graph graph) throws OperationException {
        return runAndPrintOperation(new GetEdgesBySeed.Builder()
                .addSeed(new EdgeSeed(1, 2, true))
                .addSeed(new EdgeSeed(2, 3, true))
                .build(), graph);
    }

    public Iterable<Edge> getEdgesByEdgeSeeds1to2and2to3WithCountGreaterThan2(final Graph graph) throws OperationException {
        return runAndPrintOperation(new GetEdgesBySeed.Builder()
                .addSeed(new EdgeSeed(1, 2, true))
                .addSeed(new EdgeSeed(2, 3, true))
                .view(new View.Builder()
                        .edge("edge", new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select(COUNT)
                                        .execute(new IsMoreThan(2))
                                        .build())
                                .build())
                        .build())
                .build(), graph);
    }
}
