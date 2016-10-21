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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.simple.filter.IsMoreThan;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdgesBySeed;

public class GetEdgesBySeedExample extends OperationExample {
    public static void main(final String[] args) {
        new GetEdgesBySeedExample().run();
    }

    public GetEdgesBySeedExample() {
        super(GetEdgesBySeed.class);
    }

    public void runExamples() {
        getEdgesByEdgeSeeds1to2and2to3();
        getEdgesByEdgeSeeds1to2and2to3WithCountGreaterThan2();
    }

    public CloseableIterable<Edge> getEdgesByEdgeSeeds1to2and2to3() {
        // ---------------------------------------------------------
        final GetEdgesBySeed operation = new GetEdgesBySeed.Builder()
                .addSeed(new EdgeSeed(1, 2, true))
                .addSeed(new EdgeSeed(2, 3, true))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public CloseableIterable<Edge> getEdgesByEdgeSeeds1to2and2to3WithCountGreaterThan2() {
        // ---------------------------------------------------------
        final GetEdgesBySeed operation = new GetEdgesBySeed.Builder()
                .addSeed(new EdgeSeed(1, 2, true))
                .addSeed(new EdgeSeed(2, 3, true))
                .view(new View.Builder()
                        .edge("edge", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(2))
                                        .build())
                                .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }
}
