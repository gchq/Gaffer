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
package uk.gov.gchq.gaffer.example.operation.accumulo;

import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.example.operation.OperationExample;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

public class GetElementsWithinSetExample extends OperationExample {
    public static void main(final String[] args) {
        new GetElementsWithinSetExample().run();
    }

    public GetElementsWithinSetExample() {
        super(GetElementsWithinSet.class);
    }

    public void runExamples() {
        getElementsWithinSetOfVertices1And2And3();
        getElementsWithinSetOfVertices1And2And3WithCountGreaterThan2();
    }

    public Iterable<Element> getElementsWithinSetOfVertices1And2And3() {
        // ---------------------------------------------------------
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet.Builder<>()
                .addSeed(new EntitySeed(1))
                .addSeed(new EntitySeed(2))
                .addSeed(new EntitySeed(3))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<Element> getElementsWithinSetOfVertices1And2And3WithCountGreaterThan2() {
        // ---------------------------------------------------------
        final GetElementsWithinSet<Element> operation = new GetElementsWithinSet.Builder<>()
                .addSeed(new EntitySeed(1))
                .addSeed(new EntitySeed(2))
                .addSeed(new EntitySeed(3))
                .view(new View.Builder()
                        .entity("entity", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(2))
                                        .build())
                                .build())
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
