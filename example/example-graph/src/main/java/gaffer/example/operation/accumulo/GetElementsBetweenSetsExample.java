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
package gaffer.example.operation.accumulo;

import gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.example.operation.OperationExample;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.operation.data.EntitySeed;

public class GetElementsBetweenSetsExample extends OperationExample {
    public static void main(final String[] args) {
        new GetElementsBetweenSetsExample().run();
    }

    public GetElementsBetweenSetsExample() {
        super(GetElementsBetweenSets.class);
    }

    public void runExamples() {
        getElementsWithinSetOfVertices1And2And4();
        getElementsWithinSetOfVertices1And2And4WithCountGreaterThan2();
    }

    public Iterable<Element> getElementsWithinSetOfVertices1And2And4() {
        // ---------------------------------------------------------
        final GetElementsBetweenSets<Element> operation = new GetElementsBetweenSets.Builder<>()
                .addSeed(new EntitySeed(1))
                .addSeedB(new EntitySeed(2))
                .addSeedB(new EntitySeed(4))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<Element> getElementsWithinSetOfVertices1And2And4WithCountGreaterThan2() {
        // ---------------------------------------------------------
        final GetElementsBetweenSets<Element> operation = new GetElementsBetweenSets.Builder<>()
                .addSeed(new EntitySeed(1))
                .addSeedB(new EntitySeed(2))
                .addSeedB(new EntitySeed(4))
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
