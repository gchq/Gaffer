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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.simple.filter.IsMoreThan;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedElements;

public class GetRelatedElementsExample extends OperationExample {
    public static void main(final String[] args) {
        new GetRelatedElementsExample().run();
    }

    public GetRelatedElementsExample() {
        super(GetRelatedElements.class);
    }

    public void runExamples() {
        getEntitiesAndEdgesThatAreRelatedToVertex2();
        getAllEntitiesAndEdgesThatAreRelatedToEdge1to2();
        getAllEntitiesAndEdgesThatAreRelatedToEdge1to2WithCountGreaterThan1();
    }

    public CloseableIterable<Element> getEntitiesAndEdgesThatAreRelatedToVertex2() {
        // ---------------------------------------------------------
        final GetRelatedElements<EntitySeed, Element> operation = new GetRelatedElements.Builder<EntitySeed, Element>()
                .addSeed(new EntitySeed(2))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public CloseableIterable<Element> getAllEntitiesAndEdgesThatAreRelatedToEdge1to2() {
        // ---------------------------------------------------------
        final GetRelatedElements<EdgeSeed, Element> operation = new GetRelatedElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed(1, 2, true))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<Element> getAllEntitiesAndEdgesThatAreRelatedToEdge1to2WithCountGreaterThan1() {
        // ---------------------------------------------------------
        final GetRelatedElements<EdgeSeed, Element> operation = new GetRelatedElements.Builder<EdgeSeed, Element>()
                .addSeed(new EdgeSeed(1, 2, true))
                .view(new View.Builder()
                        .entity("entity", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .edge("edge", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }
}
