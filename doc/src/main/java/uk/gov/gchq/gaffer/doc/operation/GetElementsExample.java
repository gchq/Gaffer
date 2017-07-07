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
package uk.gov.gchq.gaffer.doc.operation;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Or;

public class GetElementsExample extends OperationExample {
    public GetElementsExample() {
        super(GetElements.class);
    }

    public static void main(final String[] args) {
        new GetElementsExample().run();
    }

    @Override
    public void runExamples() {
        getEntitiesAndEdgesByEntityId2AndEdgeId2to3();
        getEntitiesAndEdgesByEntityId2AndEdgeId2to3WithCountMoreThan1();

        getEntitiesAndEdgesThatAreRelatedToVertex2();
        getAllEntitiesAndEdgesThatAreRelatedToEdge1to2();
        getAllEntitiesAndEdgesThatAreRelatedToEdge1to2WithCountMoreThan1();

        getEntitiesRelatedTo2WithCountLessThan2OrMoreThan5();
        getEdgesRelatedTo2WhenSourceIsLessThan2OrDestinationIsMoreThan3();
    }

    public CloseableIterable<? extends Element> getEntitiesAndEdgesThatAreRelatedToVertex2() {
        // ---------------------------------------------------------
        final GetElements operation = new GetElements.Builder()
                .input(new EntitySeed(2))
                .build();
        // ---------------------------------------------------------

        return runExample(operation, null);
    }

    public CloseableIterable<? extends Element> getAllEntitiesAndEdgesThatAreRelatedToEdge1to2() {
        // ---------------------------------------------------------
        final GetElements operation = new GetElements.Builder()
                .input(new EdgeSeed(1, 2, DirectedType.EITHER))
                .build();
        // ---------------------------------------------------------

        return runExample(operation, null);
    }

    public CloseableIterable<? extends Element> getAllEntitiesAndEdgesThatAreRelatedToEdge1to2WithCountMoreThan1() {
        // ---------------------------------------------------------
        final GetElements operation = new GetElements.Builder()
                .input(new EdgeSeed(1, 2, DirectedType.EITHER))
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

        return runExample(operation, null);
    }

    public CloseableIterable<? extends Element> getEntitiesAndEdgesByEntityId2AndEdgeId2to3() {
        // ---------------------------------------------------------
        final GetElements operation = new GetElements.Builder()
                .input(new EntitySeed(2), new EdgeSeed(2, 3, DirectedType.EITHER))
                .build();
        // ---------------------------------------------------------

        return runExample(operation, null);
    }

    public CloseableIterable<? extends Element> getEntitiesAndEdgesByEntityId2AndEdgeId2to3WithCountMoreThan1() {
        // ---------------------------------------------------------
        final GetElements operation = new GetElements.Builder()
                .input(new EntitySeed(2), new EdgeSeed(2, 3, DirectedType.EITHER))
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

        return runExample(operation, null);
    }

    public CloseableIterable<? extends Element> getEntitiesRelatedTo2WithCountLessThan2OrMoreThan5() {
        // ---------------------------------------------------------
        final GetElements operation = new GetElements.Builder()
                .input(new EntitySeed(2), new EdgeSeed(2, 3, DirectedType.EITHER))
                .view(new View.Builder()
                        .entity("entity", new ViewElementDefinition.Builder()
                                .preAggregationFilter(
                                        new ElementFilter.Builder()
                                                .select("count")
                                                .execute(new Or<>(new IsLessThan(2), new IsMoreThan(5)))
                                                .build())
                                .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(operation,
                "When using an Or predicate with a single selected value you can just do 'select(propertyName)' then 'execute(new Or(predicates))'");
    }

    public CloseableIterable<? extends Element> getEdgesRelatedTo2WhenSourceIsLessThan2OrDestinationIsMoreThan3() {
        // ---------------------------------------------------------
        final GetElements operation = new GetElements.Builder()
                .input(new EntitySeed(2))
                .view(new View.Builder()
                        .edge("edge", new ViewElementDefinition.Builder()
                                .preAggregationFilter(
                                        new ElementFilter.Builder()
                                                .select(IdentifierType.SOURCE.name(), IdentifierType.DESTINATION.name())
                                                .execute(new Or.Builder<>()
                                                        .select(0)
                                                        .execute(new IsLessThan(2))
                                                        .select(1)
                                                        .execute(new IsMoreThan(3))
                                                        .build())
                                                .build())
                                .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(operation,
                "When using an Or predicate with a multiple selected values, it is more complicated. " +
                        "First, you need to select all the values you want: 'select(a, b, c)'. This will create an array of the selected values, [a, b, c]. " +
                        "You then need to use the Or.Builder to build your Or predicate, using .select() then .execute(). " +
                        "When selecting values in the Or.Builder you need to refer to the position in the [a,b,c] array. So to use property 'a', use position 0 - select(0).");
    }
}
