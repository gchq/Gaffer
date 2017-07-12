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

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import java.util.Set;

public class ToVerticesExample extends OperationExample {
    public ToVerticesExample() {
        super(ToVertices.class, "In these examples we use a ToSet operation after the ToVertices operation to deduplicate the results.");
    }

    public static void main(final String[] args) throws OperationException {
        new ToVerticesExample().run();
    }

    @Override
    public void runExamples() {
        extractEntityVertices();
        extractDestinationVertex();
        extractBothSourceAndDestinationVertices();
        extractMatchedVertices();
        extractOppositeMatchedVertices();
    }

    public Iterable<?> extractEntityVertices() {
        // ---------------------------------------------------------
        final OperationChain<Set<?>> opChain = new Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .view(new View.Builder()
                                .entity("entity")
                                .build())
                        .build())
                .then(new ToVertices.Builder()
                        .edgeVertices(ToVertices.EdgeVertices.NONE)
                        .build())
                .then(new ToSet<>())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }

    public Iterable<?> extractDestinationVertex() {
        // ---------------------------------------------------------
        final OperationChain<Set<?>> opChain = new Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                        .view(new View.Builder()
                                .edge("edge")
                                .build())
                        .build())
                .then(new ToVertices.Builder()
                        .edgeVertices(ToVertices.EdgeVertices.DESTINATION)
                        .build())
                .then(new ToSet<>())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }

    public Iterable<?> extractBothSourceAndDestinationVertices() {
        // ---------------------------------------------------------
        final OperationChain<Set<?>> opChain = new Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                        .view(new View.Builder()
                                .edge("edge")
                                .build())
                        .build())
                .then(new ToVertices.Builder()
                        .edgeVertices(ToVertices.EdgeVertices.BOTH)
                        .build())
                .then(new ToSet<>())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }


    public Iterable<?> extractMatchedVertices() {
        // ---------------------------------------------------------
        final OperationChain<Set<?>> opChain = new Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                        .view(new View.Builder()
                                .edge("edge")
                                .build())
                        .build())
                .then(new ToVertices.Builder()
                        .useMatchedVertex(ToVertices.UseMatchedVertex.EQUAL)
                        .build())
                .then(new ToSet<>())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }

    public Iterable<?> extractOppositeMatchedVertices() {
        // ---------------------------------------------------------
        final OperationChain<Set<?>> opChain = new Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                        .view(new View.Builder()
                                .edge("edge")
                                .build())
                        .build())
                .then(new ToVertices.Builder()
                        .useMatchedVertex(ToVertices.UseMatchedVertex.OPPOSITE)
                        .build())
                .then(new ToSet<>())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }
}
