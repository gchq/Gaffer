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
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Deduplicate;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;

public class DeduplicateExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new DeduplicateExample().run();
    }

    public DeduplicateExample() {
        super(Deduplicate.class, "Note - deduplication is done using an in memory HashSet, so it is not advised for a large number of results.");
    }

    @Override
    public void runExamples() {
        withoutDeduplicatingEdges();
        withDeduplicateEdgesFlag();
        withDeduplicateEdgesChain();
    }

    public Iterable<Edge> withoutDeduplicatingEdges() {
        // ---------------------------------------------------------
        final GetEdges<ElementSeed> operation = new GetEdges.Builder<>()
                .addSeed(new EntitySeed(1))
                .addSeed(new EntitySeed(2))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<Edge> withDeduplicateEdgesFlag() {
        // ---------------------------------------------------------
        final GetEdges<ElementSeed> build = new GetEdges.Builder<>()
                .addSeed(new EntitySeed(1))
                .addSeed(new EntitySeed(2))
                .deduplicate(true)
                .build();
        // ---------------------------------------------------------

        return runExample(build);
    }

    public Iterable<Edge> withDeduplicateEdgesChain() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<Edge>> opChain = new OperationChain.Builder()
                .first(new GetEdges.Builder<>()
                        .addSeed(new EntitySeed(1))
                        .addSeed(new EntitySeed(2))
                        .build())
                .then(new Deduplicate<Edge>())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }
}
