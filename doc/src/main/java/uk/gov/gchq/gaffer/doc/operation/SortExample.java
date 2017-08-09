/*
 * Copyright 2017 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.doc.operation.function.ExampleScoreFunction;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

public class SortExample extends OperationExample {
    public SortExample() {
        super(Sort.class);
    }

    public static void main(final String[] args) throws OperationException {
        new SortExample().run();
    }

    @Override
    public void runExamples() {
        sortOnCount();
        sortOnCountWithoutDeduplicating();
        sortOnCountAndTransientProperty();
    }

    public Iterable<? extends Element> sortOnCount() {
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .build())
                .then(new Sort.Builder()
                        .comparators(new ElementPropertyComparator.Builder()
                                .groups("entity", "edge")
                                .property("count")
                                .reverse(false)
                                .build())
                        .resultLimit(10)
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }

    public Iterable<? extends Element> sortOnCountWithoutDeduplicating() {
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .build())
                .then(new Sort.Builder()
                        .comparators(new ElementPropertyComparator.Builder()
                                .groups("entity", "edge")
                                .property("count")
                                .reverse(false)
                                .build())
                        .resultLimit(10)
                        .deduplicate(false)
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, "Deduplication is true by default.");
    }

    public Iterable<? extends Element> sortOnCountAndTransientProperty() {
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .view(new View.Builder()
                                .entity("entity", new ViewElementDefinition.Builder()
                                        .transientProperty("score", Integer.class)
                                        .transformer(new ElementTransformer.Builder()
                                                .select("VERTEX", "count")
                                                .execute(new ExampleScoreFunction())
                                                .project("score")
                                                .build())
                                        .build())
                                .edge("edge", new ViewElementDefinition.Builder()
                                        .transientProperty("score", Integer.class)
                                        .transformer(new ElementTransformer.Builder()
                                                .select("DESTINATION", "count")
                                                .execute(new ExampleScoreFunction())
                                                .project("score")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(new Sort.Builder()
                        .comparators(
                                new ElementPropertyComparator.Builder()
                                        .groups("entity", "edge")
                                        .property("count")
                                        .reverse(false)
                                        .build(),
                                new ElementPropertyComparator.Builder()
                                        .groups("entity", "edge")
                                        .property("score")
                                        .reverse(false)
                                        .build()
                        )
                        .resultLimit(4)
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }
}
