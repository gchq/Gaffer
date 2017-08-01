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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

public class LimitExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new LimitExample().run();
    }

    public LimitExample() {
        super(Limit.class);
    }

    @Override
    public void runExamples() {
        limitElementsTo3();
        limitElementsTo3WithoutTruncation();
        limitElementsTo3WithBuilder();
    }

    public Iterable<? extends Element> limitElementsTo3() {
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>(3))
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, null);
    }

    public void limitElementsTo3WithoutTruncation() {
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>(3, false))
                .build();
        // ---------------------------------------------------------

        showExample(opChain, "Setting this flag to false will " +
                "throw an error instead of truncating the iterable. " +
                "In this case there are more than 3 elements, so " +
                "when executed a LimitExceededException would be thrown.");
    }

    public Iterable<? extends Element> limitElementsTo3WithBuilder() {
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit.Builder<Element>()
                        .resultLimit(3)
                        .truncate(true)
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain, "A builder can also be used to create the limit -" +
                " note that truncate is set to true by default, so in this case it" +
                " is redundant, but simply shown for demonstration.");
    }
}
