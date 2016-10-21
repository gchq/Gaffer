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
        limitElementsTo3InChain();
    }

    public Iterable<Element> limitElementsTo3() {
        // ---------------------------------------------------------
        final GetAllElements<Element> operation = new GetAllElements.Builder<>()
                .limitResults(3)
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<Element> limitElementsTo3InChain() {
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<Element>> opChain = new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new Limit.Builder<Element>()
                        .limitResults(3)
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }
}
