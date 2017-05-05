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
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import java.util.Set;

public class ToSetExample extends OperationExample {
    public ToSetExample() {
        super(ToSet.class, "Note - conversion into a Set is done using an in memory LinkedHashSet, so it is not advised for a large number of results.");
    }

    public static void main(final String[] args) throws OperationException {
        new ToSetExample().run();
    }

    @Override
    public void runExamples() {
        withoutToSetOperation();
        withToSetOperation();
    }

    public CloseableIterable<? extends Element> withoutToSetOperation() {
        // ---------------------------------------------------------
        final GetElements operation = new GetElements.Builder()
                .input(new EntitySeed(1), new EntitySeed(2))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Set<? extends Element> withToSetOperation() {
        // ---------------------------------------------------------
        final OperationChain<Set<? extends Element>> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(1), new EntitySeed(2))
                        .build())
                .then(new ToSet<>())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }
}
