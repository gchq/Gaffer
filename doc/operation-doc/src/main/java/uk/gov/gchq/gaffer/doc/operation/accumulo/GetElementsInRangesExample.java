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
package uk.gov.gchq.gaffer.doc.operation.accumulo;

import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.doc.operation.OperationExample;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

@SuppressWarnings("unchecked")
public class GetElementsInRangesExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new GetElementsInRangesExample().run();
    }

    public GetElementsInRangesExample() {
        super(GetElementsInRanges.class);
    }

    @Override
    public void runExamples() {
        getAllElementsInTheRangeFromEntity1toEntity4();
        getAllElementsInTheRangeFromEntity4ToEdge4_5();
    }

    public CloseableIterable<? extends Element> getAllElementsInTheRangeFromEntity1toEntity4() {
        // ---------------------------------------------------------
        final GetElementsInRanges operation = new GetElementsInRanges.Builder()
                .input(new Pair<>(new EntitySeed(1), new EntitySeed(4)))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public CloseableIterable<? extends Element> getAllElementsInTheRangeFromEntity4ToEdge4_5() {
        // ---------------------------------------------------------
        final GetElementsInRanges operation = new GetElementsInRanges.Builder()
                .input(new Pair<>(new EntitySeed(4), new EdgeSeed(4, 5, true)))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }
}
