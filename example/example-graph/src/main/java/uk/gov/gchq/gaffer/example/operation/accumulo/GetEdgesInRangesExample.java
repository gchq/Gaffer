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
package uk.gov.gchq.gaffer.example.operation.accumulo;

import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetEdgesInRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.example.operation.OperationExample;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

public class GetEdgesInRangesExample extends OperationExample {
    public static void main(final String[] args) {
        new GetEdgesInRangesExample().run();
    }

    public GetEdgesInRangesExample() {
        super(GetEdgesInRanges.class);
    }

    @Override
    public void runExamples() {
        getAllEdgesInTheRangeFromEdge1_2ToEdge1_4();
        getAllEdgesInTheRangeFromEdge1_2ToEdge4_5();
    }

    public Iterable<Edge> getAllEdgesInTheRangeFromEdge1_2ToEdge1_4() {
        // ---------------------------------------------------------
        final GetEdgesInRanges<Pair<EdgeSeed>> operation = new GetEdgesInRanges.Builder<Pair<EdgeSeed>>()
                .addSeed(new Pair<>(new EdgeSeed(1, 2, true), new EdgeSeed(1, 4, true)))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<Edge> getAllEdgesInTheRangeFromEdge1_2ToEdge4_5() {
        // ---------------------------------------------------------
        final GetEdgesInRanges<Pair<ElementSeed>> operation = new GetEdgesInRanges.Builder<Pair<ElementSeed>>()
                .addSeed(new Pair<>((ElementSeed) new EdgeSeed(1, 2, true), new EdgeSeed(4, 5, true)))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }
}
