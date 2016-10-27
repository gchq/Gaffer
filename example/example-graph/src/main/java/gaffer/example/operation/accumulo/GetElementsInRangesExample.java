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
package gaffer.example.operation.accumulo;

import gaffer.accumulostore.operation.impl.GetElementsInRanges;
import gaffer.accumulostore.utils.Pair;
import gaffer.data.element.Element;
import gaffer.example.operation.OperationExample;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;

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

    public Iterable<Element> getAllElementsInTheRangeFromEntity1toEntity4() {
        // ---------------------------------------------------------
        final GetElementsInRanges<Pair<EntitySeed>, Element> operation = new GetElementsInRanges.Builder<Pair<EntitySeed>, Element>()
                .addSeed(new Pair<>(new EntitySeed(1), new EntitySeed(4)))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<Element> getAllElementsInTheRangeFromEntity4ToEdge4_5() {
        // ---------------------------------------------------------
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges.Builder<Pair<ElementSeed>, Element>()
                .addSeed(new Pair<>(new EntitySeed(4), new EdgeSeed(4, 5, true)))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }
}
