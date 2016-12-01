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

import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetEntitiesInRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.example.operation.OperationExample;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

public class GetEntitiesInRangesExample extends OperationExample {
    public static void main(final String[] args) {
        new GetEntitiesInRangesExample().run();
    }

    public GetEntitiesInRangesExample() {
        super(GetEntitiesInRanges.class);
    }

    @Override
    public void runExamples() {
        getAllEntitiesInTheRangeFromEntity1toEntity4();
        getAllEntitiesInTheRangeFromEntity4ToEdge4_5();
    }

    public Iterable<Entity> getAllEntitiesInTheRangeFromEntity1toEntity4() {
        // ---------------------------------------------------------
        final GetEntitiesInRanges<Pair<EntitySeed>> operation = new GetEntitiesInRanges.Builder<Pair<EntitySeed>>()
                .addSeed(new Pair<>(new EntitySeed(1), new EntitySeed(4)))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<Entity> getAllEntitiesInTheRangeFromEntity4ToEdge4_5() {
        // ---------------------------------------------------------
        final GetEntitiesInRanges<Pair<ElementSeed>> operation = new GetEntitiesInRanges.Builder<Pair<ElementSeed>>()
                .addSeed(new Pair<>(new EntitySeed(4), new EdgeSeed(4, 5, true)))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }
}
