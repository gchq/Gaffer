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
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;

public class GetAdjacentEntitySeedsExample extends OperationExample {
    public static void main(final String[] args) {
        new GetAdjacentEntitySeedsExample().run();
    }

    public GetAdjacentEntitySeedsExample() {
        super(GetAdjacentEntitySeeds.class);
    }

    public void runExamples() {
        getAdjacentEntitySeedsFromVertex2();
        getAdjacentEntitySeedsAlongOutboundEdgesFromVertex2();
        getAdjacentEntitySeedsAlongOutboundEdgesFromVertex2WithCountGreaterThan1();
    }

    public CloseableIterable<EntitySeed> getAdjacentEntitySeedsFromVertex2() {
        // ---------------------------------------------------------
        final GetAdjacentEntitySeeds operation = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed(2))
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public CloseableIterable<EntitySeed> getAdjacentEntitySeedsAlongOutboundEdgesFromVertex2() {
        // ---------------------------------------------------------
        final GetAdjacentEntitySeeds operation = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed(2))
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public CloseableIterable<EntitySeed> getAdjacentEntitySeedsAlongOutboundEdgesFromVertex2WithCountGreaterThan1() {
        // ---------------------------------------------------------
        final GetAdjacentEntitySeeds operation = new GetAdjacentEntitySeeds.Builder()
                .addSeed(new EntitySeed(2))
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .view(new View.Builder()
                        .entity("entity", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .edge("edge", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .build())
                .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }
}
