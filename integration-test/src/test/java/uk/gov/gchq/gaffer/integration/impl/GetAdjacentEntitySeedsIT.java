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

package uk.gov.gchq.gaffer.integration.impl;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class GetAdjacentEntitySeedsIT extends AbstractStoreIT {
    private static final List<String> SEEDS = Arrays.asList(
            SOURCE_1, DEST_2, SOURCE_3, DEST_3,
            SOURCE_DIR_1, DEST_DIR_2, SOURCE_DIR_3, DEST_DIR_3,
            "A1");

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldGetEntitySeeds() throws Exception {
        for (final GraphFilters.IncludeIncomingOutgoingType inOutType : GraphFilters.IncludeIncomingOutgoingType.values()) {
            for (final GraphFilters.DirectedType directedType : GraphFilters.DirectedType.values()) {
                final List<String> expectedSeeds = new ArrayList<>();

                if (GraphFilters.DirectedType.DIRECTED != directedType) {
                    expectedSeeds.add(DEST_1);
                    expectedSeeds.add(SOURCE_2);
                    expectedSeeds.add(DEST_3);
                    expectedSeeds.add(SOURCE_3);
                    expectedSeeds.add("A1");
                    expectedSeeds.add("B1");
                    expectedSeeds.add("C1");
                    expectedSeeds.add("D1");
                }

                if (GraphFilters.IncludeIncomingOutgoingType.INCOMING != inOutType) {
                    if (GraphFilters.DirectedType.UNDIRECTED != directedType) {
                        expectedSeeds.add(DEST_DIR + "1");
                        expectedSeeds.add(DEST_DIR_3);
                    }
                }

                if (GraphFilters.IncludeIncomingOutgoingType.OUTGOING != inOutType) {
                    if (GraphFilters.DirectedType.UNDIRECTED != directedType) {
                        expectedSeeds.add(SOURCE_DIR_2);
                        expectedSeeds.add(SOURCE_DIR_3);
                    }
                }

                shouldGetEntitySeeds(expectedSeeds, inOutType, directedType);
            }
        }
    }

    private void shouldGetEntitySeeds(final List<String> expectedResultSeeds,
                                      final GraphFilters.IncludeIncomingOutgoingType inOutType,
                                      final GraphFilters.DirectedType directedType
    )
            throws IOException, OperationException {
        // Given
        final User user = new User();
        final List<EntitySeed> seeds = new ArrayList<>();
        for (final String seed : SEEDS) {
            seeds.add(new EntitySeed(seed));
        }

        final GetAdjacentEntitySeeds operation = new GetAdjacentEntitySeeds.Builder()
                .seeds(seeds)
                .directedType(directedType)
                .inOutType(inOutType)
                .build();

        // When
        final CloseableIterable<EntitySeed> results = graph.execute(operation, user);

        // Then
        List<String> resultSeeds = new ArrayList<>();
        for (final EntitySeed result : results) {
            resultSeeds.add((String) result.getVertex());
        }
        Collections.sort(resultSeeds);
        Collections.sort(expectedResultSeeds);
        assertArrayEquals("InOut=" + inOutType + ", directedType=" + directedType + ". Expected: " + expectedResultSeeds + ", but got: " + resultSeeds, expectedResultSeeds.toArray(), resultSeeds.toArray());
    }
}