/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.template;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_2;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_3;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_DIR;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_DIR_2;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_DIR_3;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_2;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_3;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR_2;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR_3;

public class GetAdjacentIdsITTemplate extends AbstractStoreIT {
    private static final List<String> SEEDS = Arrays.asList(
            SOURCE_1, DEST_2, SOURCE_3, DEST_3,
            SOURCE_DIR_1, DEST_DIR_2, SOURCE_DIR_3, DEST_DIR_3,
            "A1");

    @GafferTest
    public void shouldGetEntityIds(final GafferTestCase testCase) throws Exception {
        Graph graph = testCase.getPopulatedGraph();
        final List<DirectedType> directedTypes = Lists.newArrayList(DirectedType.values());
        directedTypes.add(null);

        final List<IncludeIncomingOutgoingType> inOutTypes = Lists.newArrayList(IncludeIncomingOutgoingType.values());
        inOutTypes.add(null);
        for (final IncludeIncomingOutgoingType inOutType : inOutTypes) {
            for (final DirectedType directedType : directedTypes) {
                final List<String> expectedSeeds = new ArrayList<>();

                if (DirectedType.DIRECTED != directedType) {
                    expectedSeeds.add(DEST_1);
                    expectedSeeds.add(SOURCE_2);
                    expectedSeeds.add(DEST_3);
                    expectedSeeds.add(SOURCE_3);
                    expectedSeeds.add("A1");
                    expectedSeeds.add("B1");
                    expectedSeeds.add("C1");
                    expectedSeeds.add("D1");
                }

                if (IncludeIncomingOutgoingType.INCOMING != inOutType) {
                    if (DirectedType.UNDIRECTED != directedType) {
                        expectedSeeds.add(DEST_DIR + "1");
                        expectedSeeds.add(DEST_DIR_3);
                        expectedSeeds.add("A1");
                        expectedSeeds.add("B1");
                        expectedSeeds.add("C1");
                        expectedSeeds.add("D1");
                    }
                }

                if (IncludeIncomingOutgoingType.OUTGOING != inOutType) {
                    if (DirectedType.UNDIRECTED != directedType) {
                        expectedSeeds.add(SOURCE_DIR_2);
                        expectedSeeds.add(SOURCE_DIR_3);
                    }
                }

                shouldGetEntityIds(expectedSeeds, inOutType, directedType, graph);
            }
        }
    }

    private void shouldGetEntityIds(final List<String> expectedResultSeeds,
                                    final IncludeIncomingOutgoingType inOutType,
                                    final DirectedType directedType,
                                    final Graph graph
    )
            throws OperationException {
        // Given
        final User user = new User();
        final List<EntityId> seeds = new ArrayList<>();
        for (final String seed : SEEDS) {
            seeds.add(new EntitySeed(seed));
        }

        final GetAdjacentIds operation = new GetAdjacentIds.Builder()
                .input(seeds)
                .directedType(directedType)
                .inOutType(inOutType)
                .build();

        // When
        final CloseableIterable<? extends EntityId> results = graph.execute(operation, user);

        // Then
        List<String> resultSeeds = new ArrayList<>();
        for (final EntityId result : results) {
            resultSeeds.add((String) result.getVertex());
        }
        Collections.sort(resultSeeds);
        Collections.sort(expectedResultSeeds);
        assertArrayEquals(expectedResultSeeds.toArray(), resultSeeds.toArray(), "InOut=" + inOutType + ", directedType=" + directedType
            + ".\nExpected: \n  " + StringUtils.join(expectedResultSeeds, "\n  ")
            + " \nbut got: \n  " + StringUtils.join(resultSeeds, "\n  "));
    }
}
