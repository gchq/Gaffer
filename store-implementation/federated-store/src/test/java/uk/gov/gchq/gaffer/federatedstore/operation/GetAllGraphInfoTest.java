/*
 * Copyright 2017-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @see uk.gov.gchq.gaffer.federatedstore.AdminGetAllGraphInfoTest
 */
public class GetAllGraphInfoTest extends FederationOperationTest<GetAllGraphInfo> {

    public static final List<String> GRAPH_IDS = asList("a", "b", "c");

    @Override
    protected Set<String> getNonRequiredFields() {
        return new HashSet<>();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetAllGraphInfo operation = new GetAllGraphInfo.Builder()
                .option("a", "b")
                .graphIDs(GRAPH_IDS)
                .build();

        assertThat(operation.getOptions()).containsEntry("a", "b");
        assertThat(operation.getGraphIds()).isEqualTo(GRAPH_IDS);
    }

    @Test
    @Override
    public void shouldShallowCloneOperationREVIEWMAYBEDELETE() {
        GetAllGraphInfo operation = new GetAllGraphInfo.Builder()
                .option("a", "b")
                .graphIDs(GRAPH_IDS)
                .build();

        final GetAllGraphInfo clone = operation.shallowClone();

        //Then
        assertThat(clone)
                .isNotNull()
                .isEqualTo(operation);

        assertEquals("b", clone.getOption("a"));
        assertEquals(GRAPH_IDS, clone.getGraphIds());
    }

    @Override
    protected GetAllGraphInfo getTestObjectOld() {
        return new GetAllGraphInfo();
    }
}
