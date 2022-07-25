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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @see uk.gov.gchq.gaffer.federatedstore.AdminGetAllGraphInfoTest
 */
public class GetAllGraphInfoTest extends FederationOperationTest<GetAllGraphInfo> {

    public static final String GRAPH_IDS_CSV = "a,b,c";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetAllGraphInfo operation = new GetAllGraphInfo.Builder()
                .option("a", "b")
                .graphIDsCSV(GRAPH_IDS_CSV)
                .build();

        assertThat(operation.getOptions()).containsEntry("a", "b");
        assertThat(operation.getGraphIdsCSV()).isEqualTo(GRAPH_IDS_CSV);
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        GetAllGraphInfo operation = new GetAllGraphInfo.Builder()
                .option("a", "b")
                .graphIDsCSV(GRAPH_IDS_CSV)
                .build();

        final GetAllGraphInfo clone = operation.shallowClone();
        assertNotNull(clone);
        assertEquals("b", clone.getOption("a"));
        assertEquals(GRAPH_IDS_CSV, clone.getGraphIdsCSV());
        assertEquals(operation, clone);
    }

    @Override
    protected GetAllGraphInfo getTestObject() {
        return new GetAllGraphInfo();
    }
}
