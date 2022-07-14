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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph.Builder;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoveGraphTest extends FederationOperationTest<RemoveGraph> {

    private static final String EXPECTED_GRAPH_ID = "testGraphID";

    @Test
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException, JsonProcessingException {

        RemoveGraph op = new Builder()
                .graphId(EXPECTED_GRAPH_ID)
                .userRequestingAdminUsage(true)
                .build();

        byte[] serialise = toJson(op);
        RemoveGraph deserialise = fromJson(serialise);

        assertEquals(EXPECTED_GRAPH_ID, deserialise.getGraphId());
        assertTrue(deserialise.isUserRequestingAdminUsage());
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("graphId");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        RemoveGraph op = new Builder()
                .graphId(EXPECTED_GRAPH_ID)
                .userRequestingAdminUsage(true)
                .build();

        assertEquals(EXPECTED_GRAPH_ID, op.getGraphId());
        assertTrue(op.isUserRequestingAdminUsage());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        final RemoveGraph a = getTestObject();
        final RemoveGraph b = a.shallowClone();
        assertEquals(a.getGraphId(), b.getGraphId());
    }

    @Override
    protected RemoveGraph getTestObject() {
        return new RemoveGraph();
    }
}
