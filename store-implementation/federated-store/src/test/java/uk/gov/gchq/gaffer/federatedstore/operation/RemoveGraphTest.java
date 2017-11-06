/*
 * Copyright 2017 Crown Copyright
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
import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph.Builder;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

public class RemoveGraphTest extends OperationTest<RemoveGraph> {

    private static final String EXPECTED_GRAPH_ID = "testGraphID";

    @Test
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException, JsonProcessingException {

        RemoveGraph op = new Builder()
                .setGraphId(EXPECTED_GRAPH_ID)
                .build();

        byte[] serialise = toJson(op);
        RemoveGraph deserialise = fromJson(serialise);

        Assert.assertEquals(EXPECTED_GRAPH_ID, deserialise.getGraphId());
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("graphId");
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        RemoveGraph op = new Builder()
                .setGraphId(EXPECTED_GRAPH_ID)
                .build();

        Assert.assertEquals(EXPECTED_GRAPH_ID, op.getGraphId());
    }

    @Override
    public void shouldShallowCloneOperation() {
        final RemoveGraph a = getTestObject();
        final RemoveGraph b = a.shallowClone();
        Assert.assertEquals(a.getGraphId(), b.getGraphId());
    }

    @Override
    protected RemoveGraph getTestObject() {
        return new RemoveGraph();
    }
}