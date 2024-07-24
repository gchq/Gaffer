/*
 * Copyright 2017-2024 Crown Copyright
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

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getDefaultMergeFunction;

public class FederatedOperationTest extends FederationOperationTest<FederatedOperation> {
    private static final List<String> EXPECTED_GRAPH_IDS = Arrays.asList("testGraphID1", "testGraphID2");
    public static final String JSON = String.format("{%n" +
            "  \"class\" : \"uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation\",%n" +
            "  \"operation\" : {%n" +
            "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\"%n" +
            "  },%n" +
            "  \"mergeFunction\" : {%n" +
            "    \"class\" : \"uk.gov.gchq.gaffer.federatedstore.util.ConcatenateMergeFunction\"%n" +
            "  },%n" +
            "  \"graphIds\" : [ \"testGraphID1\", \"testGraphID2\" ]%n" +
            "}");

    @Override
    protected Set<String> getRequiredFields() {
        return Collections.singleton("payloadOperation");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        //given
        final FederatedOperation federatedOperation = getFederatedOperationForSerialisation();

        //then
        assertEquals(EXPECTED_GRAPH_IDS, federatedOperation.getGraphIds());
        assertEquals(getDefaultMergeFunction(), federatedOperation.getMergeFunction());
        try {
            assertEquals(new String(JSONSerialiser.serialise(new GetAdjacentIds.Builder().build())), new String(JSONSerialiser.serialise(federatedOperation.getUnClonedPayload())));
            assertEquals(JSON, new String(JSONSerialiser.serialise(federatedOperation, true)));
        } catch (SerialisationException e) {
            fail(e);
        }
    }

    private FederatedOperation getFederatedOperationForSerialisation() {
        return new FederatedOperation.Builder()
                .op(new GetAdjacentIds.Builder()
                        .build())
                .mergeFunction(getDefaultMergeFunction())
                .graphIds(EXPECTED_GRAPH_IDS)
                .build();
    }

    @Test
    public void shouldDeserialise() throws Exception {
        //given
        final FederatedOperation federatedOperation = getFederatedOperationForSerialisation();

        //when
        FederatedOperation deserialise = JSONSerialiser.deserialise(JSON, FederatedOperation.class);

        //then
        assertEquals(federatedOperation, deserialise);

    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        FederatedOperation a = new FederatedOperation.Builder()
                .op(new GetAdjacentIds.Builder()
                        .build())
                .graphIds(EXPECTED_GRAPH_IDS)
                .mergeFunction(getDefaultMergeFunction())
                .option("op1", "val1")
                .skipFailedFederatedExecution(false)
                .build();
        final FederatedOperation b = a.shallowClone();
        assertEquals(a, b);
    }

    @Test
    public void shouldNotExposePayloadOperation() {
        Operation originalOperation = new GetElements();
        FederatedOperation federatedOperation = new FederatedOperation.Builder()
                .op(originalOperation)
                .mergeFunction(getDefaultMergeFunction())
                .graphIds(EXPECTED_GRAPH_IDS)
                .build();

        Collection<Operation> result = federatedOperation.getOperations();
        assertThat(result)
            .hasSize(1)
            .first()
            .isEqualTo(originalOperation)
            .isNotSameAs(originalOperation);
    }

    @Override
    protected FederatedOperation getTestObject() {
        return new FederatedOperation();
    }
}
