/*
 * Copyright 2017-2021 Crown Copyright
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

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class FederatedOperationTest extends FederationOperationTest<FederatedOperation> {
    private static final String EXPECTED_GRAPH_ID = "testGraphID1,testGraphID2";
    public static final String JSON = "{\n" +
            "  \"class\" : \"uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation\",\n" +
            "  \"operation\" : {\n" +
            "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\"\n" +
            "  },\n" +
            "  \"mergeFunction\" : {\n" +
            "    \"class\" : \"uk.gov.gchq.koryphe.impl.function.IterableConcat\"\n" +
            "  },\n" +
            "  \"graphIds\" : \"testGraphID1,testGraphID2\",\n" +
            "  \"skipFailedFederatedExecution\" : false\n" +
            "}";

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("payloadOperation");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        //given
        final FederatedOperation federatedOperation = getFederatedOperationForSerialisation();

        //then
        assertEquals(EXPECTED_GRAPH_ID, federatedOperation.getGraphIdsCSV());
        assertEquals(new uk.gov.gchq.koryphe.impl.function.IterableConcat(), federatedOperation.getMergeFunction());
        try {
            assertEquals(new String(JSONSerialiser.serialise(new GetAdjacentIds.Builder().build())), new String(JSONSerialiser.serialise(federatedOperation.getPayloadOperation())));
            assertEquals(JSON, new String(JSONSerialiser.serialise(federatedOperation, true)));
        } catch (SerialisationException e) {
            fail(e);
        }
    }

    private FederatedOperation getFederatedOperationForSerialisation() {
        return new FederatedOperation.Builder()
                .op(new GetAdjacentIds.Builder()
                        .build())
                .mergeFunction((Function<Iterable, Object>) new uk.gov.gchq.koryphe.impl.function.IterableConcat())
                .graphIds(EXPECTED_GRAPH_ID)
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
                .graphIds(EXPECTED_GRAPH_ID)
                .mergeFunction((Function<Iterable, Object>) new uk.gov.gchq.koryphe.impl.function.IterableConcat())
                .option("op1","val1")
                .skipFailedFederatedExecution(false)
                .build();
        final FederatedOperation b = a.shallowClone();
        assertEquals(a, b);
    }

    @Override
    protected FederatedOperation getTestObject() {
        return new FederatedOperation();
    }
}
