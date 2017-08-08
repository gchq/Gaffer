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
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.Set;

public class GetAllGraphIDTest extends OperationTest {


    @Override
    protected Class<? extends Operation> getOperationClass() {
        return GetAllGraphID.class;
    }

    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException, JsonProcessingException {
        GetAllGraphID op = new GetAllGraphID.Builder()
                .build();

        byte[] serialise = JSON_SERIALISER.serialise(op, true);
        GetAllGraphID deserialise = JSON_SERIALISER.deserialise(serialise, GetAllGraphID.class);
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet();
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetAllGraphID op = new GetAllGraphID.Builder()
                .build();
    }
}