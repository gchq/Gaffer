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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.schema.FederatedViewValidator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedWrappedSchema;

public class FederatedOperationChainValidatorTest {
    @Test
    public void shouldGetFederatedSchema() {
        // Given
        final ViewValidator viewValidator = mock(FederatedViewValidator.class);
        final FederatedOperationChainValidator validator = new FederatedOperationChainValidator(viewValidator);
        final FederatedStore store = mock(FederatedStore.class);
        final User user = mock(User.class);
        final Operation op = mock(Operation.class);
        final Schema schema = mock(Schema.class);
        given(store.getSchema(eq(getFederatedWrappedSchema()), any(Context.class))).willReturn(schema);

        // When
        final Schema actualSchema = validator.getSchema(op, user, store);

        verify(store).getSchema(eq(getFederatedWrappedSchema()), any(Context.class));
        // Then
        assertEquals(schema, actualSchema);
    }

    @Test
    public void shouldNotErrorWithInvalidViewFromMissingGraph() throws OperationException {
        //given
        String missingGraph = "missingGraph";
        final Graph graph = new Graph.Builder()
                .addStoreProperties(new FederatedStoreProperties())
                .config(new GraphConfig.Builder().graphId("testFedGraph").build())
                .build();

        try {
            //when
            graph.execute(getFederatedOperation(
                    new GetAllElements.Builder()
                            .view(new View.Builder()
                                    .entity("missingEntity")
                                    .build())
                            .build())
                    .graphIdsCSV(missingGraph), new Context());
            fail("exception expected");
        } catch (final Exception e) {
            //then
            assertEquals(String.format(FederatedGraphStorage.GRAPH_IDS_NOT_VISIBLE, Lists.newArrayList(missingGraph)), e.getMessage());
        }

    }
}
