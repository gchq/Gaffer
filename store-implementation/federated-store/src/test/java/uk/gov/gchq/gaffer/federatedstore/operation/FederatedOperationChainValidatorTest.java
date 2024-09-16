/*
 * Copyright 2017-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

public class FederatedOperationChainValidatorTest {
    @Test
    public void shouldGetFederatedSchema() throws OperationException {
        // Given
        final ViewValidator viewValidator = mock(FederatedViewValidator.class);
        final FederatedOperationChainValidator validator = new FederatedOperationChainValidator(viewValidator);
        final FederatedStore store = mock(FederatedStore.class);
        final User user = mock(User.class);
        final Operation op = mock(Operation.class);
        final Schema schema = mock(Schema.class);
        given(store.execute(any(GetSchema.class), any(Context.class))).willReturn(schema);

        // When
        final Schema actualSchema = validator.getSchema(op, user, store);

        verify(store).execute(any(GetSchema.class), any(Context.class));
        // Then
        assertEquals(schema, actualSchema);
    }

    @Test
    public void shouldNotErrorWithInvalidViewFromMissingGraph() {
        //given
        String missingGraph = "missingGraph";
        final Graph graph = new Graph.Builder()
                .addStoreProperties(new FederatedStoreProperties())
                .config(new GraphConfig.Builder().graphId("testFedGraph").build())
                .build();

        //when
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> graph.execute(getFederatedOperation(
                        new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .entity("missingEntity")
                                        .build())
                                .build())
                        .graphIdsCSV(missingGraph), new Context()))
                .withStackTraceContaining(String.format(FederatedGraphStorage.GRAPH_IDS_NOT_VISIBLE, singletonList(missingGraph)));
    }

}
