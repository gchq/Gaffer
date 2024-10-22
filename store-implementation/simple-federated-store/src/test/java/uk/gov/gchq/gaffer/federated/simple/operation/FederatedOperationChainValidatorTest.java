/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.operation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FederatedOperationChainValidatorTest {
    final ViewValidator viewValidator = mock(ViewValidator.class);
    final FederatedOperationChainValidator validator = new FederatedOperationChainValidator(viewValidator);
    final FederatedStore store = mock(FederatedStore.class);
    final User user = mock(User.class);
    final Operation op = mock(Operation.class);
    final Schema schema = mock(Schema.class);

    @Test
    void shouldGetMergedSchema() throws OperationException {
        // Given
        when(store.execute(any(GetSchema.class), any(Context.class))).thenReturn(schema);

        // When
        final Schema result = validator.getSchema(op, user, store);

        verify(store).execute(any(GetSchema.class), any(Context.class));
        // Then
        assertThat(result).isEqualTo(schema);
    }

    @Test
    void shouldThrowException() throws OperationException {
        // Given
        when(store.execute(any(GetSchema.class), any(Context.class))).thenThrow(GafferRuntimeException.class);

        // When/Then
        try {
            validator.getSchema(op, user, store);
            fail("Exception expected");
        } catch (final Exception e) {
            verify(store).execute(any(GetSchema.class), any(Context.class));
            assertThat(e).isInstanceOf(GafferRuntimeException.class);
        }
    }
}
