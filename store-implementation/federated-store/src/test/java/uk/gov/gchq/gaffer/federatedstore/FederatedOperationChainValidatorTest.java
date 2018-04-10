/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class FederatedOperationChainValidatorTest {
    @Test
    public void shouldGetFederatedSchema() {
        // Given
        final ViewValidator viewValidator = mock(ViewValidator.class);
        final FederatedOperationChainValidator validator = new FederatedOperationChainValidator(viewValidator);
        final FederatedStore store = mock(FederatedStore.class);
        final User user = mock(User.class);
        final Operation op = mock(Operation.class);
        final Schema schema = mock(Schema.class);
        given(store.getSchema(op, user)).willReturn(schema);

        // When
        final Schema actualSchema = validator.getSchema(op, user, store);

        // Then
        assertSame(schema, actualSchema);
    }
}
