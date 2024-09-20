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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetGraphCreatedTime;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GetGraphCreatedTimeHandlerTest {
    private final Store store = mock(Store.class);
    private final Context context = new Context(new User());
    private final GetGraphCreatedTimeHandler handler = new GetGraphCreatedTimeHandler();

    @Test
    void shouldReturnGraphCreatedTimeMap() throws OperationException {
        // Given
        GetGraphCreatedTime op = new GetGraphCreatedTime();

        // When
        Map<String, String> result = handler.doOperation(op, context, store);

        // Then
        assertThat(result).isInstanceOf(Map.class);
    }
}
