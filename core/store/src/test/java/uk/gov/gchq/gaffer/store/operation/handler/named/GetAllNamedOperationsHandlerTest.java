/*
 * Copyright 2018-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.named;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class GetAllNamedOperationsHandlerTest {

    private final NamedOperationCache cache = new NamedOperationCache("Suffix");
    private final AddNamedOperationHandler addNamedOperationHandler = new AddNamedOperationHandler(cache, true);
    private final GetAllNamedOperationsHandler getAllNamedOperationsHandler = new GetAllNamedOperationsHandler(cache);
    private final Context context = new Context(new User.Builder()
            .userId(User.UNKNOWN_USER_ID)
            .build());

    private final NamedOperationDetail expectedOperationDetailWithInputType = new NamedOperationDetail.Builder()
            .operationName("exampleOp")
            .inputType("uk.gov.gchq.gaffer.data.element.Element[]")
            .creatorId(User.UNKNOWN_USER_ID)
            .operationChain(
                    "{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.add.AddElements\",\"skipInvalidElements\":false,\"validate\":true}]}")
            .parameters(null)
            .build();

    private final NamedOperationDetail expectedOperationDetailWithoutInputType = new NamedOperationDetail.Builder()
            .operationName("exampleOp")
            .inputType(null)
            .creatorId(User.UNKNOWN_USER_ID)
            .operationChain("{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.store.operation.GetSchema\",\"compact\":false}]}")
            .parameters(null)
            .build();

    @Mock
    private Store store;

    @AfterEach
    public void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @BeforeEach
    public void before() {
        given(store.getProperties()).willReturn(new StoreProperties());
        CacheServiceLoader.initialise("uk.gov.gchq.gaffer.cache.impl.HashMapCacheService");
    }

    @Test
    public void shouldReturnLabelWhenNamedOperationHasLabel() throws Exception {
        final AddNamedOperation addNamedOperationWithLabel = new AddNamedOperation.Builder()
                .name("My Operation With Label")
                .labels(Arrays.asList("test label"))
                .operationChain(
                        "{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.add.AddElements\",\"skipInvalidElements\":false,\"validate\":true}]}")
                .build();
        addNamedOperationHandler.doOperation(addNamedOperationWithLabel, context, store);

        final Iterable<NamedOperationDetail> allNamedOperations =
                getAllNamedOperationsHandler.doOperation(new GetAllNamedOperations(), context, store);

        assertThat(allNamedOperations.iterator().next().getLabels()).containsExactly("test label");
    }

    @Test
    public void shouldReturnNullLabelWhenLabelIsNullFromAddNamedOperationRequest() throws Exception {
        final AddNamedOperation addNamedOperationWithNullLabel = new AddNamedOperation.Builder()
                .name("My Operation With Label")
                .labels(null)
                .operationChain(
                        "{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.add.AddElements\",\"skipInvalidElements\":false,\"validate\":true}]}")
                .build();
        addNamedOperationHandler.doOperation(addNamedOperationWithNullLabel, context, store);

        final Iterable<NamedOperationDetail> allNamedOperations =
                getAllNamedOperationsHandler.doOperation(new GetAllNamedOperations(), context, store);

        assertThat(allNamedOperations.iterator().next().getLabels()).isNull();
    }

    @Test
    public void shouldReturnNamedOperationWithInputType() throws Exception {
        // Given
        final AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .name(expectedOperationDetailWithInputType.getOperationName())
                .description(expectedOperationDetailWithInputType.getDescription())
                .operationChain(expectedOperationDetailWithInputType.getOperationChainWithDefaultParams())
                .overwrite(true)
                .build();

        addNamedOperationHandler.doOperation(addNamedOperation, context, store);

        // When
        final Iterable<NamedOperationDetail> allNamedOperationsList =
                getAllNamedOperationsHandler.doOperation(new GetAllNamedOperations(), context, store);

        // Then
        assertThat(allNamedOperationsList)
                .hasSize(1)
                .contains(expectedOperationDetailWithInputType);
    }

    @Test
    public void shouldReturnNamedOperationWithNoInputType() throws Exception {
        // Given
        final AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .name(expectedOperationDetailWithoutInputType.getOperationName())
                .description(expectedOperationDetailWithoutInputType.getDescription())
                .operationChain(expectedOperationDetailWithoutInputType.getOperationChainWithDefaultParams())
                .build();

        addNamedOperationHandler.doOperation(addNamedOperation, context, store);

        // When
        final Iterable<NamedOperationDetail> allNamedOperationsList =
                getAllNamedOperationsHandler.doOperation(new GetAllNamedOperations(), context, store);

        // Then
        assertThat(allNamedOperationsList)
                .hasSize(1)
                .contains(expectedOperationDetailWithoutInputType);
    }
}
