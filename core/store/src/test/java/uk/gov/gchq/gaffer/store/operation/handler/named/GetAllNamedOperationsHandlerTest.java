/*
 * Copyright 2018-2020 Crown Copyright
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

import com.google.common.collect.Iterables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetAllNamedOperationsHandlerTest {
    private final NamedOperationCache cache = new NamedOperationCache();
    private final AddNamedOperationHandler addNamedOperationHandler = new AddNamedOperationHandler(cache);
    private final GetAllNamedOperationsHandler getAllNamedOperationsHandler = new GetAllNamedOperationsHandler(cache);
    private Context context = new Context(new User.Builder()
            .userId(User.UNKNOWN_USER_ID)
            .build());

    private final NamedOperationDetail expectedOperationDetailWithInputType = new NamedOperationDetail.Builder()
            .operationName("exampleOp")
            .inputType("uk.gov.gchq.gaffer.data.element.Element[]")
            .creatorId(User.UNKNOWN_USER_ID)
            .operationChain("{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.add.AddElements\",\"skipInvalidElements\":false,\"validate\":true}]}")
            .readers(new ArrayList<>())
            .writers(new ArrayList<>())
            .build();

    private final NamedOperationDetail expectedOperationDetailWithoutInputType = new NamedOperationDetail.Builder()
            .operationName("exampleOp")
            .inputType(null)
            .creatorId(User.UNKNOWN_USER_ID)
            .operationChain("{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.store.operation.GetSchema\",\"compact\":false}]}")
            .readers(new ArrayList<>())
            .writers(new ArrayList<>())
            .build();

    private Store store = mock(Store.class);

    @AfterAll
    public static void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @BeforeEach
    public void before() {
        given(store.getProperties()).willReturn(new StoreProperties());
        StoreProperties properties = new StoreProperties();
        properties.set("gaffer.cache.service.class", "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService");
        CacheServiceLoader.initialise(properties.getProperties());
    }

    @Test
    public void shouldReturnLabelWhenNamedOperationHasLabel() throws Exception {
        final AddNamedOperation addNamedOperationWithLabel = new AddNamedOperation.Builder()
                .name("My Operation With Label")
                .labels(Arrays.asList("test label"))
                .operationChain("{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.add.AddElements\",\"skipInvalidElements\":false,\"validate\":true}]}")
                .build();
        addNamedOperationHandler.doOperation(addNamedOperationWithLabel, context, store);

        final CloseableIterable<NamedOperationDetail> allNamedOperations = getAllNamedOperationsHandler.doOperation(new GetAllNamedOperations(), context, store);

        assertEquals(Arrays.asList("test label"), allNamedOperations.iterator().next().getLabels());
    }

    @Test
    public void shouldReturnNullLabelWhenLabelIsNullFromAddNamedOperationRequest() throws Exception {
        final AddNamedOperation addNamedOperationWithNullLabel = new AddNamedOperation.Builder()
                .name("My Operation With Label")
                .labels(null)
                .operationChain("{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.add.AddElements\",\"skipInvalidElements\":false,\"validate\":true}]}")
                .build();
        addNamedOperationHandler.doOperation(addNamedOperationWithNullLabel, context, store);

        final CloseableIterable<NamedOperationDetail> allNamedOperations = getAllNamedOperationsHandler.doOperation(new GetAllNamedOperations(), context, store);

        assertEquals(null, allNamedOperations.iterator().next().getLabels());
    }

    @Test
    public void shouldReturnNamedOperationWithInputType() throws Exception {
        // Given
        AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .name(expectedOperationDetailWithInputType.getOperationName())
                .description(expectedOperationDetailWithInputType.getDescription())
                .operationChain(expectedOperationDetailWithInputType.getOperationChainWithDefaultParams())
                .build();

        addNamedOperationHandler.doOperation(addNamedOperation, context, store);

        // When
        CloseableIterable<NamedOperationDetail> allNamedOperationsList = getAllNamedOperationsHandler.doOperation(new GetAllNamedOperations(), context, store);

        // Then
        assertEquals(1, Iterables.size(allNamedOperationsList));
        assertTrue(Iterables.contains(allNamedOperationsList, expectedOperationDetailWithInputType));
    }

    @Test
    public void shouldReturnNamedOperationWithNoInputType() throws Exception {
        // Given
        AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .name(expectedOperationDetailWithoutInputType.getOperationName())
                .description(expectedOperationDetailWithoutInputType.getDescription())
                .operationChain(expectedOperationDetailWithoutInputType.getOperationChainWithDefaultParams())
                .build();

        addNamedOperationHandler.doOperation(addNamedOperation, context, store);

        // When
        CloseableIterable<NamedOperationDetail> allNamedOperationsList = getAllNamedOperationsHandler.doOperation(new GetAllNamedOperations(), context, store);

        // Then
        assertEquals(1, Iterables.size(allNamedOperationsList));
        assertTrue(Iterables.contains(allNamedOperationsList, expectedOperationDetailWithoutInputType));
    }
}
