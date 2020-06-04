/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.operation.named.cache;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedOperationsHandler;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class NamedOperationCacheIT {

    private static final String CACHE_NAME = "NamedOperation";
    private final Properties cacheProps = new Properties();
    private final Store store = mock(Store.class);
    private final String adminAuth = "admin auth";
    private final StoreProperties properties = new StoreProperties();

    private AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
            .name("op")
            .description("test operation")
            .operationChain(new OperationChain.Builder()
                    .first(new GetAllElements.Builder()
                            .build())
                    .build())
            .overwrite()
            .score(0)
            .build();

    private User user = new User();
    private User authorisedUser = new User.Builder().userId("authorisedUser").opAuth("authorised").build();
    private User adminAuthUser = new User.Builder().userId("adminAuthUser").opAuth(adminAuth).build();
    private Context context = new Context(user);
    private GetAllNamedOperationsHandler getAllNamedOperationsHandler = new GetAllNamedOperationsHandler();
    private AddNamedOperationHandler addNamedOperationHandler = new AddNamedOperationHandler();
    private GetAllNamedOperationsHandler getAllNamedOperationsHandler1 = new GetAllNamedOperationsHandler();
    private DeleteNamedOperationHandler deleteNamedOperationHandler = new DeleteNamedOperationHandler();
    private GetAllNamedOperations get = new GetAllNamedOperations();

    @BeforeEach
    public void before() throws CacheOperationException {
        cacheProps.clear();
        properties.setAdminAuth(adminAuth);
        given(store.getProperties()).willReturn(properties);
        initialiseHashMapCacheService();
    }

    @AfterEach
    public void after() throws CacheOperationException {
        CacheServiceLoader.getService().clearCache(CACHE_NAME);
    }

    @Test
    public void shouldWorkUsingHashMapServiceClass() throws CacheOperationException {
        initialiseHashMapCacheService();
    }

    @Test
    public void shouldBeAbleToAddNamedOperationToCache() throws OperationException {
        // given
        GetAllNamedOperations get = new GetAllNamedOperations.Builder().build();
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(properties);

        // when
        addNamedOperationHandler.doOperation(addNamedOperation, context, store);

        List<NamedOperationDetail> results = Lists.newArrayList(new GetAllNamedOperationsHandler().doOperation(get, context, store));

        // then
        final NamedOperationDetail expectedNamedOp = makeNamedOperationDetail(addNamedOperation, user);
        final List<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        assertEquals(1, results.size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldBeAbleToDeleteNamedOperationFromCache() throws OperationException {
        // given
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(properties);
        new AddNamedOperationHandler().doOperation(addNamedOperation, context, store);

        final DeleteNamedOperation deleteNamedOperation = new DeleteNamedOperation.Builder()
                .name("op")
                .build();

        // when
        deleteNamedOperationHandler.doOperation(deleteNamedOperation, context, store);

        final GetAllNamedOperations getAllNamedOperations = new GetAllNamedOperations();
        final List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler1.doOperation(getAllNamedOperations, context, store));

        // then
        assertEquals(0, results.size());
    }

    @Test
    public void shouldAllowUpdatingOfNamedOperations() throws OperationException {
        // Given
        final Store store = mock(Store.class);
        final StoreProperties storeProps = mock(StoreProperties.class);
        given(store.getProperties()).willReturn(storeProps);

        new AddNamedOperationHandler().doOperation(addNamedOperation, context, store);

        GetAllNamedOperations get = new GetAllNamedOperations();

        // When
        new AddNamedOperationHandler().doOperation(addNamedOperation, context, store);
        final List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, context, store));

        // Then
        final AddNamedOperation updatedNamedOperation = makeUpdatedAddNamedOperation();
        final NamedOperationDetail expectedNamedOp = makeNamedOperationDetail(updatedNamedOperation, user);
        final ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        assertEquals(expected.size(), results.size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldAllowUpdatingOfNamedOperationsWithAllowedUsers() throws OperationException {
        // given
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(properties);

        new AddNamedOperationHandler().doOperation(addNamedOperation, context, store);

        AddNamedOperation update = makeUpdatedAddNamedOperation();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        new AddNamedOperationHandler().doOperation(addNamedOperation, context, store);

        List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, context, store));

        // Then
        final NamedOperationDetail expectedNamedOp = makeNamedOperationDetail(update, user);
        final ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        assertEquals(expected.size(), results.size());
        assertEquals(expected, results);
    }

    @Test
    public void shouldAllowReadingOfNamedOperationsUsingAdminAuth() throws OperationException {
        // given
        Context contextWithAuthorisedUser = new Context(authorisedUser);
        Context contextWithAdminUser = new Context(adminAuthUser);
        NamedOperationDetail expectedNamedOp = makeNamedOperationDetail(addNamedOperation, authorisedUser);

        addNamedOperationHandler.doOperation(addNamedOperation, contextWithAuthorisedUser, store);

        // when
        List<NamedOperationDetail> resultsWithNoAdminRole = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, context, store));

        // then
        assertEquals(0, resultsWithNoAdminRole.size());

        // when
        List<NamedOperationDetail> resultsWithAdminRole = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, contextWithAdminUser, store));

        // then
        final ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        assertEquals(1, resultsWithAdminRole.size());
        assertEquals(expected, resultsWithAdminRole);
    }

    @Test
    public void shouldAllowUpdatingOfNamedOperationsUsingAdminAuth() throws OperationException {
        // Given
        Context contextWithAuthorisedUser = new Context(authorisedUser);
        Context contextWithAdminUser = new Context(adminAuthUser);
        addNamedOperationHandler.doOperation(addNamedOperation, contextWithAuthorisedUser, store);

        AddNamedOperation updatedNamedOperation = makeUpdatedAddNamedOperation();

        // When / Then
        final Exception exception = assertThrows(OperationException.class, () -> addNamedOperationHandler.doOperation(updatedNamedOperation, context, store));
        assertEquals("User UNKNOWN does not have permission to overwrite", exception.getMessage());

        // When
        addNamedOperationHandler.doOperation(updatedNamedOperation, contextWithAdminUser, store);

        List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, contextWithAdminUser, store));

        // Then
        final NamedOperationDetail expectedNamedOp = makeNamedOperationDetail(updatedNamedOperation, adminAuthUser);
        final ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        assertEquals(expected.size(), results.size());
        assertEquals(expected, results);
    }

    private NamedOperationDetail makeNamedOperationDetail(final AddNamedOperation addNamedOperation, final User user) {
        return new NamedOperationDetail.Builder()
                .operationName(addNamedOperation.getOperationName())
                .operationChain(addNamedOperation.getOperationChainAsString())
                .description(addNamedOperation.getDescription())
                .creatorId(user.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .score(0)
                .build();
    }

    private AddNamedOperation makeUpdatedAddNamedOperation() {
        return new AddNamedOperation.Builder()
                .name(addNamedOperation.getOperationName())
                .description("a different operation")
                .operationChain(addNamedOperation.getOperationChainAsString())
                .overwrite()
                .score(0)
                .build();
    }

    private void initialiseHashMapCacheService() throws CacheOperationException {
        cacheProps.setProperty(CacheProperties.CACHE_SERVICE_CLASS, HashMapCacheService.class.getCanonicalName());
        CacheServiceLoader.initialise(cacheProps);
        CacheServiceLoader.getService().clearCache(CACHE_NAME);
    }
}
