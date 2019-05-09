/*
 * Copyright 2017-2019 Crown Copyright
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class NamedOperationCacheIT {
    private static final String CACHE_NAME = "NamedOperation";
    private final Properties cacheProps = new Properties();
    private final Store store = mock(Store.class);
    private final String adminAuth = "admin auth";
    private final StoreProperties properties = new StoreProperties();

    private AddNamedOperation add = new AddNamedOperation.Builder()
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

    @Before
    public void before() throws CacheOperationException {
        cacheProps.clear();
        properties.setAdminAuth(adminAuth);
        given(store.getProperties()).willReturn(properties);
    }

    @After
    public void after() throws CacheOperationException {
        CacheServiceLoader.getService().clearCache(CACHE_NAME);
    }

    @Test
    public void shouldWorkUsingHashMapServiceClass() throws OperationException, CacheOperationException {
        reInitialiseCacheService(HashMapCacheService.class);
        runTests();
    }

    private void reInitialiseCacheService(final Class clazz) throws CacheOperationException {
        cacheProps.setProperty(CacheProperties.CACHE_SERVICE_CLASS, clazz.getCanonicalName());
        CacheServiceLoader.initialise(cacheProps);
        CacheServiceLoader.getService().clearCache(CACHE_NAME);
    }

    private void runTests() throws OperationException, CacheOperationException {
        shouldAllowUpdatingOfNamedOperations();
        after();
        shouldAllowUpdatingOfNamedOperationsWithAllowedUsers();
        after();
        shouldAllowReadingOfNamedOperationsUsingAdminAuth();
        after();
        shouldAllowUpdatingOfNamedOperationsUsingAdminAuth();
        after();
        shouldBeAbleToAddNamedOperationToCache();
        after();
        shouldBeAbleToDeleteNamedOperationFromCache();
    }


    private void shouldBeAbleToAddNamedOperationToCache() throws OperationException {
        // given
        GetAllNamedOperations get = new GetAllNamedOperations.Builder().build();
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(properties);

        // when
        addNamedOperationHandler.doOperation(add, context, store);

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(add.getOperationName())
                .operationChain(add.getOperationChainAsString())
                .creatorId(user.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .description(add.getDescription())
                .score(0)
                .build();

        List<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);
        List<NamedOperationDetail> results = Lists.newArrayList(new GetAllNamedOperationsHandler().doOperation(get, context, store));

        // then
        assertEquals(1, results.size());
        assertEquals(expected, results);
    }


    private void shouldBeAbleToDeleteNamedOperationFromCache() throws OperationException {
        // given
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(properties);

        new AddNamedOperationHandler().doOperation(add, context, store);

        DeleteNamedOperation del = new DeleteNamedOperation.Builder()
                .name("op")
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        deleteNamedOperationHandler.doOperation(del, context, store);

        List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler1.doOperation(get, context, store));

        // then
        assertEquals(0, results.size());

    }

    private void shouldAllowUpdatingOfNamedOperations() throws OperationException {
        // given
        final Store store = mock(Store.class);
        final StoreProperties storeProps = mock(StoreProperties.class);
        given(store.getProperties()).willReturn(storeProps);

        new AddNamedOperationHandler().doOperation(add, context, store);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChainAsString())
                .overwrite()
                .score(0)
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        new AddNamedOperationHandler().doOperation(add, context, store);

        List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, context, store));

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChainAsString())
                .description(update.getDescription())
                .creatorId(user.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .score(0)
                .build();

        ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);

        // then
        assertEquals(expected.size(), results.size());
        assertEquals(expected, results);
    }

    private void shouldAllowUpdatingOfNamedOperationsWithAllowedUsers() throws OperationException {
        // given
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(properties);

        new AddNamedOperationHandler().doOperation(add, context, store);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChainAsString())
                .overwrite()
                .score(0)
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        new AddNamedOperationHandler().doOperation(add, context, store);

        List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, context, store));

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChainAsString())
                .description(update.getDescription())
                .creatorId(user.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .score(0)
                .build();

        ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);

        // then
        assertEquals(expected.size(), results.size());
        assertEquals(expected, results);
    }

    private void shouldAllowReadingOfNamedOperationsUsingAdminAuth() throws OperationException {
        // given
        Context contextWithAuthorisedUser = new Context(authorisedUser);
        Context contextWithAdminUser = new Context(adminAuthUser);
        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(add.getOperationName())
                .operationChain(add.getOperationChainAsString())
                .description(add.getDescription())
                .creatorId(authorisedUser.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .score(0)
                .build();
        ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);

        addNamedOperationHandler.doOperation(add, contextWithAuthorisedUser, store);

        // when
        List<NamedOperationDetail> resultsWithNoAdminRole = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, context, store));

        // then
        assertEquals(0, resultsWithNoAdminRole.size());

        // when
        List<NamedOperationDetail> resultsWithAdminRole = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, contextWithAdminUser, store));

        // then
        assertEquals(1, resultsWithAdminRole.size());
        assertEquals(expected, resultsWithAdminRole);
    }

    private void shouldAllowUpdatingOfNamedOperationsUsingAdminAuth() throws OperationException {
        // given
        Context contextWithAuthorisedUser = new Context(authorisedUser);
        Context contextWithAdminUser = new Context(adminAuthUser);
        addNamedOperationHandler.doOperation(add, contextWithAuthorisedUser, store);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChainAsString())
                .overwrite()
                .score(0)
                .build();

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChainAsString())
                .description(update.getDescription())
                .creatorId(adminAuthUser.getUserId())
                .readers(new ArrayList<>())
                .writers(new ArrayList<>())
                .score(0)
                .build();

        ArrayList<NamedOperationDetail> expected = Lists.newArrayList(expectedNamedOp);

        // when / then
        try {
            addNamedOperationHandler.doOperation(update, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("User UNKNOWN does not have permission to overwrite"));
        }

        // when
        addNamedOperationHandler.doOperation(update, contextWithAdminUser, store);

        List<NamedOperationDetail> results = Lists.newArrayList(getAllNamedOperationsHandler.doOperation(get, contextWithAdminUser, store));

        // then
        assertEquals(expected.size(), results.size());
        assertEquals(expected, results);
    }
}
