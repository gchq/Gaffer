/*
 * Copyright 2017-2024 Crown Copyright
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

import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
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
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class NamedOperationCacheIT {
    private static final String CACHE_NAME = "NamedOperation";
    public static final String SUFFIX = "suffix";
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
    private GetAllNamedOperationsHandler getAllNamedOperationsHandler = new GetAllNamedOperationsHandler(SUFFIX);
    private AddNamedOperationHandler addNamedOperationHandler = new AddNamedOperationHandler(SUFFIX, true);
    private GetAllNamedOperationsHandler getAllNamedOperationsHandler1 = new GetAllNamedOperationsHandler(SUFFIX);
    private DeleteNamedOperationHandler deleteNamedOperationHandler = new DeleteNamedOperationHandler(SUFFIX);
    private GetAllNamedOperations get = new GetAllNamedOperations();

    @BeforeEach
    public void before() throws CacheOperationException {
        properties.setAdminAuth(adminAuth);
        given(store.getProperties()).willReturn(properties);
    }

    @AfterEach
    public void after() throws CacheOperationException {
        CacheServiceLoader.getDefaultService().clearCache(CACHE_NAME);
        CacheServiceLoader.getDefaultService().clearCache(NamedOperationCache.getCacheNameFrom(SUFFIX));
    }

    @Test
    public void shouldWorkUsingHashMapServiceClass() throws OperationException, CacheOperationException {
        reInitialiseCacheService(HashMapCacheService.class);
        runTests();
    }

    private void reInitialiseCacheService(final Class clazz) throws CacheOperationException {
        CacheServiceLoader.initialise(clazz.getCanonicalName());
        CacheServiceLoader.getDefaultService().clearCache(CACHE_NAME);
    }

    @Test
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

    @Test
    public List<NamedOperationDetail> shouldBeAbleToAddNamedOperationToCache() throws OperationException {
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
                .description(add.getDescription())
                .score(0)
                .parameters(null)
                .build();

        List<NamedOperationDetail> expected = Arrays.asList(expectedNamedOp);
        List<NamedOperationDetail> results = new ArrayList<>();
        for (NamedOperationDetail detail: getAllNamedOperationsHandler.doOperation(get, context, store)) {
            results.add(detail);
        }

        // then
        assertThat(results)
                .hasSize(1)
                .isEqualTo(expected);

        return results;
    }

    @Test
    public List<NamedOperationDetail> shouldBeAbleToDeleteNamedOperationFromCache() throws OperationException {
        // given
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(properties);

        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, store);

        DeleteNamedOperation del = new DeleteNamedOperation.Builder()
                .name("op")
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        deleteNamedOperationHandler.doOperation(del, context, store);

        List<NamedOperationDetail> results = new ArrayList<>();
        for (NamedOperationDetail detail : getAllNamedOperationsHandler1.doOperation(get, context, store)) {
            results.add(detail);
        }

        // then
        assertThat(results).isEmpty();

        return results;
    }

    @Test
    public List<NamedOperationDetail> shouldAllowUpdatingOfNamedOperations() throws OperationException {
        // given
        final Store store = mock(Store.class);
        final StoreProperties storeProps = mock(StoreProperties.class);
        given(store.getProperties()).willReturn(storeProps);

        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, store);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChainAsString())
                .overwrite()
                .score(0)
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, store);

        List<NamedOperationDetail> results = new ArrayList<>();
        for (NamedOperationDetail detail : getAllNamedOperationsHandler.doOperation(get, context, store)) {
            results.add(detail);
        }

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChainAsString())
                .description(update.getDescription())
                .creatorId(user.getUserId())
                .score(0)
                .parameters(null)
                .build();

        List<NamedOperationDetail> expected = new ArrayList<>();
        expected.add(expectedNamedOp);

        // then
        assertThat(results)
                .hasSameSizeAs(expected)
                .isEqualTo(expected);

        return results;
    }

    @Test
    public List<NamedOperationDetail> shouldAllowUpdatingOfNamedOperationsWithAllowedUsers() throws OperationException {
        // given
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(properties);

        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, store);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChainAsString())
                .overwrite()
                .score(0)
                .build();

        GetAllNamedOperations get = new GetAllNamedOperations();

        // when
        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, store);

        List<NamedOperationDetail> results = new ArrayList<>();
        for (NamedOperationDetail detail : getAllNamedOperationsHandler.doOperation(get, context, store)) {
            results.add(detail);
        }

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChainAsString())
                .description(update.getDescription())
                .creatorId(user.getUserId())
                .score(0)
                .parameters(null)
                .build();

        List<NamedOperationDetail> expected = new ArrayList<>();
        expected.add(expectedNamedOp);

        // then
        assertThat(results)
                .hasSameSizeAs(expected)
                .isEqualTo(expected);

        return results;
    }

    @Test
    public List<NamedOperationDetail> shouldAllowReadingOfNamedOperationsUsingAdminAuth() throws OperationException {
        // given
        Context contextWithAuthorisedUser = new Context(authorisedUser);
        Context contextWithAdminUser = new Context(adminAuthUser);
        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(add.getOperationName())
                .operationChain(add.getOperationChainAsString())
                .description(add.getDescription())
                .creatorId(authorisedUser.getUserId())
                .score(0)
                .parameters(null)
                .build();

        List<NamedOperationDetail> expected = new ArrayList<>();
        expected.add(expectedNamedOp);

        addNamedOperationHandler.doOperation(add, contextWithAuthorisedUser, store);

        // when
        List<NamedOperationDetail> resultsWithNoAdminRole = new ArrayList<>();
        for (NamedOperationDetail detail : getAllNamedOperationsHandler.doOperation(get, context, store)) {
            resultsWithNoAdminRole.add(detail);
        }

        // then
        assertThat(resultsWithNoAdminRole).isEmpty();

        // when
        List<NamedOperationDetail> resultsWithAdminRole = new ArrayList<>();
        for (NamedOperationDetail detail : getAllNamedOperationsHandler.doOperation(get, contextWithAdminUser, store)) {
            resultsWithAdminRole.add(detail);
        }

        // then
        assertThat(resultsWithAdminRole)
                .hasSize(1)
                .isEqualTo(expected);

        return resultsWithAdminRole;
    }

    @Test
    public List<NamedOperationDetail> shouldAllowUpdatingOfNamedOperationsUsingAdminAuth() throws OperationException {
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
                .score(0)
                .parameters(null)
                .build();

        List<NamedOperationDetail> expected = new ArrayList<>();
        expected.add(expectedNamedOp);

        // when / then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> addNamedOperationHandler.doOperation(update, context, store))
                .withMessageContaining("User UNKNOWN does not have permission to overwrite");


        // when
        addNamedOperationHandler.doOperation(update, contextWithAdminUser, store);

        List<NamedOperationDetail> results = new ArrayList<>();
        for (NamedOperationDetail detail : getAllNamedOperationsHandler.doOperation(get, contextWithAdminUser, store)) {
            results.add(detail);
        }

        // then
        assertThat(results)
                .hasSameSizeAs(expected)
                .isEqualTo(expected);

        return results;
    }
}
