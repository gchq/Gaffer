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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class NamedOperationCacheIT {
    private static final String CACHE_NAME = "NamedOperation";
    private static final String SUFFIX = "suffix";
    private static final Store STORE = mock(Store.class);
    private static final String ADMIN_AUTH = "admin auth";
    private static final StoreProperties PROPERTIES = new StoreProperties();

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
    private User adminAuthUser = new User.Builder().userId("adminAuthUser").opAuth(ADMIN_AUTH).build();
    private Context context = new Context(user);
    private GetAllNamedOperationsHandler getAllNamedOperationsHandler = new GetAllNamedOperationsHandler(SUFFIX);
    private AddNamedOperationHandler addNamedOperationHandler = new AddNamedOperationHandler(SUFFIX, true);
    private GetAllNamedOperationsHandler getAllNamedOperationsHandler1 = new GetAllNamedOperationsHandler(SUFFIX);
    private DeleteNamedOperationHandler deleteNamedOperationHandler = new DeleteNamedOperationHandler(SUFFIX);
    private GetAllNamedOperations get = new GetAllNamedOperations();

    @BeforeAll
    public static void before() throws CacheOperationException {
        PROPERTIES.setAdminAuth(ADMIN_AUTH);
        CacheServiceLoader.initialise(HashMapCacheService.class.getCanonicalName());
        CacheServiceLoader.getDefaultService().clearCache(CACHE_NAME);
    }

    @AfterEach
    public void after() throws CacheOperationException {
        CacheServiceLoader.getDefaultService().clearCache(CACHE_NAME);
        CacheServiceLoader.getDefaultService().clearCache(NamedOperationCache.getCacheNameFrom(SUFFIX));
    }

    @Test
    public void shouldBeAbleToAddNamedOperationToCache() throws OperationException {
        // given
        given(STORE.getProperties()).willReturn(PROPERTIES);

        // when
        addNamedOperationHandler.doOperation(add, context, STORE);

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
        getAllNamedOperationsHandler.doOperation(get, context, STORE).forEach(results::add);

        // then
        assertThat(results)
                .hasSize(1)
                .isEqualTo(expected);
    }

    @Test
    public void shouldBeAbleToDeleteNamedOperationFromCache() throws OperationException {
        // given
        given(STORE.getProperties()).willReturn(PROPERTIES);

        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, STORE);

        DeleteNamedOperation del = new DeleteNamedOperation.Builder()
                .name("op")
                .build();

        // when
        deleteNamedOperationHandler.doOperation(del, context, STORE);

        List<NamedOperationDetail> results = new ArrayList<>();
        getAllNamedOperationsHandler1.doOperation(get, context, STORE).forEach(results::add);

        // then
        assertThat(results).isEmpty();
    }

    @Test
    public void shouldAllowUpdatingOfNamedOperations() throws OperationException {
        // given
        final StoreProperties storeProps = mock(StoreProperties.class);
        given(STORE.getProperties()).willReturn(storeProps);

        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, STORE);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChainAsString())
                .overwrite()
                .score(0)
                .build();

        // when
        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, STORE);

        List<NamedOperationDetail> results = new ArrayList<>();
        getAllNamedOperationsHandler.doOperation(get, context, STORE).forEach(results::add);

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChainAsString())
                .description(update.getDescription())
                .creatorId(user.getUserId())
                .score(0)
                .parameters(null)
                .build();

        List<NamedOperationDetail> expected = Arrays.asList(expectedNamedOp);

        // then
        assertThat(results)
                .hasSameSizeAs(expected)
                .isEqualTo(expected);
    }

    @Test
    public void shouldAllowUpdatingOfNamedOperationsWithAllowedUsers() throws OperationException {
        // given
        given(STORE.getProperties()).willReturn(PROPERTIES);

        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, STORE);

        AddNamedOperation update = new AddNamedOperation.Builder()
                .name(add.getOperationName())
                .description("a different operation")
                .operationChain(add.getOperationChainAsString())
                .overwrite()
                .score(0)
                .build();

        // when
        new AddNamedOperationHandler(SUFFIX, true).doOperation(add, context, STORE);

        List<NamedOperationDetail> results = new ArrayList<>();
        getAllNamedOperationsHandler.doOperation(get, context, STORE).forEach(results::add);

        NamedOperationDetail expectedNamedOp = new NamedOperationDetail.Builder()
                .operationName(update.getOperationName())
                .operationChain(update.getOperationChainAsString())
                .description(update.getDescription())
                .creatorId(user.getUserId())
                .score(0)
                .parameters(null)
                .build();

        List<NamedOperationDetail> expected = Arrays.asList(expectedNamedOp);

        // then
        assertThat(results)
                .hasSameSizeAs(expected)
                .isEqualTo(expected);
    }

    @Test
    public void shouldAllowReadingOfNamedOperationsUsingAdminAuth() throws OperationException {
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

        List<NamedOperationDetail> expected = Arrays.asList(expectedNamedOp);

        addNamedOperationHandler.doOperation(add, contextWithAuthorisedUser, STORE);

        // when
        List<NamedOperationDetail> resultsWithNoAdminRole = new ArrayList<>();
        for (NamedOperationDetail detail : getAllNamedOperationsHandler.doOperation(get, context, STORE)) {
            resultsWithNoAdminRole.add(detail);
        }

        // then
        assertThat(resultsWithNoAdminRole).isEmpty();

        // when
        List<NamedOperationDetail> resultsWithAdminRole = new ArrayList<>();
        getAllNamedOperationsHandler.doOperation(get, contextWithAdminUser, STORE).forEach(resultsWithAdminRole::add);

        // then
        assertThat(resultsWithAdminRole)
                .hasSize(1)
                .isEqualTo(expected);
    }

    @Test
    public void shouldAllowUpdatingOfNamedOperationsUsingAdminAuth() throws OperationException {
        // given
        Context contextWithAuthorisedUser = new Context(authorisedUser);
        Context contextWithAdminUser = new Context(adminAuthUser);
        addNamedOperationHandler.doOperation(add, contextWithAuthorisedUser, STORE);

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

        List<NamedOperationDetail> expected = Arrays.asList(expectedNamedOp);

        // when / then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> addNamedOperationHandler.doOperation(update, context, STORE))
                .withMessageContaining("User UNKNOWN does not have permission to overwrite");


        // when
        addNamedOperationHandler.doOperation(update, contextWithAdminUser, STORE);

        List<NamedOperationDetail> results = new ArrayList<>();
        getAllNamedOperationsHandler.doOperation(get, contextWithAdminUser, STORE).forEach(results::add);

        // then
        assertThat(results)
                .hasSameSizeAs(expected)
                .isEqualTo(expected);
    }
}
