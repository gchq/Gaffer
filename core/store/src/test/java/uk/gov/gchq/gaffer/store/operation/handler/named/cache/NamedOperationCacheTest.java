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

package uk.gov.gchq.gaffer.store.operation.handler.named.cache;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class NamedOperationCacheTest {

    public static final String SUFFIX_CACHE_NAME = "Suffix";
    private static NamedOperationCache cache;
    private static final String GAFFER_USER = "gaffer user";
    private static final String ADVANCED_GAFFER_USER = "advanced gaffer user";
    private static final String ADMIN_AUTH = "admin auth";
    private static final String EMPTY_ADMIN_AUTH = "";
    private final List<String> readers = Collections.singletonList(GAFFER_USER);
    private final List<String> writers = Collections.singletonList(ADVANCED_GAFFER_USER);
    private final User standardUser = new User.Builder().opAuths(GAFFER_USER).userId("123").build();
    private final User userWithAdminAuth = new User.Builder().opAuths(ADMIN_AUTH).userId("adminUser").build();
    private final User advancedUser = new User.Builder().opAuths(GAFFER_USER, ADVANCED_GAFFER_USER).userId("456").build();

    @SuppressWarnings("rawtypes")
    private final OperationChain standardOpChain = new OperationChain.Builder().first(new AddElements()).build();
    @SuppressWarnings("rawtypes")
    private final OperationChain alternativeOpChain = new OperationChain.Builder()
            .first(new GetElements.Builder().build())
            .build();
    private static final String OPERATION_NAME = "New operation";

    private final NamedOperationDetail standard = new NamedOperationDetail.Builder()
            .operationName(OPERATION_NAME)
            .description("standard operation")
            .creatorId(standardUser.getUserId())
            .readers(readers)
            .writers(writers)
            .operationChain(standardOpChain)
            .build();

    private final NamedOperationDetail alternative = new NamedOperationDetail.Builder()
            .operationName(OPERATION_NAME)
            .description("alternative operation")
            .creatorId(advancedUser.getUserId())
            .readers(readers)
            .writers(writers)
            .operationChain(alternativeOpChain)
            .build();

    @BeforeAll
    public static void setUp() {
        CacheServiceLoader.initialise(HashMapCacheService.class.getName());
        cache = new NamedOperationCache(SUFFIX_CACHE_NAME);
    }

    @BeforeEach
    public void beforeEach() throws CacheOperationException {
        cache.clearCache();
    }

    @Test
    public void shouldAddNamedOperation() throws CacheOperationException {
        cache.addNamedOperation(standard, false, standardUser);
        final NamedOperationDetail namedOperation = cache.getNamedOperation(OPERATION_NAME, standardUser);

        assertThat(namedOperation).isEqualTo(standard);
    }

    @Test
    public void shouldThrowExceptionIfNamedOperationAlreadyExists() throws CacheOperationException {
        cache.addNamedOperation(standard, false, standardUser);
        assertThatExceptionOfType(OverwritingException.class)
            .isThrownBy(() -> cache.addNamedOperation(alternative, false, advancedUser));
    }

    @Test
    public void shouldThrowExceptionWhenDeletingIfKeyIsNull() throws CacheOperationException { // needs work
        cache.addNamedOperation(standard, false, standardUser);
        assertThatExceptionOfType(CacheOperationException.class)
            .isThrownBy(() -> cache.deleteNamedOperation(null, advancedUser));
    }

    @Test
    public void shouldThrowExceptionWhenGettingIfKeyIsNull() throws CacheOperationException {
        assertThatExceptionOfType(CacheOperationException.class)
            .isThrownBy(() -> cache.getNamedOperation(null, advancedUser));
    }

    @Test
    public void shouldThrowExceptionIfNamedOperationIsNull() throws CacheOperationException {
        assertThatExceptionOfType(CacheOperationException.class)
            .isThrownBy(() -> cache.addNamedOperation(null, false, standardUser));
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToReadOperation() throws CacheOperationException {
        cache.addNamedOperation(standard, false, standardUser);
        assertThatExceptionOfType(CacheOperationException.class)
            .isThrownBy(() -> cache.getNamedOperation(OPERATION_NAME, new User()));
    }

    @Test
    public void shouldAllowUsersWithCorrectOpAuthsReadAccessToTheOperationChain() throws CacheOperationException { // see if this works with standard user - it should do
        cache.addNamedOperation(standard, false, standardUser);
        assertThat(cache.getNamedOperation(OPERATION_NAME, advancedUser)).isEqualTo(standard);
    }

    @Test
    public void shouldAllowUsersReadAccessToTheirOwnNamedOperations() throws CacheOperationException {
        final NamedOperationDetail op = new NamedOperationDetail.Builder()
                .operationName(OPERATION_NAME)
                .creatorId(standardUser.getUserId())
                .operationChain(standardOpChain)
                .readers(new ArrayList<>())
                .writers(writers)
                .build();

        cache.addNamedOperation(op, false, standardUser);
        assertThat(cache.getNamedOperation(OPERATION_NAME, standardUser)).isEqualTo(op);
    }

    @Test
    public void shouldAllowUsersWriteAccessToTheirOwnOperations() throws CacheOperationException {
        final NamedOperationDetail op = new NamedOperationDetail.Builder()
                .operationName(OPERATION_NAME)
                .creatorId(standardUser.getUserId())
                .operationChain(standardOpChain)
                .readers(readers)
                .writers(new ArrayList<>())
                .build();

        cache.addNamedOperation(op, false, standardUser);
        cache.addNamedOperation(standard, true, standardUser);

        assertThat(cache.getNamedOperation(OPERATION_NAME, standardUser)).isEqualTo(standard);
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToOverwriteOperation() throws CacheOperationException {
        cache.addNamedOperation(alternative, false, advancedUser);
        assertThatExceptionOfType(CacheOperationException.class)
                .isThrownBy(() -> cache.addNamedOperation(standard, true, standardUser));
    }

    @Test
    public void shouldAllowOverWriteIfFlagIsSetAndUserIsAuthorised() throws CacheOperationException {
        cache.addNamedOperation(standard, false, standardUser);
        cache.addNamedOperation(alternative, true, advancedUser);

        assertThat(cache.getNamedOperation(OPERATION_NAME, standardUser)).isEqualTo(alternative);
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToDeleteOperation() throws CacheOperationException {
        cache.addNamedOperation(alternative, false, advancedUser);
        assertThatExceptionOfType(CacheOperationException.class)
                .isThrownBy(() -> cache.deleteNamedOperation(OPERATION_NAME, standardUser));
    }

    @Test
    public void shouldThrowExceptionTryingToDeleteOperationConfiguredWithWriteNoAccessPredicate() throws CacheOperationException {
        final NamedOperationDetail noWriteAccess = new NamedOperationDetail.Builder()
                .creatorId(standardUser.getUserId())
                .description("an operation that does no allow read access")
                .operationName("test")
                .readers(readers)
                .operationChain(standardOpChain)
                .writeAccessPredicate(new NoAccessPredicate())
                .build();
        cache.addNamedOperation(noWriteAccess, false, standardUser);
        assertThatExceptionOfType(CacheOperationException.class).isThrownBy(() -> cache.deleteNamedOperation("test", standardUser));
    }

    @Test
    public void shouldReturnEmptySetIfThereAreNoOperationsInTheCache() {
        final Iterable<NamedOperationDetail> ops = cache.getAllNamedOperations(standardUser);
        assertThat(ops).hasSize(0);
    }

    @Test
    public void shouldReturnSetOfNamedOperationsThatAUserCanExecute() throws CacheOperationException {
        cache.addNamedOperation(standard, false, standardUser);
        final NamedOperationDetail alt = new NamedOperationDetail.Builder()
                .operationName("different operation")
                .description("alt")
                .creatorId(advancedUser.getUserId())
                .readers(readers)
                .writers(writers)
                .operationChain(alternativeOpChain)
                .build();

        cache.addNamedOperation(alt, false, advancedUser);

        final Set<NamedOperationDetail> actual = Sets.newHashSet(cache.getAllNamedOperations(standardUser));

        assertThat(actual)
                .contains(standard)
                .contains(alt)
                .hasSize(2);
    }

    @Test
    public void shouldNotReturnANamedOperationThatAUserCannotExecute() throws CacheOperationException {
        cache.addNamedOperation(standard, false, standardUser);

        final NamedOperationDetail noReadAccess = new NamedOperationDetail.Builder()
                .creatorId(advancedUser.getUserId())
                .description("an operation that a standard user cannot execute")
                .operationName("test")
                .readers(writers)
                .writers(writers)
                .operationChain(standardOpChain)
                .build();
        cache.addNamedOperation(noReadAccess, false, advancedUser);

        final Set<NamedOperationDetail> actual = Sets.newHashSet(cache.getAllNamedOperations(standardUser));

        assertThat(actual)
                .contains(standard)
                .hasSize(1);
    }

    @Test
    public void shouldNotReturnNamedOperationConfiguredWithReadNoAccessPredicate() throws CacheOperationException {
        final NamedOperationDetail noReadAccess = new NamedOperationDetail.Builder()
                .creatorId(standardUser.getUserId())
                .description("an operation that does no allow read access")
                .operationName("test")
                .writers(writers)
                .operationChain(standardOpChain)
                .readAccessPredicate(new NoAccessPredicate())
                .build();
        cache.addNamedOperation(noReadAccess, false, standardUser);

        assertThat(cache.getAllNamedOperations(standardUser).iterator()).isExhausted();
    }

    @Test
    public void shouldBeAbleToReturnFullExtendedOperationChain() throws CacheOperationException {
        cache.addNamedOperation(standard, false, standardUser);
        final NamedOperationDetail alt = new NamedOperationDetail.Builder()
                .operationName("different")
                .description("alt")
                .creatorId(advancedUser.getUserId())
                .readers(readers)
                .writers(writers)
                .operationChain(alternativeOpChain)
                .build();

        cache.addNamedOperation(alt, false, advancedUser);

        final Set<NamedOperationDetail> actual = Sets.newHashSet(cache.getAllNamedOperations(standardUser));
        assertThat(actual)
                .contains(standard)
                .contains(alt)
                .hasSize(2);
    }

    @Test
    public void shouldAllowAddingWhenUserHasAdminAuth() throws CacheOperationException {
        cache.addNamedOperation(alternative, false, advancedUser, EMPTY_ADMIN_AUTH);
        final NamedOperationDetail alt = new NamedOperationDetail.Builder()
                .operationName(alternative.getOperationName())
                .description("alt")
                .creatorId(standardUser.getUserId())
                .operationChain(alternativeOpChain)
                .build();

        cache.addNamedOperation(alt, true, userWithAdminAuth, ADMIN_AUTH);
    }
}
