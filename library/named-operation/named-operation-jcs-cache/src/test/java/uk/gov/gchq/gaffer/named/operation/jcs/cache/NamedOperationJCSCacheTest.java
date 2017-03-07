/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.named.operation.jcs.cache;


import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.named.operation.ExtendedNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NamedOperationJCSCacheTest {

    private static final String GAFFER_USER = "gaffer user";
    private static final String ADVANCED_GAFFER_USER = "advanced gaffer user";
    private List<String> readers = Collections.singletonList(GAFFER_USER);
    private List<String> writers = Collections.singletonList(ADVANCED_GAFFER_USER);
    private User standardUser = new User.Builder().opAuths(GAFFER_USER).userId("123").build();
    private User advancedUser = new User.Builder().opAuths(GAFFER_USER, ADVANCED_GAFFER_USER).userId("456").build();
    private OperationChain standardOpChain = new OperationChain.Builder().first(new AddElements()).build();
    private OperationChain alternativeOpChain = new OperationChain.Builder().
            first(new GetEntities.Builder<>().build())
            .build();
    private static final String OPERATION_NAME = "New operation";
    private NamedOperationJCSCache cache = new NamedOperationJCSCache();

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private ExtendedNamedOperation standard = new ExtendedNamedOperation.Builder()
            .operationName(OPERATION_NAME)
            .description("standard operation")
            .creatorId(standardUser.getUserId())
            .readers(readers)
            .writers(writers)
            .operationChain(standardOpChain)
            .build();


    private ExtendedNamedOperation alternative = new ExtendedNamedOperation.Builder()
            .operationName(OPERATION_NAME)
            .description("alternative operation")
            .creatorId(advancedUser.getUserId())
            .readers(readers)
            .writers(writers)
            .operationChain(alternativeOpChain)
            .build();

    @Before
    public void setUp() throws CacheOperationFailedException {
        cache.clear();
    }

    @After
    public void teardown() throws CacheOperationFailedException {
        cache.clear();
    }

    @Test
    public void shouldAddNamedOperation() throws CacheOperationFailedException {
        cache.addNamedOperation(standard, false, standardUser);
        ExtendedNamedOperation namedOperation = cache.getNamedOperation(OPERATION_NAME, standardUser);

        assertEquals(standard, namedOperation);

    }

    @Test
    public void shouldThrowExceptionIfNamedOperationAlreadyExists() throws CacheOperationFailedException {
        cache.addNamedOperation(standard, false, standardUser);
        exception.expect(CacheOperationFailedException.class);
        cache.addNamedOperation(alternative, false, advancedUser);
    }

    @Test
    public void shouldThrowExceptionWhenAddingIfKeyIsNull() throws CacheOperationFailedException {
        ExtendedNamedOperation op = standard;
        op.setOperationName(null);
        exception.expect(CacheOperationFailedException.class);
        cache.addNamedOperation(op, false, standardUser);
    }

    @Test
    public void shouldThrowExceptionWhenDeletingIfKeyIsNull() throws CacheOperationFailedException { // needs work
        cache.addNamedOperation(standard, false, standardUser);
        exception.expect(CacheOperationFailedException.class);
        cache.deleteNamedOperation(null, advancedUser);
    }

    @Test
    public void shouldThrowExceptionWhenGettingIfKeyIsNull() throws CacheOperationFailedException {
        exception.expect(CacheOperationFailedException.class);
        cache.getNamedOperation(null, advancedUser);
    }

    @Test
    public void shouldThrowExceptionIfNamedOperationIsNull() throws CacheOperationFailedException {
        exception.expect(CacheOperationFailedException.class);
        cache.addNamedOperation(null, false, standardUser);
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToReadOperation() throws CacheOperationFailedException {
        cache.addNamedOperation(standard, false, standardUser);
        exception.expect(CacheOperationFailedException.class);
        cache.getNamedOperation(OPERATION_NAME, new User());
    }

    @Test
    public void shouldAllowUsersWithCorrectOpAuthsReadAccessToTheOperationChain() throws CacheOperationFailedException { // see if this works with standard user - it should do
        cache.addNamedOperation(standard, false, standardUser);
        assertEquals(standard, cache.getNamedOperation(OPERATION_NAME, advancedUser));
    }

    @Test
    public void shouldAllowUsersReadAccessToTheirOwnNamedOperations() throws CacheOperationFailedException {
        ExtendedNamedOperation op = new ExtendedNamedOperation.Builder()
                .operationName(OPERATION_NAME)
                .creatorId(standardUser.getUserId())
                .operationChain(standardOpChain)
                .readers(new ArrayList<>())
                .writers(writers)
                .build();

        cache.addNamedOperation(op, false, standardUser);
        assertEquals(op, cache.getNamedOperation(OPERATION_NAME, standardUser));
    }

    @Test
    public void shouldAllowUsersWriteAccessToTheirOwnOperations() throws CacheOperationFailedException {
        ExtendedNamedOperation op = new ExtendedNamedOperation.Builder()
                .operationName(OPERATION_NAME)
                .creatorId(standardUser.getUserId())
                .operationChain(standardOpChain)
                .readers(readers)
                .writers(new ArrayList<>())
                .build();

        cache.addNamedOperation(op, false, standardUser);
        cache.addNamedOperation(standard, true, standardUser);

        assertEquals(standard, cache.getNamedOperation(OPERATION_NAME, standardUser));
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToOverwriteOperation() throws CacheOperationFailedException {
        cache.addNamedOperation(alternative, false, advancedUser);
        exception.expect(CacheOperationFailedException.class);
        cache.addNamedOperation(standard, true, standardUser);
    }

    @Test
    public void shouldAllowOverWriteIfFlagIsSetAndUserIsAuthorised() throws CacheOperationFailedException {
        cache.addNamedOperation(standard, false, standardUser);
        cache.addNamedOperation(alternative, true, advancedUser);

        assertEquals(alternative, cache.getNamedOperation(OPERATION_NAME, standardUser));
    }

    @Test
    public void shouldThrowExceptionIfUnauthorisedUserTriesToDeleteOperation() throws CacheOperationFailedException {
        cache.addNamedOperation(alternative, false, advancedUser);
        exception.expect(CacheOperationFailedException.class);
        cache.deleteNamedOperation(OPERATION_NAME, standardUser);
    }

    @Test
    public void shouldReturnEmptySetIfThereAreNoOperationsInTheCache() {
        CloseableIterable<NamedOperation> ops = cache.getAllNamedOperations(standardUser, true);
        assert (Iterables.size(ops) == 0);
    }

    @Test
    public void shouldReturnSetOfNamedOperationsThatAUserCanExecute() throws CacheOperationFailedException {
        cache.addNamedOperation(standard, false, standardUser);
        ExtendedNamedOperation alt = alternative;
        alt.setOperationName("alt");
        cache.addNamedOperation(alt, false, advancedUser);

        Set<NamedOperation> actual = Sets.newHashSet(cache.getAllNamedOperations(standardUser, true));

        assert (actual.contains(standard.getBasic()));
        assert (actual.contains(alt.getBasic()));
        assert (actual.size() == 2);
    }

    @Test
    public void shouldNotReturnANamedOperationThatAUserCannotExecute() throws CacheOperationFailedException {
        cache.addNamedOperation(standard, false, standardUser);

        ExtendedNamedOperation noReadAccess = new ExtendedNamedOperation.Builder()
                .creatorId(advancedUser.getUserId())
                .description("an operation that a standard user cannot execute")
                .operationName("test")
                .readers(writers)
                .writers(writers)
                .operationChain(standardOpChain)
                .build();
        cache.addNamedOperation(noReadAccess, false, advancedUser);

        Set<NamedOperation> actual = Sets.newHashSet(cache.getAllNamedOperations(standardUser, true));

        assert (actual.contains(standard.getBasic()));
        assert (actual.size() == 1);
    }

    @Test
    public void shouldBeAbleToReturnFullExtendedOperationChain() throws CacheOperationFailedException {
        cache.addNamedOperation(standard, false, standardUser);
        ExtendedNamedOperation alt = alternative;
        alt.setOperationName("alt");
        cache.addNamedOperation(alt, false, advancedUser);

        Set<NamedOperation> actual = Sets.newHashSet(cache.getAllNamedOperations(standardUser, false));
        assert (actual.contains(standard));
        assert (actual.contains(alt));
        assert (actual.size() == 2);
    }
}
