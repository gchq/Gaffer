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

package uk.gov.gchq.gaffer.named.operation.handler;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.ExtendedNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.operation.cache.MockNamedOperationCache;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import static junit.framework.TestCase.fail;

public class AddNamedOperationHandlerTest {

    AddNamedOperationHandler handler = new AddNamedOperationHandler();
    private Context context = new Context(new User.Builder()
            .userId("test user")
            .build());

    private Store store = Mockito.mock(Store.class);
    private NamedOperation operation = new NamedOperation(OPERATION_NAME, "a named operation");
    private AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
            .overwrite(false)
            .build();

    private static final String OPERATION_NAME = "test";

    @Before
    public void before() {
        addNamedOperation.setOperationName(OPERATION_NAME);
        handler.setCache(new MockNamedOperationCache());
    }


    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void after() throws CacheOperationFailedException {
        addNamedOperation.setOperationName(null);
        addNamedOperation.setOperationChain(null);
        addNamedOperation.setDescription(null);
        addNamedOperation.setOverwriteFlag(false);

        handler.getCache().clear();
    }


    @Test
    public void shouldNotAllowAdditionOfRecursiveNamedOperation() throws OperationException {


        OperationChain singleRecursion = new OperationChain.Builder()
                .first(operation)
                .build();


        addNamedOperation.setOperationChain(singleRecursion);

        exception.expect(OperationException.class);
        handler.doOperation(addNamedOperation, context, store);

    }

    @Test
    public void shouldNotAddNamedOperationIfItContainsAnOperationWhichReferencesTheParent() throws CacheOperationFailedException, OperationException {

        NamedOperation op = new NamedOperation("parent", "this is the parent which has not yet been created");
        OperationChain opChain = new OperationChain.Builder().first(op).build();

        ExtendedNamedOperation child = new ExtendedNamedOperation.Builder().
                operationChain(opChain)
                .operationName(OPERATION_NAME)
                .build();

        handler.getCache().addNamedOperation(child, false, context.getUser());

        OperationChain parentOpChain = new OperationChain.Builder().first(operation).build();
        addNamedOperation.setOperationName("parent");
        addNamedOperation.setOperationChain(parentOpChain);

        exception.expect(OperationException.class);
        handler.doOperation(addNamedOperation, context, store);
    }

    @Test
    public void shouldNotAllowNamedOperationToBeAddedContainingANamedOperationThatDoesNotExist() throws OperationException {

        NamedOperation op = new NamedOperation("parent", "this is the parent which has not yet been created");
        OperationChain opChain = new OperationChain.Builder().first(op).build();

        addNamedOperation.setOperationChain(opChain);
        addNamedOperation.setOperationName(OPERATION_NAME);

        exception.expect(OperationException.class);
        handler.doOperation(addNamedOperation, context, store);
    }

    @Test
    public void shouldNotAllowUpdateToNamedOperationIfItCausesRecursion() throws OperationException {
        String innocentOpName = "innocent";
        OperationChain innocent = new OperationChain.Builder().first(new GetElements<>()).build();

        addNamedOperation.setOperationName(innocentOpName);
        addNamedOperation.setOperationChain(innocent);

        handler.doOperation(addNamedOperation, context, store);

        OperationChain parent = new OperationChain.Builder()
                .first(new NamedOperation(innocentOpName, "call down to currently innocent named op"))
                .build();

        addNamedOperation.setOperationChain(parent);
        addNamedOperation.setOperationName(OPERATION_NAME);

        handler.doOperation(addNamedOperation, context, store);

        OperationChain recursive = new OperationChain.Builder()
                .first(new NamedOperation(OPERATION_NAME, ""))
                .build();

        addNamedOperation.setOperationName(innocentOpName);
        addNamedOperation.setOperationChain(recursive);
        addNamedOperation.setOverwriteFlag(true);

        exception.expect(OperationException.class);
        handler.doOperation(addNamedOperation, context, store);
    }

    @Test
    public void shouldAllowForNonRecursiveNamedOperationsToBeNested() {
        try {
            OperationChain child = new OperationChain.Builder().first(new AddElements()).build();
            addNamedOperation.setOperationChain(child);
            addNamedOperation.setOperationName("child");
            handler.doOperation(addNamedOperation, context, store);

            OperationChain parent = new OperationChain.Builder()
                    .first(new NamedOperation("child", ""))
                    .then(new GetElements<>())
                    .build();

            addNamedOperation.setOperationChain(parent);
            addNamedOperation.setOperationName("parent");
            handler.doOperation(addNamedOperation, context, store);

            OperationChain grandparent = new OperationChain.Builder()
                    .first(new AddElements())
                    .then(new NamedOperation("parent", ""))
                    .then(new NamedOperation("child", ""))
                    .then(new GetEntities<>())
                    .build();

            addNamedOperation.setOperationChain(grandparent);
            addNamedOperation.setOperationName("grandparent");
            handler.doOperation(addNamedOperation, context, store);
            assert (cacheContains("grandparent"));


        } catch (final Exception e) {
            fail("Expected test to pass without error");
        }

    }

    private boolean cacheContains(String opName) {
        Iterable<NamedOperation> ops = handler.getCache().getAllNamedOperations(context.getUser(), true);
        for (NamedOperation op : ops) {
            if (op.getOperationName().equals(opName)) {
                return true;
            }
        }
        return false;

    }
}
