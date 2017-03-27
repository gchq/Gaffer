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


import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.ExtendedNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.operation.cache.INamedOperationCache;
import uk.gov.gchq.gaffer.named.operation.cache.MockNamedOperationCache;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;
import uk.gov.gchq.gaffer.user.User;
import java.io.InputStream;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


public class NamedOperationHandlerTest {
    private final JSONSerialiser json = new JSONSerialiser();

    @Test
    public void shouldLoadFromNamedOperationDeclarationsFile() throws SerialisationException {
        final InputStream s = StreamUtil.openStream(getClass(), "NamedOperationDeclarations.json");
        final OperationDeclarations deserialised = json.deserialise(s, OperationDeclarations.class);

        assertEquals(4, deserialised.getOperations().size());
        assert (deserialised.getOperations().get(0).getHandler() instanceof AddNamedOperationHandler);
        assert (deserialised.getOperations().get(1).getHandler() instanceof NamedOperationHandler);
        assert (deserialised.getOperations().get(2).getHandler() instanceof DeleteNamedOperationHandler);
        assert (deserialised.getOperations().get(3).getHandler() instanceof GetAllNamedOperationsHandler);
    }

    @Test
    public void shouldLoadCacheFromOperationsDeclarationsFile() throws SerialisationException {
        final InputStream s = StreamUtil.openStream(getClass(), "NamedOperationDeclarations.json");
        final OperationDeclarations deserialised = json.deserialise(s, OperationDeclarations.class);

        INamedOperationCache cache = ((AddNamedOperationHandler) deserialised.getOperations().get(0).getHandler()).getCache();

        assert (cache instanceof MockNamedOperationCache);
    }

    @Test
    public void shouldExecuteNamedOperation() throws OperationException, CacheOperationFailedException {
        // Given
        final String opName = "opName";
        final NamedOperationHandler operationHandler = new NamedOperationHandler();
        final INamedOperationCache cache = mock(INamedOperationCache.class);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);
        final ExtendedNamedOperation extendedNamedOperation = mock(ExtendedNamedOperation.class);

        final Operation op1 = mock(Operation.class);
        final Operation op2 = mock(Operation.class);
        final OperationChain opChain = new OperationChain(Arrays.asList(op1, op2));
        final Object expectedResult = mock(Object.class);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        final CloseableIterable<Object> input = mock(CloseableIterable.class);
        final View view = mock(View.class);
        final View clonedView = mock(View.class);
        final View op2View = mock(View.class);

        given(op1.getInput()).willReturn(null);
        given(op1.getView()).willReturn(null);
        given(op2.getView()).willReturn(op2View);
        given(op2View.hasGroups()).willReturn(true);
        given(view.clone()).willReturn(clonedView);
        given(context.getUser()).willReturn(user);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(extendedNamedOperation.getOperationChain()).willReturn(opChain);
        given(store._execute(opChainCaptor.capture(), eq(context))).willReturn(expectedResult);

        operationHandler.setCache(cache);

        // When
        final Object result = operationHandler.doOperation(
                new NamedOperation.Builder()
                        .name(opName)
                        .view(view)
                        .input(input)
                        .build(), context, store);

        // Then
        assertSame(expectedResult, result);

        assertSame(op1, opChainCaptor.getValue().getOperations().get(0));
        verify(op1).setInput(input);
        verify(op1).setView(clonedView);
        verify(clonedView).expandGlobalDefinitions();

        assertSame(op2, opChainCaptor.getValue().getOperations().get(1));
        verify(op2, never()).setInput(input);
        verify(op2).setView(op2View);
        verify(op2View).expandGlobalDefinitions();
    }

    @Test
    public void shouldExecuteNamedOperationWithoutOverridingInput() throws OperationException, CacheOperationFailedException {
        // Given
        final String opName = "opName";
        final NamedOperationHandler operationHandler = new NamedOperationHandler();
        final INamedOperationCache cache = mock(INamedOperationCache.class);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);
        final ExtendedNamedOperation extendedNamedOperation = mock(ExtendedNamedOperation.class);

        final Operation op1 = mock(Operation.class);
        final Operation op2 = mock(Operation.class);
        final OperationChain opChain = new OperationChain(Arrays.asList(op1, op2));
        final Object expectedResult = mock(Object.class);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        final CloseableIterable<Object> input = mock(CloseableIterable.class);
        final View view = mock(View.class);
        final View clonedView = mock(View.class);
        final View op2View = mock(View.class);

        given(op1.getInput()).willReturn("some existing input");
        given(op1.getView()).willReturn(null);
        given(op2.getView()).willReturn(op2View);
        given(op2View.hasGroups()).willReturn(true);
        given(view.clone()).willReturn(clonedView);
        given(context.getUser()).willReturn(user);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(extendedNamedOperation.getOperationChain()).willReturn(opChain);
        given(store._execute(opChainCaptor.capture(), eq(context))).willReturn(expectedResult);

        operationHandler.setCache(cache);

        // When
        final Object result = operationHandler.doOperation(
                new NamedOperation.Builder()
                        .name(opName)
                        .view(view)
                        .input(input)
                        .build(), context, store);

        // Then
        assertSame(expectedResult, result);

        assertSame(op1, opChainCaptor.getValue().getOperations().get(0));
        verify(op1, never()).setInput(input);
        verify(op1).setView(clonedView);
        verify(clonedView).expandGlobalDefinitions();

        assertSame(op2, opChainCaptor.getValue().getOperations().get(1));
        verify(op2, never()).setInput(input);
        verify(op2).setView(op2View);
        verify(op2View).expandGlobalDefinitions();
    }
}
