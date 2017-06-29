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


import com.google.common.collect.Maps;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.operation.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;
import uk.gov.gchq.gaffer.user.User;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


public class NamedOperationHandlerTest {
    private final JSONSerialiser json = new JSONSerialiser();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldLoadFromNamedOperationDeclarationsFile() throws SerialisationException {
        final InputStream s = StreamUtil.openStream(getClass(), "NamedOperationDeclarations.json");
        final OperationDeclarations deserialised = json.deserialise(s, OperationDeclarations.class);

        assertEquals(4, deserialised.getOperations().size());
        assert deserialised.getOperations().get(0).getHandler() instanceof AddNamedOperationHandler;
        assert deserialised.getOperations().get(1).getHandler() instanceof NamedOperationHandler;
        assert deserialised.getOperations().get(2).getHandler() instanceof DeleteNamedOperationHandler;
        assert deserialised.getOperations().get(3).getHandler() instanceof GetAllNamedOperationsHandler;
    }

    @Test
    public void shouldExecuteNamedOperation() throws OperationException, CacheOperationFailedException {
        // Given
        final String opName = "opName";
        final NamedOperationCache cache = mock(NamedOperationCache.class);
        final NamedOperationHandler operationHandler = new NamedOperationHandler(cache);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);
        final NamedOperationDetail extendedNamedOperation = mock(NamedOperationDetail.class);

        final GetAdjacentIds op1 = mock(GetAdjacentIds.class);
        final GetElements op2 = mock(GetElements.class);
        final OperationChain opChain = new OperationChain(Arrays.asList(op1, op2));
        final Object expectedResult = mock(Object.class);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        final Iterable<?> input = mock(CloseableIterable.class);
        final View view = mock(View.class);
        final View clonedView = mock(View.class);
        final View op2View = mock(View.class);

        final Map<String, Object> params = null;

        given(op1.getInput()).willReturn(null);
        given(op1.getView()).willReturn(null);
        given(op2.getView()).willReturn(op2View);
        given(op2View.hasGroups()).willReturn(true);
        given(view.clone()).willReturn(clonedView);
        given(context.getUser()).willReturn(user);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(extendedNamedOperation.getOperationChain(params)).willReturn(opChain);
        given(store._execute(opChainCaptor.capture(), eq(context))).willReturn(expectedResult);

        // When
        final Object result = operationHandler.doOperation(
                new NamedOperation.Builder<>()
                        .name(opName)
                        .view(view)
                        .input(input)
                        .build(),
                context, store);

        // Then
        assertSame(expectedResult, result);

        assertSame(op1, opChainCaptor.getValue().getOperations().get(0));
        verify(op1).setInput((Iterable) input);
        verify(op1).setView(clonedView);
        verify(clonedView).expandGlobalDefinitions();

        assertSame(op2, opChainCaptor.getValue().getOperations().get(1));
        verify(op2, never()).setInput((Iterable) input);
        verify(op2).setView(op2View);
        verify(op2View).expandGlobalDefinitions();
    }

    @Test
    public void shouldExecuteNamedOperationWithoutOverridingInput() throws OperationException, CacheOperationFailedException {
        // Given
        final String opName = "opName";
        final NamedOperationCache cache = mock(NamedOperationCache.class);
        final NamedOperationHandler operationHandler = new NamedOperationHandler(cache);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);
        final NamedOperationDetail extendedNamedOperation = mock(NamedOperationDetail.class);

        final GetAdjacentIds op1 = mock(GetAdjacentIds.class);
        final GetElements op2 = mock(GetElements.class);
        final OperationChain opChain = new OperationChain(Arrays.asList(op1, op2));
        final Object expectedResult = mock(Object.class);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        final Iterable<?> input = mock(CloseableIterable.class);
        final View view = mock(View.class);
        final View clonedView = mock(View.class);
        final View op2View = mock(View.class);

        final Map<String, Object> params = null;

        given(op1.getInput()).willReturn(mock(CloseableIterable.class));
        given(op1.getView()).willReturn(null);
        given(op2.getView()).willReturn(op2View);
        given(op2View.hasGroups()).willReturn(true);
        given(view.clone()).willReturn(clonedView);
        given(context.getUser()).willReturn(user);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(extendedNamedOperation.getOperationChain(params)).willReturn(opChain);
        given(store._execute(opChainCaptor.capture(), eq(context))).willReturn(expectedResult);

        // When
        final Object result = operationHandler.doOperation(
                new NamedOperation.Builder<>()
                        .name(opName)
                        .view(view)
                        .input(input)
                        .build(),
                context, store);

        // Then
        assertSame(expectedResult, result);

        assertSame(op1, opChainCaptor.getValue().getOperations().get(0));
        verify(op1, never()).setInput((Iterable) input);
        verify(op1).setView(clonedView);
        verify(clonedView).expandGlobalDefinitions();

        assertSame(op2, opChainCaptor.getValue().getOperations().get(1));
        verify(op2, never()).setInput((Iterable) input);
        verify(op2).setView(op2View);
        verify(op2View).expandGlobalDefinitions();
    }

    @Test
    public void shouldExecuteNamedOperationWithParameter() throws OperationException, CacheOperationFailedException {
        // Given
        final String opName = "opName";
        final NamedOperationCache cache = mock(NamedOperationCache.class);
        final NamedOperationHandler operationHandler = new NamedOperationHandler(cache);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("param1", 1L);

        ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail extendedNamedOperation = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        final Object expectedResult = mock(Object.class);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        final View view = mock(View.class);
        final View clonedView = mock(View.class);

        given(view.clone()).willReturn(clonedView);
        given(context.getUser()).willReturn(user);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(store._execute(opChainCaptor.capture(), eq(context))).willReturn(expectedResult);

        // When
        final Object result = operationHandler.doOperation(
                new NamedOperation.Builder<>()
                        .name(opName)
                        .view(view)
                        .parameters(paramMap)
                        .build(),
                context, store);

        // Then
        assertSame(expectedResult, result);

        List<Operation> opChain = opChainCaptor.getValue().getOperations();

        assertEquals(opChain.get(0).getClass(), GetAllElements.class);
        assertEquals(opChain.get(1).getClass(), Limit.class);

        // Check the parameter has been inserted
        assertEquals((long) ((Limit) opChain.get(1)).getResultLimit(), 1L);
    }

    @Test
    public void shouldNotExecuteNamedOperationWithParameterOfWrongType() throws OperationException, CacheOperationFailedException {
        // Given
        final String opName = "opName";
        final NamedOperationCache cache = mock(NamedOperationCache.class);
        final NamedOperationHandler operationHandler = new NamedOperationHandler(cache);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        Map<String, Object> paramMap = Maps.newHashMap();
        // A parameter of the wrong type
        paramMap.put("param1", new ArrayList());

        ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail extendedNamedOperation = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        final Object expectedResult = mock(Object.class);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        final View view = mock(View.class);
        final View clonedView = mock(View.class);

        given(view.clone()).willReturn(clonedView);
        given(context.getUser()).willReturn(user);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(store._execute(opChainCaptor.capture(), eq(context))).willReturn(expectedResult);

        // When
        exception.expect(IllegalArgumentException.class);
        operationHandler.doOperation(
                new NamedOperation.Builder<>()
                        .name(opName)
                        .view(view)
                        .parameters(paramMap)
                        .build(),
                context, store);
    }

    @Test
    public void shouldNotExecuteNamedOperationWithWrongParameterName() throws OperationException, CacheOperationFailedException {
        // Given
        final String opName = "opName";
        final NamedOperationCache cache = mock(NamedOperationCache.class);
        final NamedOperationHandler operationHandler = new NamedOperationHandler(cache);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        Map<String, Object> paramMap = Maps.newHashMap();
        // A parameter with the wrong name
        paramMap.put("param2", 1L);

        ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail extendedNamedOperation = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        final Object expectedResult = mock(Object.class);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        final View view = mock(View.class);
        final View clonedView = mock(View.class);

        given(view.clone()).willReturn(clonedView);
        given(context.getUser()).willReturn(user);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(store._execute(opChainCaptor.capture(), eq(context))).willReturn(expectedResult);

        // When
        exception.expect(IllegalArgumentException.class);
        operationHandler.doOperation(
                new NamedOperation.Builder<>()
                        .name(opName)
                        .view(view)
                        .parameters(paramMap)
                        .build(),
                context, store);
    }

    @Test
    public void shouldNotExecuteNamedOperationWithMissingRequiredArg() throws OperationException, CacheOperationFailedException {
        // Given
        final String opName = "opName";
        final NamedOperationCache cache = mock(NamedOperationCache.class);
        final NamedOperationHandler operationHandler = new NamedOperationHandler(cache);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        // Don't set any parameters
        Map<String, Object> paramMap = Maps.newHashMap();

        ParameterDetail param = new ParameterDetail.Builder()
                .description("Limit param")
                .valueClass(Long.class)
                .required(true)
                .build();
        Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail extendedNamedOperation = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        final Object expectedResult = mock(Object.class);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        final View view = mock(View.class);
        final View clonedView = mock(View.class);

        given(view.clone()).willReturn(clonedView);
        given(context.getUser()).willReturn(user);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(store._execute(opChainCaptor.capture(), eq(context))).willReturn(expectedResult);

        // When
        exception.expect(IllegalArgumentException.class);
        operationHandler.doOperation(
                new NamedOperation.Builder<>()
                        .name(opName)
                        .view(view)
                        .parameters(paramMap)
                        .build(),
                context, store);
    }
}
