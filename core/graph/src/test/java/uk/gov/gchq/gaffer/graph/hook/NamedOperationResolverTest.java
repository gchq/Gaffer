/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class NamedOperationResolverTest extends GraphHookTest<NamedOperationResolver> {

    public NamedOperationResolverTest() {
        super(NamedOperationResolver.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldResolveNamedOperation(@Mock final User user,
                                            @Mock final NamedOperationCache cache,
                                            @Mock final NamedOperationDetail extendedNamedOperation,
                                            @Mock final GetAdjacentIds op1,
                                            @Mock final GetElements op2,
                                            @Mock final Iterable<? extends EntityId> input)
            throws CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        final OperationChain namedOperationOpChain = new OperationChain(Arrays.asList(op1, op2));
        final Map<String, Object> params = null;

        given(op1.getInput()).willReturn(null);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(extendedNamedOperation.getOperationChain(params)).willReturn(namedOperationOpChain);

        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(opName)
                        .input(input)
                        .build())
                .build();

        // When
        resolver.preExecute(opChain, new Context(user));

        // Then
        assertThat(opChain.getOperations()).isEqualTo(namedOperationOpChain.getOperations());

        verify(op1).setInput(input);
        verify(op2, never()).setInput(input);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldResolveNestedNamedOperation(@Mock final User user,
                                                  @Mock final NamedOperationCache cache,
                                                  @Mock final NamedOperationDetail extendedNamedOperation,
                                                  @Mock final GetAdjacentIds op1,
                                                  @Mock final GetElements op2,
                                                  @Mock final Iterable<? extends EntityId> input)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        final OperationChain namedOperationOpChain = new OperationChain(Arrays.asList(op1, op2));

        final Map<String, Object> params = null;

        given(op1.getInput()).willReturn(null);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(extendedNamedOperation.getOperationChain(params)).willReturn(namedOperationOpChain);

        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(opName)
                                .input(input)
                                .build())
                        .build())
                .build();

        // When
        resolver.preExecute(opChain, new Context(user));

        // Then
        assertThat(opChain.getOperations().size()).isEqualTo(1);
        final OperationChain<?> nestedOpChain = (OperationChain<?>) opChain.getOperations().get(0);
        assertThat(nestedOpChain.getOperations()).isEqualTo(namedOperationOpChain.getOperations());

        verify(op1).setInput(input);
        verify(op2, never()).setInput(input);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldExecuteNamedOperationWithoutOverridingInput(@Mock final User user,
                                                                  @Mock final NamedOperationCache cache,
                                                                  @Mock final NamedOperationDetail extendedNamedOperation,
                                                                  @Mock final GetAdjacentIds op1,
                                                                  @Mock final GetElements op2,
                                                                  @Mock final Iterable<? extends EntityId> input,
                                                                  @Mock final Iterable mockIterable)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        final OperationChain namedOpChain = new OperationChain(Arrays.asList(op1, op2));
        final Map<String, Object> params = null;

        given(op1.getInput()).willReturn(mockIterable);
        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);
        given(extendedNamedOperation.getOperationChain(params)).willReturn(namedOpChain);

        // When
        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(opName)
                        .input(input)
                        .build())
                .build();
        resolver.preExecute(opChain, new Context(user));

        // Then
        assertThat(opChain.getOperations().get(0)).isSameAs(op1);
        verify(op1, never()).setInput(input);
        assertThat(opChain.getOperations().get(1)).isSameAs(op2);
        verify(op2, never()).setInput(input);
    }

    @Test
    public void shouldResolveNamedOperationWithParameter(@Mock final User user,
                                                         @Mock final NamedOperationCache cache)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        final Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("param1", 1L);

        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail extendedNamedOperation = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                        + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);

        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(opName)
                        .parameters(paramMap)
                        .build())
                .build();
        // When

        resolver.preExecute(opChain, new Context(user));

        // Then
        assertThat(opChain.getOperations().get(0)).isInstanceOf(GetAllElements.class);
        assertThat(opChain.getOperations().get(1)).isInstanceOf(Limit.class);

        // Check the parameter has been inserted
        assertThat(((Limit<?>) opChain.getOperations().get(1)).getResultLimit()).isEqualTo(1L);
    }

    @Test
    public void shouldNotExecuteNamedOperationWithParameterOfWrongType(@Mock final User user,
                                                                       @Mock final NamedOperationCache cache)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        final Map<String, Object> paramMap = Maps.newHashMap();
        // A parameter of the wrong type
        paramMap.put("param1", new ArrayList<>());

        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail extendedNamedOperation = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                        + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);

        // When
        assertThatIllegalArgumentException()
                .isThrownBy(() -> resolver.preExecute(new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(opName)
                                .parameters(paramMap)
                                .build())
                        .build(), new Context(user)));
    }

    @Test
    public void shouldNotExecuteNamedOperationWithWrongParameterName(@Mock final User user,
                                                                     @Mock final NamedOperationCache cache)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        final Map<String, Object> paramMap = Maps.newHashMap();
        // A parameter with the wrong name
        paramMap.put("param2", 1L);

        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail extendedNamedOperation = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                        + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);

        // When
        assertThatIllegalArgumentException().isThrownBy(() -> resolver.preExecute(new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(opName)
                        .parameters(paramMap)
                        .build())
                .build(), new Context(user)));
    }

    @Test
    public void shouldNotExecuteNamedOperationWithMissingRequiredArg(@Mock final User user,
                                                                     @Mock final NamedOperationCache cache)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        // Don't set any parameters
        final Map<String, Object> paramMap = Maps.newHashMap();

        final ParameterDetail param = new ParameterDetail.Builder()
                .description("Limit param")
                .valueClass(Long.class)
                .required(true)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail extendedNamedOperation = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                        + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        given(cache.getNamedOperation(opName, user)).willReturn(extendedNamedOperation);

        // When
        assertThatIllegalArgumentException()
                .isThrownBy(() -> resolver.preExecute(new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(opName)
                                .parameters(paramMap)
                                .build())
                        .build(), new Context(user)));
    }

    @SuppressWarnings({"unchecked", "resource", "rawtypes"})
    @Test
    public void shouldReturnOperationsInParameters() {
        // Given
        final NamedOperation namedOperation = new NamedOperation();
        final Operation operation = new GetElements();
        final Map<String, Object> paramMap = Maps.newHashMap();

        paramMap.put("test param", operation);
        namedOperation.setParameters(paramMap);

        // When
        final List<Operation> paramOperations = namedOperation.getOperations();
        final Operation op = paramOperations.get(0);

        // Then
        assertThat(paramOperations).hasSize(1);
        assertThat(op).isInstanceOf(GetElements.class);
    }

    @Override
    public NamedOperationResolver getTestObject() {
        return new NamedOperationResolver((String) null);
    }
}
