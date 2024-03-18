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

package uk.gov.gchq.gaffer.graph.hook;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class NamedOperationResolverTest extends GraphHookTest<NamedOperationResolver> {

    static final String SUFFIX_CACHE_NAME = "suffix";

    NamedOperationResolverTest() {
        super(NamedOperationResolver.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldResolveNamedOperation(@Mock final User user,
                                     @Mock final NamedOperationCache cache,
                                     @Mock final NamedOperationDetail namedOpDetail,
                                     @Mock final GetAdjacentIds op1,
                                     @Mock final GetElements op2,
                                     @Mock final Iterable<? extends EntityId> input)
            throws CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        final List<Operation> expectedResolvedChain = Arrays.asList(new OperationChain(Arrays.asList(op1, op2)));

        given(op1.getInput()).willReturn(null);
        given(cache.getNamedOperation(opName, user)).willReturn(namedOpDetail);
        given(namedOpDetail.getOperationChain(null)).willReturn(new OperationChain(Arrays.asList(op1, op2)));

        final OperationChain<Object> opChain = new OperationChain.Builder()
            .first(new NamedOperation.Builder<>()
                    .name(opName)
                    .input(input)
                    .build())
            .build();

        // When
        resolver.preExecute(opChain, new Context(user));

        // Then
        assertThat(opChain.getOperations()).isEqualTo(expectedResolvedChain);

        verify(op1).setInput(input);
        verify(op2, never()).setInput(input);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldResolveNestedNamedOperation(@Mock final User user,
                                           @Mock final NamedOperationCache cache,
                                           @Mock final NamedOperationDetail namedOpDetail,
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
        given(cache.getNamedOperation(opName, user)).willReturn(namedOpDetail);
        given(namedOpDetail.getOperationChain(params)).willReturn(namedOperationOpChain);

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
        assertThat(opChain.getOperations()).hasSize(1);
        final OperationChain<?> nestedOpChain = (OperationChain<?>) opChain.getOperations().get(0);
        assertThat(nestedOpChain.getOperations()).isEqualTo(namedOperationOpChain.getOperations());

        verify(op1).setInput(input);
        verify(op2, never()).setInput(input);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldResolveNestedNamedOperationsWithLimit(@Mock final User user,
                                                     @Mock final NamedOperationCache cache,
                                                     @Mock final NamedOperationDetail namedOp1Detail,
                                                     @Mock final NamedOperationDetail namedOp2Detail,
                                                     @Mock final NamedOperationDetail namedOp3Detail,
                                                     @Mock final GetAdjacentIds op1,
                                                     @Mock final GetElements op2,
                                                     @Mock final GetElements op3,
                                                     @Mock final Iterable<? extends EntityId> input)
            throws CacheOperationException {
        // Given
        final String namedOp1Name = "namedOp1";
        final String namedOp2Name = "namedOp2";
        final String namedOp3Name = "namedOp3";
        final String namedOp4Name = "namedOp4";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        // Setup cache returns (we can ignore named op 4 as it wont be used due to the depth limit)
        given(cache.getNamedOperation(namedOp1Name, user)).willReturn(namedOp1Detail);
        given(cache.getNamedOperation(namedOp2Name, user)).willReturn(namedOp2Detail);
        given(cache.getNamedOperation(namedOp3Name, user)).willReturn(namedOp3Detail);

        // Create named ops
        NamedOperation namedOp1 = new NamedOperation.Builder<>()
                .name(namedOp1Name)
                .input(input)
                .build();
        NamedOperation namedOp2 = new NamedOperation.Builder<>()
                .name(namedOp2Name)
                .input(input)
                .build();
        NamedOperation namedOp3 = new NamedOperation.Builder<>()
                .name(namedOp3Name)
                .input(input)
                .build();
        NamedOperation namedOp4 = new NamedOperation.Builder<>()
                .name(namedOp4Name)
                .input(input)
                .build();
        // Set up named op returns so they're nested e.g. 4 is nested in 3, 3 in 2 and so on
        given(namedOp1Detail.getOperationChain(null)).willReturn(
                new OperationChain.Builder()
                        .first(op1)
                        .then(namedOp2)
                        .build());
        given(namedOp2Detail.getOperationChain(null)).willReturn(
                new OperationChain.Builder()
                        .first(op2)
                        .then(namedOp3)
                        .build());
        given(namedOp3Detail.getOperationChain(null)).willReturn(
                new OperationChain.Builder()
                        .first(op3)
                        .then(namedOp4)
                        .build());
        // This is the expected list of resolved operation(s), only to a depth of 3 should be returned by default
        final List<Operation> expectedResolvedChain = Arrays.asList(
            new OperationChain<>(Arrays.asList(op1,
                new OperationChain<>(Arrays.asList(op2,
                    new OperationChain<>(Arrays.asList(op3, namedOp4)))
                ))));

        // Create chain with named op 1 in to see if get resolved to the limit
        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(namedOp1)
                .build();
        // When
        resolver.preExecute(opChain, new Context(user));

        // Then
        assertThat(opChain.getOperations()).isEqualTo(expectedResolvedChain);
    }


    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldExecuteNamedOperationWithoutOverridingInput(@Mock final User user,
                                                           @Mock final NamedOperationCache cache,
                                                           @Mock final NamedOperationDetail namedOpDetail,
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
        given(cache.getNamedOperation(opName, user)).willReturn(namedOpDetail);
        given(namedOpDetail.getOperationChain(params)).willReturn(namedOpChain);

        // When
        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(opName)
                        .input(input)
                        .build())
                .build();
        resolver.preExecute(opChain, new Context(user));

        // Then
        assertThat(opChain.getOperations().get(0)).isSameAs(namedOpChain);
        verify(op1, never()).setInput(input);
        verify(op2, never()).setInput(input);
    }

    @SuppressWarnings({ "rawtypes" })
    @Test
    void shouldResolveNamedOperationWithParameter(@Mock final User user,
                                                         @Mock final NamedOperationCache cache)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        final Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("param1", 1L);

        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = new HashMap<>();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail namedOpDetail = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                        + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        given(cache.getNamedOperation(opName, user)).willReturn(namedOpDetail);

        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(opName)
                        .parameters(paramMap)
                        .build())
                .build();
        // When

        resolver.preExecute(opChain, new Context(user));

        // Then
        assertThat(opChain.getOperations().get(0))
            .isInstanceOf(OperationChain.class);
        assertThat(((OperationChain) opChain.getOperations().get(0)).getOperations().get(0)).isInstanceOf(GetAllElements.class);
        assertThat(((OperationChain) opChain.getOperations().get(0)).getOperations().get(1)).isInstanceOf(Limit.class);

        // Check the parameter has been inserted
        assertThat(((Limit<?>) ((OperationChain) opChain.getOperations().get(0)).getOperations().get(1)).getResultLimit()).isEqualTo(1L);
    }

    @Test
    void shouldNotExecuteNamedOperationWithParameterOfWrongType(@Mock final User user,
                                                                @Mock final NamedOperationCache cache)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        final Map<String, Object> paramMap = new HashMap<>();
        // A parameter of the wrong type
        paramMap.put("param1", new ArrayList<>());

        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = new HashMap<>();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail namedOpDetail = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                        + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        given(cache.getNamedOperation(opName, user)).willReturn(namedOpDetail);

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> resolver.preExecute(new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(opName)
                                .parameters(paramMap)
                                .build())
                        .build(), new Context(user)))
                .withMessageContaining("Cannot deserialize value of type");
    }

    @Test
    void shouldNotExecuteNamedOperationWithWrongParameterName(@Mock final User user,
                                                              @Mock final NamedOperationCache cache)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        final Map<String, Object> paramMap = new HashMap<>();
        // A parameter with the wrong name
        paramMap.put("param2", 1L);

        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = new HashMap<>();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail namedOpDetail = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                        + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        given(cache.getNamedOperation(opName, user)).willReturn(namedOpDetail);

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> resolver.preExecute(new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(opName)
                        .parameters(paramMap)
                        .build())
                .build(), new Context(user)))
                .withMessageContaining("Unexpected parameter name in NamedOperation");
    }

    @Test
    void shouldNotExecuteNamedOperationWithMissingRequiredArg(@Mock final User user,
                                                              @Mock final NamedOperationCache cache)
            throws OperationException, CacheOperationException {
        // Given
        final String opName = "opName";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        // Don't set any parameters
        final Map<String, Object> paramMap = new HashMap<>();

        final ParameterDetail param = new ParameterDetail.Builder()
                .description("Limit param")
                .valueClass(Long.class)
                .required(true)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = new HashMap<>();
        paramDetailMap.put("param1", param);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail namedOpDetail = new NamedOperationDetail.Builder()
                .operationName(opName)
                .description("standard operation")
                .operationChain("{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                        + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }")
                .parameters(paramDetailMap)
                .build();

        given(cache.getNamedOperation(opName, user)).willReturn(namedOpDetail);

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> resolver.preExecute(new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(opName)
                                .parameters(paramMap)
                                .build())
                        .build(), new Context(user)))
                .withMessageContaining("Missing parameter param1 with no default");
    }

    @SuppressWarnings({"unchecked", "resource", "rawtypes"})
    @Test
    void shouldReturnOperationsInParameters() {
        // Given
        final NamedOperation namedOperation = new NamedOperation();
        final Operation operation = new GetElements();
        final Map<String, Object> paramMap = new HashMap<>();

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
        return new NamedOperationResolver(SUFFIX_CACHE_NAME);
    }
}
