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

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
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
    static final String OP_NAME = "opName";

    NamedOperationResolverTest() {
        super(NamedOperationResolver.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldResolveNamedOperation(@Mock final User user,
                                     @Mock final Context context,
                                     @Mock final NamedOperationCache cache,
                                     @Mock final NamedOperationDetail namedOpDetail,
                                     @Mock final GetAdjacentIds op1,
                                     @Mock final GetElements op2,
                                     @Mock final Iterable<? extends EntityId> input)
            throws CacheOperationException {
        // Given
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        final OperationChain<?> testChain = new OperationChain(Arrays.asList(op1, op2));
        final List<Operation> expectedResolvedChain = Arrays.asList(testChain);

        given(op1.getInput()).willReturn(null);
        given(cache.getNamedOperation(OP_NAME, user)).willReturn(namedOpDetail);
        given(namedOpDetail.getOperationChain(null)).willReturn(testChain);
        given(context.getUser()).willReturn(user);

        final OperationChain<Object> opChain = new OperationChain.Builder()
            .first(new NamedOperation.Builder<>()
                    .name(OP_NAME)
                    .input(input)
                    .build())
            .build();

        // When
        resolver.preExecute(opChain, context);

        // Then
        assertThat(opChain.getOperations()).isEqualTo(expectedResolvedChain);

        verify(op1).setInput(input);
        verify(op2, never()).setInput(input);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldResolveNestedNamedOperation(@Mock final User user,
                                            @Mock final Context context,
                                           @Mock final NamedOperationCache cache,
                                           @Mock final NamedOperationDetail namedOpDetail,
                                           @Mock final GetAdjacentIds op1,
                                           @Mock final GetElements op2,
                                           @Mock final Iterable<? extends EntityId> input)
            throws CacheOperationException {
        // Given
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        final OperationChain namedOperationOpChain = new OperationChain(Arrays.asList(op1, op2));

        final Map<String, Object> params = null;

        given(op1.getInput()).willReturn(null);
        given(cache.getNamedOperation(OP_NAME, user)).willReturn(namedOpDetail);
        given(namedOpDetail.getOperationChain(params)).willReturn(namedOperationOpChain);
        given(context.getUser()).willReturn(user);

        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(OP_NAME)
                                .input(input)
                                .build())
                        .build())
                .build();

        // When
        resolver.preExecute(opChain, context);

        // Then
        assertThat(opChain.getOperations()).hasSize(1);
        final OperationChain<?> nestedOpChain = (OperationChain<?>) opChain.getOperations().get(0);
        assertThat(nestedOpChain.getOperations()).isEqualTo(namedOperationOpChain.getOperations());

        verify(op1).setInput(input);
        verify(op2, never()).setInput(input);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldFailToResolveNestedNamedOperationsOverDefaultLimit(
            @Mock final User user,
            @Mock final Context context,
            @Mock final NamedOperationCache cache,
            @Mock final NamedOperationDetail namedOp1Detail,
            @Mock final NamedOperationDetail namedOp2Detail,
            @Mock final NamedOperationDetail namedOp3Detail,
            @Mock final GetAdjacentIds op1,
            @Mock final GetElements op2,
            @Mock final GetElements op3,
            @Mock final Iterable<? extends EntityId> input) throws CacheOperationException {
        // Given
        final String namedOp1Name = "namedOp1";
        final String namedOp2Name = "namedOp2";
        final String namedOp3Name = "namedOp3";
        final String namedOp4Name = "namedOp4";
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        given(context.getUser()).willReturn(user);
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

        // Create chain with named op 1 in to see if get resolved to the limit
        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(namedOp1)
                .build();
        // When
        assertThatExceptionOfType(GafferRuntimeException.class)
            .isThrownBy(() -> resolver.preExecute(opChain, context))
            .withMessageContaining("NamedOperation Resolver hit nested depth limit");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldAllowConfigurableResolverDepthLimit(
            @Mock final User user,
            @Mock final Context context,
            @Mock final NamedOperationCache cache,
            @Mock final NamedOperationDetail namedOp1Detail,
            @Mock final NamedOperationDetail namedOp2Detail,
            @Mock final GetAdjacentIds op1,
            @Mock final GetElements op2,
            @Mock final GetElements op3,
            @Mock final Iterable<? extends EntityId> input) throws CacheOperationException {

        // Given
        final String namedOp1Name = "namedOp1";
        final String namedOp2Name = "namedOp2";
        final String namedOp3Name = "namedOp3";
        // Make a resolver with a stricter depth limit
        final NamedOperationResolver resolverStrict = new NamedOperationResolver(cache, 2);
        given(context.getUser()).willReturn(user);

        // Setup cache returns
        given(cache.getNamedOperation(namedOp1Name, user)).willReturn(namedOp1Detail);
        given(cache.getNamedOperation(namedOp2Name, user)).willReturn(namedOp2Detail);

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

        // Set up named op returns so they're nested e.g. 3 in 2, 2 in 1
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

        // Create chains with named op 1 in to see if get resolved to the limit
        final OperationChain<Object> opChainStrict = new OperationChain.Builder()
                .first(namedOp1)
                .build();

        // When resolved using the stricter limit it should fail to resolve the chain
        assertThatExceptionOfType(GafferRuntimeException.class)
            .isThrownBy(() -> resolverStrict.preExecute(opChainStrict, context))
            .withMessageContaining("NamedOperation Resolver hit nested depth limit");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldExecuteNamedOperationWithoutOverridingInput(@Mock final User user,
                                                            @Mock final Context context,
                                                           @Mock final NamedOperationCache cache,
                                                           @Mock final NamedOperationDetail namedOpDetail,
                                                           @Mock final GetAdjacentIds op1,
                                                           @Mock final GetElements op2,
                                                           @Mock final Iterable<? extends EntityId> input,
                                                           @Mock final Iterable mockIterable)
            throws CacheOperationException {
        // Given
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        final OperationChain namedOpChain = new OperationChain(Arrays.asList(op1, op2));
        final Map<String, Object> params = null;

        given(op1.getInput()).willReturn(mockIterable);
        given(cache.getNamedOperation(OP_NAME, user)).willReturn(namedOpDetail);
        given(namedOpDetail.getOperationChain(params)).willReturn(namedOpChain);
        given(context.getUser()).willReturn(user);

        // When
        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(OP_NAME)
                        .input(input)
                        .build())
                .build();
        resolver.preExecute(opChain, context);

        // Then
        assertThat(opChain.getOperations().get(0)).isSameAs(namedOpChain);
        verify(op1, never()).setInput(input);
        verify(op2, never()).setInput(input);
    }

    @SuppressWarnings({ "rawtypes" })
    @Test
    void shouldResolveNamedOperationWithParameter(@Mock final User user,
                                                    @Mock final Context context,
                                                    @Mock final NamedOperationCache cache)
            throws CacheOperationException {
        // Given
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        final Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("param1", 1L);

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail namedOpDetail = getValidNamedOperation();

        given(cache.getNamedOperation(OP_NAME, user)).willReturn(namedOpDetail);
        given(context.getUser()).willReturn(user);

        final OperationChain<Object> opChain = new OperationChain.Builder()
                .first(new NamedOperation.Builder<>()
                        .name(OP_NAME)
                        .parameters(paramMap)
                        .build())
                .build();
        // When

        resolver.preExecute(opChain, context);

        // Then
        assertThat(opChain.getOperations().get(0)).isInstanceOf(OperationChain.class);
        assertThat(((OperationChain) opChain.getOperations().get(0)).getOperations().get(0)).isInstanceOf(GetAllElements.class);
        assertThat(((OperationChain) opChain.getOperations().get(0)).getOperations().get(1)).isInstanceOf(Limit.class);

        // Check the parameter has been inserted
        assertThat(((Limit<?>) ((OperationChain) opChain.getOperations().get(0)).getOperations().get(1)).getResultLimit()).isEqualTo(1);
    }

    @Test
    void shouldNotExecuteNamedOperationWithParameterOfWrongType(@Mock final User user,
                                                                @Mock final Context context,
                                                                @Mock final NamedOperationCache cache)
            throws CacheOperationException {
        // Given
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        // Create Named Op with param of wrong type
        final Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("param1", new ArrayList<>());

        final OperationChain<Object> wrongParamTypeNamedOp = new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(OP_NAME)
                                .parameters(paramMap)
                                .build())
                        .build();

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail namedOpDetail = getValidNamedOperation();

        given(cache.getNamedOperation(OP_NAME, user)).willReturn(namedOpDetail);
        given(context.getUser()).willReturn(user);

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> resolver.preExecute(wrongParamTypeNamedOp, context))
                .withMessageContaining("Cannot deserialize value of type");
    }

    @Test
    void shouldNotExecuteNamedOperationWithWrongParameterName(@Mock final User user,
                                                                @Mock final Context context,
                                                              @Mock final NamedOperationCache cache)
            throws CacheOperationException {
        // Given
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);

        // A parameter with the wrong name
        final Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("param2", 1L);

        final OperationChain<Object> wrongParamNameNamedOp = new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(OP_NAME)
                                .parameters(paramMap)
                                .build())
                        .build();

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail namedOpDetail = getValidNamedOperation();

        given(cache.getNamedOperation(OP_NAME, user)).willReturn(namedOpDetail);
        given(context.getUser()).willReturn(user);

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> resolver.preExecute(wrongParamNameNamedOp, context))
                .withMessageContaining("Unexpected parameter name in NamedOperation");
    }

    @Test
    void shouldNotExecuteNamedOperationWithMissingRequiredArg(@Mock final User user,
                                                            @Mock final Context context,
                                                              @Mock final NamedOperationCache cache)
            throws CacheOperationException {
        // Given
        final NamedOperationResolver resolver = new NamedOperationResolver(cache);
        // Don't set any parameters
        final Map<String, Object> paramMap = new HashMap<>();

        final OperationChain<Object> badParamNamedOp = new OperationChain.Builder()
                        .first(new NamedOperation.Builder<>()
                                .name(OP_NAME)
                                .parameters(paramMap)
                                .build())
                        .build();

        // Make a real NamedOperationDetail with a parameter
        final NamedOperationDetail namedOpDetail = getValidNamedOperation();

        given(cache.getNamedOperation(OP_NAME, user)).willReturn(namedOpDetail);
        given(context.getUser()).willReturn(user);

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> resolver.preExecute(badParamNamedOp, context))
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

    private NamedOperationDetail getValidNamedOperation() {
        final ParameterDetail param = new ParameterDetail.Builder()
                .description("Limit param")
                .valueClass(Long.class)
                .required(true)
                .build();
        final Map<String, ParameterDetail> paramDetailMap = new HashMap<>();
        paramDetailMap.put("param1", param);

        final String opChainString = new JSONObject()
            .put("operations", new JSONArray()
                .put(new JSONObject()
                    .put("class", "uk.gov.gchq.gaffer.operation.impl.get.GetAllElements"))
                .put(new JSONObject()
                    .put("class", "uk.gov.gchq.gaffer.operation.impl.Limit")
                    .put("resultLimit", "${param1}")))
            .toString();

        // Make a real NamedOperationDetail with a parameter
        return new NamedOperationDetail.Builder()
                .operationName(OP_NAME)
                .description("standard operation")
                .operationChain(opChainString)
                .parameters(paramDetailMap)
                .build();
    }
}
