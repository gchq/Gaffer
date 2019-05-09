/*
 * Copyright 2017-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.resolver.named;

import com.google.common.collect.Maps;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.store.operation.resolver.DefaultScoreResolver;
import uk.gov.gchq.gaffer.store.operation.resolver.ScoreResolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class NamedOperationScoreResolverTest {

    @Test
    public void shouldGetScore() throws CacheOperationFailedException {
        final Integer expectedScore = 5;
        final String opName = "otherOp";

        final NamedOperation<Element, Iterable<? extends Element>> namedOp = mock(NamedOperation.class);
        namedOp.setOperationName(opName);
        final NamedOperationDetail namedOpDetail = mock(NamedOperationDetail.class);
        final NamedOperationCache cache = mock(NamedOperationCache.class);

        final NamedOperationScoreResolver resolver = new NamedOperationScoreResolver(cache);

        given(cache.getFromCache(namedOpDetail.getOperationName())).willReturn(namedOpDetail);
        given(namedOpDetail.getOperationName()).willReturn(opName);
        given(namedOpDetail.getScore()).willReturn(5);

        final Integer result = resolver.getScore(namedOp);

        assertEquals(expectedScore, result);
    }

    @Test
    public void shouldGetScoreFromOperationsInParameters() throws CacheOperationFailedException {

        //Given
        final Integer expectedScore = 8;

        final String opName = "otherOp";
        final NamedOperation<Element, Iterable<? extends Element>> namedOp = mock(NamedOperation.class);
        namedOp.setOperationName(opName);

        Operation operation = new GetAllElements();
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("test param", operation);
        namedOp.setParameters(paramMap);

        final Map<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        opScores.put(GetAllElements.class, 3);
        final ScoreResolver scoreResolver = new DefaultScoreResolver(opScores);

        final NamedOperationDetail namedOpDetail = mock(NamedOperationDetail.class);

        final NamedOperationCache cache = mock(NamedOperationCache.class);

        final NamedOperationScoreResolver resolver = new NamedOperationScoreResolver(cache);

        given(cache.getFromCache(namedOpDetail.getOperationName())).willReturn(namedOpDetail);
        given(namedOpDetail.getOperationName()).willReturn(opName);
        given(namedOpDetail.getScore()).willReturn(5);
        final List<Operation> operations = new ArrayList<>();
        operations.add(operation);
        given(namedOp.getOperations()).willReturn(operations);

        //When
        final Integer result = resolver.getScore(namedOp, scoreResolver);

        //Then
        assertEquals(expectedScore, result);
    }

    @Test
    public void shouldDefaultScoreFromOpChainIfNamedOpScoreEmpty() throws CacheOperationFailedException {
        // Given
        final String namedOpName = "namedOp";
        final Map parametersMap = new HashMap<>();
        final Integer expectedScore = 7;
        final NamedOperation<Element, Iterable<? extends Element>> namedOp = mock(NamedOperation.class);
        final NamedOperationCache cache = mock(NamedOperationCache.class);
        final ScoreResolver defaultScoreResolver = mock(DefaultScoreResolver.class);
        final NamedOperationDetail namedOpDetail = mock(NamedOperationDetail.class);
        final NamedOperationScoreResolver resolver = new NamedOperationScoreResolver(cache);
        final OperationChain opChain = new OperationChain();
        namedOp.setOperationName(namedOpName);
        namedOp.setParameters(parametersMap);

        given(namedOpDetail.getOperationChain(parametersMap)).willReturn(opChain);
        given(namedOpDetail.getScore()).willReturn(null);
        given(cache.getFromCache(namedOpDetail.getOperationName())).willReturn(namedOpDetail);
        given(defaultScoreResolver.getScore(opChain)).willReturn(7);

        // When
        final Integer result = resolver.getScore(namedOp, defaultScoreResolver);

        // Then
        assertEquals(expectedScore, result);
    }

    @Test
    public void shouldCatchExceptionForCacheFailures() {
        final NamedOperation<Element, Iterable<? extends Element>> namedOp = mock(NamedOperation.class);

        final NamedOperationScoreResolver resolver = new NamedOperationScoreResolver();

        final Integer result = resolver.getScore(namedOp);

        assertNull(result);
    }
}
