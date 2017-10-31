/*
 * Copyright 2017 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;

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
    public void shouldCatchExceptionForCacheFailures() {
        final NamedOperation<Element, Iterable<? extends Element>> namedOp = mock(NamedOperation.class);

        final NamedOperationScoreResolver resolver = new NamedOperationScoreResolver();

        final Integer result = resolver.getScore(namedOp);

        assertNull(result);
    }
}
