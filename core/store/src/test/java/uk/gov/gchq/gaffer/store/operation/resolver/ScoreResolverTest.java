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
package uk.gov.gchq.gaffer.store.operation.resolver;

import org.junit.Test;

import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;

public abstract class ScoreResolverTest {

    /**
     * Implementation should test that the Resolver returns the correct score for a given Operation of compatible type.
     */
    @Test
    public abstract void shouldGetScore() throws CacheOperationFailedException;

    /**
     * Implementation should test that if the Cache fails to retrieve the score for the given Operation, then it should also handle
     * the resulting {@link CacheOperationFailedException}, and return a default value.
     */
    @Test
    public abstract void shouldCatchExceptionForCacheFailures();
}
