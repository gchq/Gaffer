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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.gaffer.operation.Operation;

/**
 * A <code>ScoreResolver</code> is used to retrieve the score associated with a provided {@link Operation}.
 * The implementations of {@link ScoreResolver} are used in the {@link uk.gov.gchq.gaffer.store.operation.handler.ScoreOperationChainHandler}.
 *
 * @param <T> the {@link Operation} type
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface ScoreResolver<T extends Operation> {
    /**
     * Should return a (nullable) score for a given operation.
     *
     * @param operation the provided operation for which the score should be resolved
     * @return the score for the operation, otherwise null if not found
     */
    Integer getScore(final T operation);


    /**
     * Should return a (nullable) score for a given operation.
     *
     * @param operation            the provided operation for which the score should be resolved
     * @param defaultScoreResolver the default score resolver to look up scores for nested operations
     * @return the score for the operation, otherwise null if not found
     */
    default Integer getScore(final T operation, final ScoreResolver defaultScoreResolver) {
        return getScore(operation);
    }
}
