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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.If;

import java.util.Collection;

public class IfScoreResolver implements ScoreResolver<If> {
    public static final int DEFAULT_OPERATION_SCORE = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(IfScoreResolver.class);
    @Override
    public Integer getScore(final If operation) {
        throw new UnsupportedOperationException("Default Score Resolver has not been provided.");
    }

    @Override
    public Integer getScore(final If operation, final ScoreResolver defaultScoreResolver) {
        if (null != operation) {
            final Collection<Operation> ops = operation.getOperations();
            Integer maxScore = DEFAULT_OPERATION_SCORE;

            for (final Operation op : ops) {
                final Integer score = defaultScoreResolver.getScore(op);
                maxScore = Integer.max(score, maxScore);
            }

            return maxScore;
        } else {
            LOGGER.warn("Cannot score a null operation");
        }

        return DEFAULT_OPERATION_SCORE;
    }
}
