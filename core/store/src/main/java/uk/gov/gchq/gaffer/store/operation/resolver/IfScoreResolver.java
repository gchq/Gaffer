/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.impl.If;

import static uk.gov.gchq.gaffer.store.operation.resolver.DefaultScoreResolver.DEFAULT_OPERATION_SCORE;

/**
 * An {@code IfScoreResolver} is an implementation of {@link ScoreResolver}
 * for the {@link If} operation.
 *
 * <p>The score will be the maximum of the operations contained within the If operation,
 * regardless of which of the two possibilities are executed.</p>
 */
public class IfScoreResolver implements ScoreResolver<If> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IfScoreResolver.class);

    @Override
    public Integer getScore(final If operation) {
        throw new UnsupportedOperationException("Default Score Resolver has not been provided.");
    }

    @Override
    public Integer getScore(final If operation, final ScoreResolver defaultScoreResolver) {
        if (null != operation) {
            Integer conditionalScore = DEFAULT_OPERATION_SCORE;

            if (null != operation.getConditional() && null != operation.getConditional().getTransform()) {
                conditionalScore = Integer.max(defaultScoreResolver.getScore(
                        operation.getConditional().getTransform()), conditionalScore);
            }

            Integer opScore = DEFAULT_OPERATION_SCORE;

            if (null != operation.getThen()) {
                opScore = Integer.max(defaultScoreResolver.getScore(
                        operation.getThen()), opScore);
            }

            if (null != operation.getOtherwise()) {
                opScore = Integer.max(defaultScoreResolver.getScore(
                        operation.getOtherwise()), opScore);
            }

            return conditionalScore + opScore;
        }

        LOGGER.warn("Cannot score a null operation");
        return DEFAULT_OPERATION_SCORE;
    }
}
