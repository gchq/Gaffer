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

import uk.gov.gchq.gaffer.operation.impl.While;

/**
 * An {@code WhileScoreResolver} is an implementation of {@link ScoreResolver}
 * for the {@link While} operation.
 * <p>The score will be the maximum of the transform operation and the delegate operation,
 * multiplied by the minimum of the configured number of max repeats vs the global maximum
 * number of allowed repeats.</p>
 * <p>This is simply because the number of actual repetitions is nondeterministic,
 * therefore a "worst"-case scenario is considered.</p>
 */
public class WhileScoreResolver implements ScoreResolver<While> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WhileScoreResolver.class);

    @Override
    public Integer getScore(final While operation) {
        throw new UnsupportedOperationException("Default Score Resolver has not been provided.");
    }

    @Override
    public Integer getScore(final While operation, final ScoreResolver defaultScoreResolver) {
        if (null != operation) {
            Integer opScore = 0;

            if (null != operation.getConditional() && null != operation.getConditional().getTransform()) {
                opScore += defaultScoreResolver.getScore(operation.getConditional().getTransform());
            }

            opScore += defaultScoreResolver.getScore(operation.getOperation());

            return opScore * operation.getMaxRepeats();
        }

        LOGGER.warn("Cannot score a null operation");
        return null;
    }
}
