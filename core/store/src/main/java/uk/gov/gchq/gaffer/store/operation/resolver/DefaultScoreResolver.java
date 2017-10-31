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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@code DefaultScoreResolver} is the default {@link ScoreResolver} that
 * returns the score based on a map of operation scores.
 */
public class DefaultScoreResolver implements ScoreResolver<Operation> {
    public static final int DEFAULT_OPERATION_SCORE = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultScoreResolver.class);

    private final Map<Class<? extends Operation>, Integer> opScores;

    public DefaultScoreResolver(final Map<Class<? extends Operation>, Integer> opScores) {
        if (null == opScores) {
            this.opScores = Collections.emptyMap();
        } else {
            this.opScores = opScores;
        }
    }

    @Override
    public Integer getScore(final Operation operation) {
        if (null != operation) {
            final Class<? extends Operation> opClass = operation.getClass();
            final List<Class<? extends Operation>> keys = new ArrayList<>(opScores.keySet());
            for (int i = keys.size() - 1; i >= 0; i--) {
                final Class<? extends Operation> key = keys.get(i);
                if (key.isAssignableFrom(opClass)) {
                    return opScores.get(key);
                }
            }
            LOGGER.warn("The operation '{}' was not found in the config file provided - the configured default value of {} will be used", operation.getClass().getName(), DEFAULT_OPERATION_SCORE);
        } else {
            LOGGER.warn("Cannot score a null operation");
        }
        return DEFAULT_OPERATION_SCORE;
    }
}
