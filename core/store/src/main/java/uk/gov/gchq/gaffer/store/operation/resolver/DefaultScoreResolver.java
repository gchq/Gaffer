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

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;

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

    private final Map<Class<? extends Operation>, ScoreResolver> scoreResolvers;
    private final Map<Class<? extends Operation>, Integer> opScores;

    /**
     * The skipResolvingOperation field is used to prevent recursively calling
     * getScore on the same resolver with the same operation. If getScore
     * is called with exactly the same instance as this field, then we do not
     * use any of the score resolvers.
     */
    private final Operation skipResolvingOperation;

    public DefaultScoreResolver() {
        this(null);
    }

    public DefaultScoreResolver(final Map<Class<? extends Operation>, Integer> opScores) {
        this(opScores, null);
    }

    public DefaultScoreResolver(final Map<Class<? extends Operation>, Integer> opScores,
                                final Map<Class<? extends Operation>, ScoreResolver> scoreResolvers) {
        this(opScores, scoreResolvers, null);
    }

    public DefaultScoreResolver(final Map<Class<? extends Operation>, Integer> opScores,
                                final Map<Class<? extends Operation>, ScoreResolver> scoreResolvers,
                                final Operation skipResolvingOperation) {
        if (null == opScores) {
            this.opScores = Collections.emptyMap();
        } else {
            this.opScores = opScores;
        }

        if (null == scoreResolvers) {
            this.scoreResolvers = Collections.emptyMap();
        } else {
            this.scoreResolvers = scoreResolvers;
        }

        this.skipResolvingOperation = skipResolvingOperation;
    }

    @Override
    public Integer getScore(final Operation operation) {
        if (null == operation) {
            return 0;
        }

        if (operation instanceof Operations) {
            int score = 0;
            for (final Operation op : ((Operations<?>) operation).getOperations()) {
                score += getScore(op);
            }
            return score;
        } else {
            final Class<? extends Operation> opClass = operation.getClass();

            // The skipResolvingOperation is used to prevent recursively calling getScore on the same resolver with the same operation.
            // Check the operation is not the same instance
            if (null == skipResolvingOperation || skipResolvingOperation != operation) {
                ScoreResolver resolver = scoreResolvers.get(opClass);
                if (null != resolver) {
                    // Create a delegate resolver with the current resolver skipped to prevent an infinite loop
                    final DefaultScoreResolver delegateResolver = new DefaultScoreResolver(opScores, scoreResolvers, operation);
                    Integer opScore = resolver.getScore(operation, delegateResolver);
                    if (null != opScore) {
                        return opScore;
                    }
                }
            }

            // Use the operation scores map to get the score.
            final List<Class<? extends Operation>> keys = new ArrayList<>(opScores.keySet());
            for (int i = keys.size() - 1; i >= 0; i--) {
                final Class<? extends Operation> key = keys.get(i);
                if (key.isAssignableFrom(opClass)) {
                    return opScores.get(key);
                }
            }
        }

        return DEFAULT_OPERATION_SCORE;
    }
}
