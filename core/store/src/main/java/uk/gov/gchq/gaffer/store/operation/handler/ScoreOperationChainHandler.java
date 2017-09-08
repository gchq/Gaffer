/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Operation Handler for ScoreOperationChain
 */
public class ScoreOperationChainHandler implements OutputOperationHandler<ScoreOperationChain, Integer> {
    private static final int DEFAULT_OPERATION_SCORE = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreOperationChainHandler.class);

    private final LinkedHashMap<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
    private final Map<String, Integer> authScores = new HashMap<>();

    /**
     * Returns the OperationChainLimiter score for the OperationChain provided.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return a Long containing the score
     * @throws OperationException thrown if the property keys have not been set
     */
    @Override
    public Integer doOperation(final ScoreOperationChain operation, final Context context, final Store store) throws OperationException {
        return getChainScore(operation.getOperationChain(), context.getUser());
    }

    public int getChainScore(final OperationChain<?> opChain, final User user) {
        int chainScore = 0;

        if (null != opChain) {
            for (final Operation operation : opChain.getOperations()) {
                if (operation instanceof OperationChain) {
                    chainScore += getChainScore((OperationChain<?>) operation, user);
                } else {
                    chainScore += authorise(operation);
                }
            }
        }
        return chainScore;
    }

    /**
     * Iterates through each of the users operation authorisations listed in the config file and returns the highest score
     * associated with those auths.
     * <p>
     * Defaults to 0.
     *
     * @param opAuths a set of operation authorisations
     * @return maxUserScore the highest score associated with any of the supplied user auths
     */
    public int getMaxUserAuthScore(final Set<String> opAuths) {
        Integer maxUserScore = 0;
        for (final String opAuth : opAuths) {
            Integer authScore = authScores.get(opAuth);
            if (null != authScore) {
                if (authScore > maxUserScore) {
                    maxUserScore = authScore;
                }
            }
        }
        LOGGER.debug("Returning users max operation chain limit score of {}", maxUserScore);
        return maxUserScore;
    }

    protected int authorise(final Operation operation) {
        if (null != operation) {
            final Class<? extends Operation> opClass = operation.getClass();
            final List<Class<? extends Operation>> keys = new ArrayList<>(opScores.keySet());
            for (int i = keys.size() - 1; i >= 0; i--) {
                final Class<? extends Operation> key = keys.get(i);
                if (key.isAssignableFrom(opClass)) {
                    return opScores.get(key);
                }
            }
            LOGGER.warn("The operation '{}' was not found in the config file provided the configured default value of {} will be used", operation.getClass().getName(), DEFAULT_OPERATION_SCORE);
        } else {
            LOGGER.warn("A Null operation was passed to the OperationChainLimiter graph hook");
        }
        return DEFAULT_OPERATION_SCORE;
    }

    public Map<Class<? extends Operation>, Integer> getOpScores() {
        return Collections.unmodifiableMap(opScores);
    }

    public void setOpScores(final Map<Class<? extends Operation>, Integer> opScores) {
        this.opScores.clear();
        if (null != opScores) {
            this.opScores.putAll(opScores);
        }
    }

    @JsonGetter("opScores")
    public Map<String, Integer> getOpScoresAsStrings() {
        final Map<String, Integer> opScoresAsNames = new LinkedHashMap<>(opScores.size());
        CollectionUtil.toMapWithStringKeys(opScores, opScoresAsNames);
        return Collections.unmodifiableMap(opScoresAsNames);
    }

    @JsonSetter("opScores")
    public void setOpScoresFromStrings(final Map<String, Integer> opScores) throws ClassNotFoundException {
        this.opScores.clear();
        CollectionUtil.toMapWithClassKeys(opScores, this.opScores);
        validateOpScores();
    }

    public Map<String, Integer> getAuthScores() {
        return Collections.unmodifiableMap(authScores);
    }

    public void setAuthScores(final Map<String, Integer> authScores) {
        this.authScores.clear();
        if (null != authScores) {
            this.authScores.putAll(authScores);
        }
        validateOpScores();
    }

    public void validateOpScores() {
        final List<Class<? extends Operation>> ops = new ArrayList<>(opScores.keySet());
        int i = 0;
        for (final Class<? extends Operation> op : ops) {
            for (int j = 0; j < i; j++) {
                if (op.isAssignableFrom(ops.get(j))) {
                    throw new IllegalArgumentException(
                            "Operation scores are configured incorrectly. "
                                    + " The operation " + op.getSimpleName()
                                    + " is a parent operation of " + ops.get(j).getSimpleName()
                                    + " so the score of " + ops.get(j).getSimpleName()
                                    + " can never be accessed."
                    );
                }
            }
            i++;
        }
    }
}
