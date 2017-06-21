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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Operation Handler for ScoreOperationChain
 */
@JsonDeserialize(builder = ScoreOperationChainHandler.Builder.class)
public class ScoreOperationChainHandler implements OutputOperationHandler<ScoreOperationChain, Integer> {
    public static final String OPERATION_SCORES_FILE_KEY = "gaffer.chain.limiter.operation.scores.path";
    public static final String AUTH_SCORES_FILE_KEY = "gaffer.chain.limiter.authorisation.scores.path";

    private static final int DEFAULT_OPERATION_SCORE = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreOperationChainHandler.class);

    private final LinkedHashMap<Class<? extends Operation>, Integer> operationScores = new LinkedHashMap<>();
    private final Map<String, Integer> authScores = new HashMap<>();

    public ScoreOperationChainHandler() throws OperationException {
        this((String) null, null);
    }

    public ScoreOperationChainHandler(final String operationScores, final String authScores) {
        loadMaps(operationScores, authScores);
    }

    public ScoreOperationChainHandler(final Path operationScores, final Path authScores) {
        loadMaps(operationScores, authScores);
    }

    public ScoreOperationChainHandler(final InputStream operationScorePropertiesStream, final InputStream operationAuthorisationScoreLimitStream) {
        loadMaps(operationScorePropertiesStream, operationAuthorisationScoreLimitStream);
    }

    public ScoreOperationChainHandler(final LinkedHashMap<String, String> operationScoreEntries,
                                      final LinkedHashMap<String, String> operationAuthorisationScoreLimitEntries) {
        loadMaps(operationScoreEntries, operationAuthorisationScoreLimitEntries);
    }

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
                chainScore += authorise(operation);
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
        LOGGER.info("Returning users max operation chain limit score of {}", maxUserScore);
        return maxUserScore;
    }

    protected int authorise(final Operation operation) {
        if (null != operation) {
            final Class<? extends Operation> opClass = operation.getClass();
            final List<Class<? extends Operation>> keys = new ArrayList<>(operationScores.keySet());
            for (int i = keys.size() - 1; i >= 0; i--) {
                final Class<? extends Operation> key = keys.get(i);
                if (key.isAssignableFrom(opClass)) {
                    return operationScores.get(key);
                }
            }
            LOGGER.warn("The operation '{}' was not found in the config file provided the configured default value of {} will be used", operation.getClass().getName(), DEFAULT_OPERATION_SCORE);
        } else {
            LOGGER.warn("A Null operation was passed to the OperationChainLimiter graph hook");
        }
        return DEFAULT_OPERATION_SCORE;
    }

    private void loadMaps(final String operationScores, final String authScores) {
        String operationScoresFileName = operationScores;
        String authScoresFileName = authScores;

        if (operationScoresFileName == null) {
            operationScoresFileName = (String) System.getProperties().get(OPERATION_SCORES_FILE_KEY);
        }

        if (authScoresFileName == null) {
            authScoresFileName = (String) System.getProperties().get(AUTH_SCORES_FILE_KEY);
        }

        if (authScoresFileName == null || operationScoresFileName == null) {
            throw new IllegalArgumentException("Auth or operation scores file names not found in system properties");
        }
        loadMaps(Paths.get(operationScoresFileName), Paths.get(authScoresFileName));
    }

    private void loadMaps(final Path operationScores, final Path authScores) {
        loadMaps(readEntries(operationScores), readEntries(authScores));
    }

    private void loadMaps(final InputStream operationScores, final InputStream authScores) {
        try {
            loadMaps(readEntries(operationScores), readEntries(authScores));
        } finally {
            CloseableUtil.close(operationScores, authScores);
        }
    }

    private void loadMaps(final LinkedHashMap<String, String> operationScoreEntries,
                          final LinkedHashMap<String, String> operationAuthorisationScoreLimitEntries) {
        final Map<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        for (final Map.Entry<String, String> opScoreEntry : operationScoreEntries.entrySet()) {
            final Class<? extends Operation> opClass;
            final String opClassName = opScoreEntry.getKey();
            try {
                opClass = Class.forName(opClassName)
                        .asSubclass(Operation.class);
            } catch (final ClassNotFoundException e) {
                LOGGER.error("An operation class could not be found for operation score property {}", opClassName, e);
                throw new IllegalArgumentException(e);
            }
            opScores.put(opClass, Integer.parseInt(opScoreEntry.getValue()));
        }
        operationScores.clear();
        operationScores.putAll(opScores);

        Map<String, Integer> authScores = new HashMap<>();
        for (final Map.Entry<String, String> authScoreEntry : operationAuthorisationScoreLimitEntries.entrySet()) {
            authScores.put(authScoreEntry.getKey(), Integer.parseInt(authScoreEntry.getValue()));
        }
        this.authScores.clear();
        this.authScores.putAll(authScores);
    }

    private static LinkedHashMap<String, String> readEntries(final Path propFileLocation) {
        final LinkedHashMap<String, String> map;
        if (null != propFileLocation) {
            try {
                map = readEntries(Files.newInputStream(propFileLocation, StandardOpenOption.READ));
            } catch (final IOException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            map = new LinkedHashMap<>(0);
        }

        return map;
    }

    private static LinkedHashMap<String, String> readEntries(final InputStream stream) {
        final LinkedHashMap<String, String> map = new LinkedHashMap<>();

        if (null != stream) {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(stream, CommonConstants.UTF_8));

                String line;
                while ((line = in.readLine()) != null) {
                    line = line.trim();

                    if (!line.startsWith("#")) {
                        String[] bits = line.split("=");
                        if (bits.length == 2) {
                            map.put(bits[0], bits[1]);
                        } else if (bits.length != 0) {
                            throw new IllegalArgumentException("Failed to load opScores file : invalid line:%n" + line);
                        }
                    }
                }
            } catch (final IOException e) {
                throw new IllegalArgumentException("Failed to load opScores file : " + e
                        .getMessage(), e);
            } finally {
                CloseableUtil.close(stream);
            }
        }
        return map;
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        private String authScoresFileName;
        private String operationScoresFileName;

        public Builder authScoresFileName(final String authScoresFileKey) {
            this.authScoresFileName = authScoresFileKey;
            return this;
        }

        public Builder operationScoresFileName(final String operationScoresFileKey) {
            this.operationScoresFileName = operationScoresFileKey;
            return this;
        }

        public ScoreOperationChainHandler build() throws OperationException {
            return new ScoreOperationChainHandler(operationScoresFileName, authScoresFileName);
        }
    }
}
