
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
package gaffer.graph.hook;

import gaffer.commonutil.exception.UnauthorisedException;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.user.User;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/*
 * An <code>OperationChainLimiter</code> is a {@link GraphHook} that checks a
 * user is authorised to execute an operation chain based on that user's maximum chain score and the configured score value for each operaiton in the chain.
 * This class requires a map of operation scores, these can be added using setOpScores(Map<Class,Integer>) or
 * addOpScore(Class, Integer...). Alternatively a properties file can be provided.
 * When using a properties file the last entry in the file that an operation can be assigned to will be the score that is used for that operation.
 * E.g if you put gaffer.operation.impl.add.AddElements = 8
 * And then gaffer.operation.impl.add = 1
 * The add elements will have a score of 1 not 8.
 * So make sure to write your properties file in class hierarchical order.
 *
 * containing the operations and the score.
 * This class also requires a map of authorisation scores,
 * this is the score value someone with that auth can have, the maximum score value of a users auths is used.
 * These can be added using setAuthScores(Map<String, Integer>) or
 * addAuthScore(String, Integer). Alternatively a properties file can be provided
 * containing the authorisations and the score.
 */
public class OperationChainLimiter implements GraphHook {

    private static final int DEFAULT_OPERATION_SCORE = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationChainLimiter.class);
    private final Map<Class<? extends Operation>, Integer> operationScores = new LinkedHashMap<>();
    private final Map<String, Integer> authScores = new HashMap<>();

    /**
     * Default constructor. Use setOpScores(Map<Class, Integer>) or
     * addOpScore(Class, Integer) to add operation scores.
     * And Use setAuthScores(Map<String, Integer>) or
     * addAuthScore(String, Integer)
     */
    public OperationChainLimiter() {
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the property file from the {@link Path} provided.
     *
     * @param operationScorePropertiesFileLocation path to operation scores property file
     * @param operationAuthorisationScoreLimitFileLocation path to authorisation scores property file
     */
    public OperationChainLimiter(final Path operationScorePropertiesFileLocation, final Path operationAuthorisationScoreLimitFileLocation) {
        this(readProperties(operationScorePropertiesFileLocation), readProperties(operationAuthorisationScoreLimitFileLocation));
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the property file from the {@link InputStream} provided.
     *
     * @param operationScorePropertiesStream input stream of operation scores property file
     * @param operationAuthorisationScoreLimitStream input stream of authorisation scores property file
     */
    public OperationChainLimiter(final InputStream operationScorePropertiesStream, final InputStream operationAuthorisationScoreLimitStream) {
        this(readProperties(operationScorePropertiesStream), readProperties(operationAuthorisationScoreLimitStream));
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the provided authorisations property file.
     *
     * @param operationScorePropertiesFile authorisation scores property file
     * @param operationAuthorisationScoreLimitPropertiesFile authorisation scores property file
     */
    public OperationChainLimiter(final Properties operationScorePropertiesFile, final Properties operationAuthorisationScoreLimitPropertiesFile) {
        loadMapsFromProperties(operationScorePropertiesFile, operationAuthorisationScoreLimitPropertiesFile);
    }

    /**
     * Checks the {@link OperationChain}
     * is allowed to be executed by the user.
     * This is done by checking the user's auths against the auth scores getting the users maximum score limit value.
     * Then checking the operation score of all operations in the chain and comparing the total score value of the chain against a users maximum score limit.
     * If an operation cannot be executed then an {@link IllegalAccessError} is thrown.
     *
     * @param user    the user to authorise.
     * @param opChain the operation chain.
     */
    @Override
    public void preExecute(final OperationChain<?> opChain, final User user) {
        if (null != opChain) {
            Integer chainScore = 0;
            Integer maxRoleScore = getMaxUserRoleScore(user.getOpAuths());
            for (Operation operation : opChain.getOperations()) {
                chainScore += authorise(operation, user);
                if (chainScore > maxRoleScore) {
                    throw new UnauthorisedException("The maximum score limit for this user is " + maxRoleScore + ".\n" +
                            "The requested operation chain exceeded this score limit.");
                }
            }
        }
    }

    /**
     * Iterates through each of the users operation authorisations listed in the config file and returns the highest score
     * associated with those roles.
     * <p/>
     * Defaults to 0.
     *
     * @param opAuths a set of operation authorisations
     * @return maxUserScore the highest score associated with any of the supplied user roles
     */
    private Integer getMaxUserRoleScore(Set<String> opAuths) {
        Integer maxUserScore = 0;
        for (String opAuth : opAuths) {
            Integer roleScore = authScores.get(opAuth);
            if (null != roleScore) {
                if (roleScore > maxUserScore) {
                    maxUserScore = roleScore;
                }
            }
        }
        LOGGER.info("Returning users max operation chain limit score of " + maxUserScore);
        return maxUserScore;
    }

    @Override
    public void postExecute(final Object result, final OperationChain<?> opChain, final User user) {
        // This method can be overridden to add additional authorisation checks on the results.
    }

    protected Integer authorise(final Operation operation, final User user) {
        Integer operationScore = null;
        if (null != operation) {
            final Class<? extends Operation> opClass = operation.getClass();
            for (Map.Entry<Class<? extends Operation>, Integer> entry : operationScores.entrySet()) {
                if (entry.getKey().isAssignableFrom(opClass)) {
                       operationScore = entry.getValue();
                   }
                }
            }
        if(operationScore != null) {
            return operationScore;
        } else {
            LOGGER.warn("The operation '" + operation.getClass().getName() + "' was not found in the config file provided the configured default value of " + DEFAULT_OPERATION_SCORE + " will be used");
            return DEFAULT_OPERATION_SCORE;
        }
    }

    private static Properties readProperties(final Path propFileLocation) {
        Properties props;
        if (null != propFileLocation) {
            try {
                props = readProperties(Files.newInputStream(propFileLocation, StandardOpenOption.READ));
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            props = new Properties();
        }

        return props;
    }

    private static Properties readProperties(final InputStream stream) {
        final Properties props = new Properties();
        if (null != stream) {
            try {
                props.load(stream);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to load store properties file : " + e.getMessage(), e);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        }
        return props;
    }

    private void loadMapsFromProperties(final Properties operationScorePropertiesFile, final Properties operationAuthorisationScoreLimitPropertiesFile) {
        Map<Class<? extends Operation>, Integer> opScores = new LinkedHashMap<>();
        for (String opClassName : operationScorePropertiesFile.stringPropertyNames()) {
            final Class<? extends Operation> opClass;
            try {
                opClass = Class.forName(opClassName).asSubclass(Operation.class);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
            final Integer score = Integer.parseInt(operationScorePropertiesFile.getProperty(opClassName));
            opScores.put(opClass, score);
        }
        setOpScores(opScores);
        Map<String, Integer> authScores = new HashMap<>();
        for(String authName : operationAuthorisationScoreLimitPropertiesFile.stringPropertyNames()) {
            final Integer score = Integer.parseInt(operationAuthorisationScoreLimitPropertiesFile.getProperty(authName));
            authScores.put(authName, score);
        }
        setAuthScores(authScores);
    }

    /**
     * Set the operation scores.
     *
     * @param opScores a map of operation classes to scores
     */
    public void setOpScores(final Map<Class<? extends Operation>, Integer> opScores) {
        operationScores.putAll(opScores);
    }

    /**
     * Add a score for a given operation class.
     *
     * @param opClass the operation class
     * @param score   the score for the operation class
     */
    public void addOpScore(final Class<? extends Operation> opClass, final Integer score) {
        operationScores.put(opClass, score);
    }

    /**
     * Set the authorisation scores.
     *
     * @param authScores a map of authorisations to scores
     */
    public void setAuthScores(final Map<String, Integer> authScores) {
        this.authScores.putAll(authScores);
    }

    /**
     * Add a score for a given operation class.
     *
     * @param auth    the authorisation
     * @param score   the score for the operation class
     */
    public void addAuthScore(String auth, final Integer score) {
        authScores.put(auth, score);
    }
}