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
package uk.gov.gchq.gaffer.graph.hook;

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.operation.handler.ScoreOperationChainHandler;
import uk.gov.gchq.gaffer.user.User;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;

/*
 * An <code>OperationChainLimiter</code> is a {@link GraphHook} that checks a
 * user is authorised to execute an operation chain based on that user's maximum chain score and the configured score value for each operation in the chain.
 * This class requires a map of operation scores, these can be added using setOpScores(Map<Class,Integer>) or
 * addOpScore(Class, Integer...). Alternatively a properties file can be provided.
 * When using a properties file the last entry in the file that an operation can be assigned to will be the score that is used for that operation.
 * containing the operations and the score.
 * E.g if you put gaffer.operation.impl.add.AddElements = 8
 * And then gaffer.operation.impl.add = 1
 * The add elements will have a score of 1 not 8.
 * So make sure to write your properties file in class hierarchical order.
 *
 * This class also requires a map of authorisation scores,
 * this is the score value someone with that auth can have, the maximum score value of a users auths is used.
 * These can be added using setAuthScores(Map<String, Integer>) or
 * addAuthScore(String, Integer). Alternatively a properties file can be provided
 * containing the authorisations and the score.
 */
public class OperationChainLimiter implements GraphHook {
    public static final String OPERATION_SCORES_FILE_KEY = ScoreOperationChainHandler.OPERATION_SCORES_FILE_KEY;
    public static final String AUTH_SCORES_FILE_KEY = ScoreOperationChainHandler.AUTH_SCORES_FILE_KEY;

    private ScoreOperationChainHandler scorer;

    /**
     * Default constructor.
     * Use setOpScores or addOpScore to add operation scores.
     * And Use setAuthScores or
     * addAuthScore to add Authorisation scores.
     */
    public OperationChainLimiter() {
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the property file from the {@link Path} provided.
     *
     * @param operationScorePropertiesFileLocation         path to operation scores property file
     * @param operationAuthorisationScoreLimitFileLocation path to authorisation scores property file
     */
    public OperationChainLimiter(final Path operationScorePropertiesFileLocation, final Path operationAuthorisationScoreLimitFileLocation) {
        scorer = new ScoreOperationChainHandler(operationScorePropertiesFileLocation, operationAuthorisationScoreLimitFileLocation);
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the property file from the {@link InputStream} provided.
     *
     * @param operationScorePropertiesStream         input stream of operation scores property file
     * @param operationAuthorisationScoreLimitStream input stream of authorisation scores property file
     */
    public OperationChainLimiter(final InputStream operationScorePropertiesStream, final InputStream operationAuthorisationScoreLimitStream) {
        scorer = new ScoreOperationChainHandler(operationScorePropertiesStream, operationAuthorisationScoreLimitStream);
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the provided authorisations property file.
     *
     * @param operationScoreEntries                   operation scores entries
     * @param operationAuthorisationScoreLimitEntries authorisation scores entries
     */
    public OperationChainLimiter(final LinkedHashMap<String, String> operationScoreEntries,
                                 final LinkedHashMap<String, String> operationAuthorisationScoreLimitEntries) {
        scorer = new ScoreOperationChainHandler(operationScoreEntries, operationAuthorisationScoreLimitEntries);
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
            Integer chainScore = scorer.getChainScore(opChain, user);
            Integer maxAuthScore = scorer.getMaxUserAuthScore(user.getOpAuths());

            if (chainScore > maxAuthScore) {
                throw new UnauthorisedException("The maximum score limit for this user is " + maxAuthScore + ".\n" +
                        "The requested operation chain exceeded this score limit.");
            }
        }
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final User user) {
        // This method can be overridden to add additional authorisation checks on the results.
        return result;
    }
}
