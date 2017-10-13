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
package uk.gov.gchq.gaffer.graph.hook;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.ScoreOperationChainHandler;
import uk.gov.gchq.gaffer.store.operation.resolver.ScoreResolver;

import java.util.Map;

/**
 * An {@code OperationChainLimiter} is a {@link GraphHook} that checks a
 * user is authorised to execute an operation chain based on that user's maximum chain score and the configured score value for each operation in the chain.
 * This class requires a map of operation scores.
 * When using a properties file the last entry in the file that an operation can be assigned to will be the score that is used for that operation.
 * containing the operations and the score.
 * E.g if you put gaffer.operation.impl.add.AddElements = 8
 * And then gaffer.operation.impl.add = 1
 * The add elements will have a score of 1 not 8.
 * So make sure to write your properties file in class hierarchical order.
 *
 * This class also requires a map of authorisation scores,
 * this is the score value someone with that auth can have, the maximum score value of a users auths is used.
 *
 * The class delegates the logic to {@link ScoreOperationChainHandler}. If you
 * wish to use the {@link uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain} operation and this graph hook,
 * then both need to have the same score configuration.
 */
public class OperationChainLimiter implements GraphHook {
    private ScoreOperationChainHandler scorer = new ScoreOperationChainHandler();

    /**
     * Checks the {@link OperationChain} is allowed to be executed by the user.
     * This is done by checking the user's auths against the auth scores getting the users maximum score limit value.
     * Then checking the operation score of all operations in the chain and comparing the total score value of the chain against a users maximum score limit.
     * If an operation cannot be executed then an {@link IllegalAccessError} is thrown.
     *
     * @param context    the Context containing the user to authorise.
     * @param opChain the operation chain.
     */
    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (null != opChain) {
            Integer chainScore = scorer.getChainScore(opChain, context.getUser());
            Integer maxAuthScore = scorer.getMaxUserAuthScore(context.getUser().getOpAuths());

            if (chainScore > maxAuthScore) {
                throw new UnauthorisedException("The maximum score limit for user: " +
                        context.getUser().toString() + " is " + maxAuthScore + ".\n" +
                        "The requested operation chain exceeded this score limit.");
            }
        }
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        // This method can be overridden to add additional authorisation checks on the results.
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    public Map<Class<? extends Operation>, Integer> getOpScores() {
        return scorer.getOpScores();
    }

    public void setOpScores(final Map<Class<? extends Operation>, Integer> opScores) {
        scorer.setOpScores(opScores);
    }

    @JsonGetter("opScores")
    public Map<String, Integer> getOpScoresAsStrings() {
        return scorer.getOpScoresAsStrings();
    }

    @JsonSetter("opScores")
    public void setOpScoresFromStrings(final Map<String, Integer> opScores) throws ClassNotFoundException {
        scorer.setOpScoresFromStrings(opScores);
    }

    public Map<String, Integer> getAuthScores() {
        return scorer.getAuthScores();
    }

    public void setAuthScores(final Map<String, Integer> authScores) {
        scorer.setAuthScores(authScores);
    }
    public Map<Class<? extends Operation>, ScoreResolver> getScoreResolvers() {
        return scorer.getScoreResolvers();
    }

    public void setScoreResolvers(final Map<Class<? extends Operation>, ScoreResolver> resolvers) {
        scorer.setScoreResolvers(resolvers);
    }
}
