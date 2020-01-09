/*
 * Copyright 2019 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.impl.predicate.Or;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@JsonPropertyOrder(value = { "unauthorisedFunctions", "unauthorisedFunctionPatterns"})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FunctionAuthoriser implements GraphHook {


    private static final String ERROR_MESSAGE_PREFIX = "Operation chain contained an unauthorised function: ";

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionAuthoriser.class);
    private List<Class<? extends Function>> unauthorisedFunctions = new ArrayList<>();
    private List<Pattern> authorisedFunctionPatterns = new ArrayList<>();

    public FunctionAuthoriser() {
    }

    public FunctionAuthoriser(final List<Class< ? extends Function>> unauthorisedFunctions, final List<Pattern> authorisedFunctionPatterns) {
        this.setUnauthorisedFunctions(unauthorisedFunctions);
        this.setAuthorisedFunctionPatterns(authorisedFunctionPatterns);
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        String chainString;
        try {
            chainString = new String(JSONSerialiser.serialise(opChain));
        } catch (final SerialisationException e) {
            // This should never happen in real life as operation chains should
            // always be json serialisable. However this could happen if using a
            // mock in testing. To account for this, it will be logged.
            LOGGER.warn("Failed to serialise operation chain: " + opChain);
            return;
        }

        if (unauthorisedFunctions != null) {
            checkNoBlacklistedFunctionsArePresent(chainString);
        }
        if (authorisedFunctionPatterns != null) {
            checkAllFunctionsUsedAppearInWhitelist(chainString);
        }
    }

    private void checkAllFunctionsUsedAppearInWhitelist(final String chainString) {
        if (authorisedFunctionPatterns.size() == 0) {
            return;
        }

        int lastIndexMatched = chainString.indexOf("\"class\":\"");

        while (lastIndexMatched != -1) {
            // find the next index of \"class\":
            lastIndexMatched += 9; // last index is now at the start of the class name
            int endIndex = chainString.indexOf('"', lastIndexMatched);
            String className = chainString.substring(lastIndexMatched, endIndex);
            lastIndexMatched = chainString.indexOf("\"class\":\"", lastIndexMatched);
            Class clazz;
            try {
                clazz = Class.forName(className);
            } catch (final ClassNotFoundException e) {
                // This can only happen if there is some kind of class field which isn't a java class
                // As this is technically possible, log it but continue.
                LOGGER.warn("Operation chain contained a class field which didn't relate to a java class: " + className);
                continue;
            }

            if (Function.class.isAssignableFrom(clazz)) { // then we must check it exists in the whitelist
                // Create an or predicate from the patterns
                List<Predicate> patterns = authorisedFunctionPatterns.stream()
                        .map(Pattern::asPredicate)
                        .collect(Collectors.toList());
                Or<String> or = new Or<>(patterns);
                if (!or.test(className)) {
                    throw new UnauthorisedException(ERROR_MESSAGE_PREFIX + className);
                }
            }
        }
    }

    private void checkNoBlacklistedFunctionsArePresent(final String chainString) {
        for (final Class<? extends Function> blacklistedFunction : unauthorisedFunctions) {
            if (chainString.contains(blacklistedFunction.getName())) {
                throw new UnauthorisedException(ERROR_MESSAGE_PREFIX +
                        blacklistedFunction.getName());
            }
        }
    }

    public List<Class<? extends Function>> getUnauthorisedFunctions() {
        return unauthorisedFunctions;
    }

    public void setUnauthorisedFunctions(final List<Class<? extends Function>> unauthorisedFunctions) {
        this.unauthorisedFunctions = unauthorisedFunctions;
    }

    public List<Pattern> getAuthorisedFunctionPatterns() {
        return authorisedFunctionPatterns;
    }

    public void setAuthorisedFunctionPatterns(final List<Pattern> authorisedFunctionPatterns) {
        this.authorisedFunctionPatterns = authorisedFunctionPatterns;
    }

    public static class Builder {
        private FunctionAuthoriser authoriser = new FunctionAuthoriser();

        public Builder authorisedPatterns(final List<Pattern> authorisedPatterns) {
            authoriser.setAuthorisedFunctionPatterns(authorisedPatterns);
            return this;
        }

        public Builder authorsisedPatterns(final List<String> authorisedPatterns) {
            authoriser.setAuthorisedFunctionPatterns(authorisedPatterns
                    .stream()
                    .map(Pattern::compile)
                    .collect(Collectors.toList()));
            return this;
        }

        public Builder unauthorisedFunctions(final List<Class<? extends Function>> unauthorisedFunctions) {
            authoriser.setUnauthorisedFunctions(unauthorisedFunctions);
            return this;
        }

        public FunctionAuthoriser build() {
            return authoriser;
        }
    }
}