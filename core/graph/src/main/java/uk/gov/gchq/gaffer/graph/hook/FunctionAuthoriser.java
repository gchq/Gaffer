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
import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The FunctionAuthoriser is a {@link GraphHook} which stops a user running
 * Functions which have been banned. The Authoriser can be configured with a
 * list of unauthorised function classes or patterns, or a list of authorised
 * patterns to check against.
 * <p>
 * It should be noted that using the unauthorisedFunctions list will be more
 * efficient than using any of the patterns.
 */
@JsonPropertyOrder(value = {"unauthorisedFunctions", "unauthorisedFunctionPatterns", "authorisedFunctionPatterns"})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FunctionAuthoriser implements GraphHook {

    private static final String ERROR_MESSAGE_PREFIX = "Operation chain contained an unauthorised function: ";
    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionAuthoriser.class);
    private List<Class<? extends Function>> unauthorisedFunctions = new ArrayList<>();
    private List<Pattern> authorisedFunctionPatterns = new ArrayList<>();
    private List<Pattern> unauthorisedFunctionPatterns = new ArrayList<>();

    public FunctionAuthoriser() {
    }

    public FunctionAuthoriser(final List<Class<? extends Function>> unauthorisedFunctions, final List<Pattern> unauthorisedFunctionPatterns, final List<Pattern> authorisedFunctionPatterns) {
        this.setUnauthorisedFunctions(unauthorisedFunctions);
        this.setAuthorisedFunctionPatterns(authorisedFunctionPatterns);
        this.setUnauthorisedFunctionPatterns(unauthorisedFunctionPatterns);
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
        if (authorisedFunctionPatterns != null || unauthorisedFunctionPatterns != null) {
            checkAllFunctionsUsedAppearInPatterns(chainString);
        }
    }

    private List<Predicate> convertPatternsToPredicates(final List<Pattern> patterns) {
        return patterns.stream()
                .map(Pattern::asPredicate)
                .collect(Collectors.toList());
    }

    private void checkAllFunctionsUsedAppearInPatterns(final String chainString) {
        boolean noWhitelistPatterns = (authorisedFunctionPatterns == null || authorisedFunctionPatterns.isEmpty());
        boolean noBlacklistPatterns = (unauthorisedFunctionPatterns == null || unauthorisedFunctionPatterns.isEmpty());

        Predicate<String> isAuthorised = createAuthorisationFunction(noBlacklistPatterns, noWhitelistPatterns);

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
                if (!isAuthorised.test(className)) {
                    throw new UnauthorisedException(ERROR_MESSAGE_PREFIX + className);
                }
            }
        }
    }

    private Predicate<String> createAuthorisationFunction(final boolean noBlacklistPatterns, final boolean noWhitelistPatterns) {
        List<Predicate> predicateComponents = new ArrayList<>();

        if (!noBlacklistPatterns) {
            predicateComponents.add(new Not<>(new Or<>(convertPatternsToPredicates(unauthorisedFunctionPatterns))));
        }
        if (!noWhitelistPatterns) {
            predicateComponents.add(new Or<>(convertPatternsToPredicates(authorisedFunctionPatterns)));
        }

        return new And<>(predicateComponents);
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

    public List<Pattern> getUnauthorisedFunctionPatterns() {
        return unauthorisedFunctionPatterns;
    }

    public void setUnauthorisedFunctionPatterns(final List<Pattern> unauthorisedFunctionPatterns) {
        this.unauthorisedFunctionPatterns = unauthorisedFunctionPatterns;
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

        public Builder unauthorisedPatterns(final List<Pattern> unauthorisedPatterns) {
            authoriser.setAuthorisedFunctionPatterns(unauthorisedPatterns);
            return this;
        }

        public Builder unauthorsisedPatterns(final List<String> unauthorisedPatterns) {
            authoriser.setAuthorisedFunctionPatterns(unauthorisedPatterns
                    .stream()
                    .map(Pattern::compile)
                    .collect(Collectors.toList()));
            return this;
        }

        public FunctionAuthoriser build() {
            return authoriser;
        }
    }
}
