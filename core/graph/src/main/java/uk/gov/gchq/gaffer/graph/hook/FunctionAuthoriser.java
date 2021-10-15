/*
 * Copyright 2020 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameCache;

import java.util.List;
import java.util.function.Function;

/**
 * The FunctionAuthoriser is a {@link GraphHook} which stops a user running
 * Functions which have been banned. The Authoriser can be configured with
 * unauthorised function classes.
 */
@JsonPropertyOrder(alphabetic = true)
public class FunctionAuthoriser implements GraphHook {

    private static final String ERROR_MESSAGE_PREFIX = "Operation chain contained an unauthorised function: ";
    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionAuthoriser.class);

    private List<Class<? extends Function>> unauthorisedFunctions;

    public FunctionAuthoriser() {
    }

    public FunctionAuthoriser(final List<Class<? extends Function>> unauthorisedFunctions) {
        this.setUnauthorisedFunctions(unauthorisedFunctions);
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (unauthorisedFunctions == null || unauthorisedFunctions.isEmpty()) {
            return;
        }

        Object input = null;
        // Null the input to avoid serialising potentially large inputs
        if (opChain.getOperations().size() > 0 && opChain.getOperations().get(0) instanceof Input) {
            input = ((Input) opChain.getOperations().get(0)).getInput();
            ((Input) opChain.getOperations().get(0)).setInput(null);
        }

        SimpleClassNameCache.setUseFullNameForSerialisation(true);
        String chainString;
        try {
            chainString = new String(JSONSerialiser.serialise(opChain));
        } catch (final SerialisationException e) {
            // This should never happen in real life as operation chains should
            // always be json serialisable. However this could happen if using a
            // mock in testing. To account for this, it will be logged.
            LOGGER.warn("Failed to serialise operation chain: " + opChain + " due to " + e.getMessage());
            return;
        } finally {
            if (input != null) {
                // The only way input could have been set to non null value would
                // be if the first operation was an Input operation.
                ((Input) opChain.getOperations().get(0)).setInput(input);
            }
        }

        checkNoUnauthorisedFunctionsArePresent(chainString);
    }

    private void checkNoUnauthorisedFunctionsArePresent(final String chainString) {
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
}
