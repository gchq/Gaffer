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

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;

import java.util.List;
import java.util.function.Function;

public class FunctionAuthoriser implements GraphHook {

    private List<Class<? extends Function>> blacklistedFunctions;

    public FunctionAuthoriser() {
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        String chainString;
        try {
            chainString = new String(JSONSerialiser.serialise(opChain));
        } catch (final SerialisationException e) {
            // This should never happen as operation chains should always be
            // json serialisable
            throw new RuntimeException(e);
        }

        for (final Class<? extends Function> blacklistedFunction : blacklistedFunctions) {
            if (chainString.contains(blacklistedFunction.getName())) {
                throw new UnauthorisedException("Operation contains the " +
                        blacklistedFunction.getName() + " Function which is " +
                        "not allowed");
            }
        }

    }

    public List<Class<? extends Function>> getBlacklistedFunctions() {
        return blacklistedFunctions;
    }

    public void setBlacklistedFunctions(final List<Class<? extends Function>> blacklistedFunctions) {
        this.blacklistedFunctions = blacklistedFunctions;
    }
}
