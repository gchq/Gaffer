/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;

/**
 * A {@code GraphHookPath} allows GraphHooks to be defined as paths to other graph hooks.
 * This should never actually be added to a Graph and should never be executed.
 */
@JsonPropertyOrder(alphabetic = true)
public class GraphHookPath implements GraphHook {
    private static final String ERROR_MSG = "This " + GraphHookPath.class.getSimpleName() + " graph hook should not be executed";
    private String path;

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    public String getPath() {
        return path;
    }

    public void setPath(final String path) {
        this.path = path;
    }
}
