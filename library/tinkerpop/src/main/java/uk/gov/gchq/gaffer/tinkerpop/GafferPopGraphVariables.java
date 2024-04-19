/*
 * Copyright 2016-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class GafferPopGraphVariables implements Graph.Variables {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopGraphVariables.class);
    private static final String VAR_UPDATE_ERROR_STRING = "Ignoring update variable: {}, incorrect value type: {}";

    /**
     * Variable key for the {@link Map} of Gaffer operation options.
     */
    public static final String OP_OPTIONS = "operationOptions";

    /**
     * Variable key for the list of data auths for the user interacting with the graph.
     */
    public static final String DATA_AUTHS = "dataAuths";

    /**
     * Variable key for the userId of who is interacting with the graph.
     */
    public static final String USER_ID = "userId";

    private final Map<String, Object> variables;

    public GafferPopGraphVariables(final Map<String, Object> variables) {
        this.variables = variables;
    }

    public GafferPopGraphVariables() {
        this.variables = new HashMap<>();
    }

    @Override
    public Set<String> keys() {
        return variables.keySet();
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) variables.get(key));
    }

    @Override
    public void remove(final String key) {
        variables.remove(key);
    }

    @Override
    public void set(final String key, final Object value) {
        LOGGER.debug("Updating graph variable: {} to {}", key, value);
        switch (key) {
            case OP_OPTIONS:
                if (value instanceof Iterable<?>) {
                    setOperationOptions((Iterable<String>) value);
                } else if (value instanceof Map) {
                    variables.put(key, value);
                } else {
                    LOGGER.error(VAR_UPDATE_ERROR_STRING, OP_OPTIONS, value.getClass());
                }
                break;

            case DATA_AUTHS:
                if (value instanceof String[]) {
                    variables.put(key, value);
                } else if (value instanceof String) {
                    variables.put(key, ((String) value).split(","));
                } else {
                    LOGGER.error(VAR_UPDATE_ERROR_STRING, DATA_AUTHS, value.getClass());
                }
                break;

            default:
                variables.put(key, value);
                break;
        }
    }

    /**
     * Sets the operation options key, attempts to convert to
     * a String {@link Map} by spitting each value on ':'.
     *
     * @param opOptions List of String key value pairs e.g. <pre> [ "key:value", "key2:value2" ] </pre>
     */
    public void setOperationOptions(final Iterable<String> opOptions) {
        Map<String, String> opOptionsMap = new HashMap<>();
        for (final String option : opOptions) {
            opOptionsMap.put(option.split(":")[0], option.split(":")[1]);
        }
        variables.put(OP_OPTIONS, opOptionsMap);
    }

    /**
     * Gets the operation options if available.
     *
     * @return Operation options map
     */
    public Map<String, String> getOperationOptions() {
        if (variables.containsKey(OP_OPTIONS)) {
            return (Map<String, String>) variables.get(OP_OPTIONS);
        }
        return new HashMap<>();
    }

    /**
     * Gets the list of data auths.
     *
     * @return List of data auths.
     */
    public String[] getDataAuths() {
        if (variables.containsKey(DATA_AUTHS)) {
            return (String[]) variables.get(DATA_AUTHS);
        }
        return new String[0];
    }

    /**
     * Gets the current user ID.
     *
     * @return The user ID
     */
    public String getUserId() {
        return (String) variables.get(USER_ID);
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
