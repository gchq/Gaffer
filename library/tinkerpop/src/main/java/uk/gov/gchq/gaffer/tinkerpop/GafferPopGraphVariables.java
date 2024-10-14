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

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;

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
     * Variable key for the list of data auths for the default user.
     */
    public static final String DATA_AUTHS = "dataAuths";

    /**
     * Variable key for the userId used for constructing a default user.
     */
    public static final String USER_ID = "userId";

    /**
     * Variable key for the user who is interacting with the graph.
     */
    public static final String USER = "user";

    /**
     * The max number of elements that can be returned by GetElements
     */
    public static final String GET_ELEMENTS_LIMIT = "getElementsLimit";

    /**
     * When to apply HasStep filtering
     */
    public static final String HAS_STEP_FILTER_STAGE = "hasStepFilterStage";

    /**
     * Key used in a with step to include a opencypher query traversal
     */
    public static final String CYPHER_KEY = "cypher";

    /**
     * The variable with the last Gaffer operation chain that was ran from the Gremlin query
     */
    public static final String LAST_OPERATION_CHAIN = "lastOperation";


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
                    LOGGER.error(VAR_UPDATE_ERROR_STRING, key, value.getClass());
                }
                break;

            case USER:
                if (value instanceof User) {
                    variables.put(key, value);
                } else {
                    LOGGER.error(VAR_UPDATE_ERROR_STRING, key, value.getClass());
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

    public User getUser() {
        return (User) variables.get(USER);
    }

    public Integer getElementsLimit() {
        return (Integer) variables.get(GET_ELEMENTS_LIMIT);
    }

    public String getHasStepFilterStage() {
        return (String) variables.get(HAS_STEP_FILTER_STAGE);
    }

    public OperationChain<?> getLastOperationChain() {
        return (OperationChain) variables.get(LAST_OPERATION_CHAIN);
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
