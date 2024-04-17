/*
 * Copyright 2016-2023 Crown Copyright
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

import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class GafferPopGraphVariables implements Graph.Variables {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopGraphVariables.class);
    private static final String VAR_UPDATE_ERROR_STRING = "Ignoring update variable: {}, incorrect value type: {}";

    /**
     * Variable key for the {@link uk.gov.gchq.gaffer.store.schema.Schema} object.
     */
    public static final String SCHEMA = "schema";

    /**
     * Variable key for the {@link Map} of Gaffer operation options.
     */
    public static final String OP_OPTIONS = "operationOptions";

    /**
     * Variable key for the userId of who is interacting with the graph.
     */
    public static final String USER_ID = "userId";

    /**
     * Variable key for the Graph ID of the graph to interact with.
     */
    public static final String GRAPH_ID = "graphId";

    private final Map<String, Object> variables;

    public GafferPopGraphVariables(final Map<String, Object> variables) {
        this.variables = variables;
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
        switch (key) {
            case OP_OPTIONS:
                if (value instanceof Iterable<?>) {
                    setOperationOptions((Iterable<String>) value);
                } else {
                    LOGGER.error(VAR_UPDATE_ERROR_STRING, OP_OPTIONS, value.getClass());
                }
                break;

            case SCHEMA:
                if (value instanceof Schema) {
                    variables.put(key, value);
                } else {
                    LOGGER.error(VAR_UPDATE_ERROR_STRING, SCHEMA, value.getClass());
                }
                break;

            default:
                LOGGER.info("Updating: {} to {}", key, value);
                variables.put(key, value);
                break;
        }
    }

    public void setOperationOptions(Iterable<String> opOptions) {
        Map<String, String> opOptionsMap = new HashMap<>();
        for (String option : opOptions) {
            opOptionsMap.put(option.split(":")[0], option.split(":")[1]);
        }
        variables.put(OP_OPTIONS, opOptionsMap);
    }

    public Map<String, String> getOperationOptions() {
        return (Map<String, String>) variables.get(OP_OPTIONS);
    }

    public String getUserId() {
        return (String) variables.get(USER_ID);
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
