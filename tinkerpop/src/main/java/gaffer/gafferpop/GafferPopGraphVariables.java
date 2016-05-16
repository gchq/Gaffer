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
package gaffer.gafferpop;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class GafferPopGraphVariables implements Graph.Variables {
    /**
     * Variable key for the {@link gaffer.store.schema.Schema} object.
     */
    public static final String SCHEMA = "schema";

    /**
     * Variable key for the {@link Map} of Gaffer operation options.
     */
    public static final String OP_OPTIONS = "operationOptions";

    /**
     * The user who is interacting with the graph.
     */
    public static final String USER = "user";

    private final Map<String, Object> variables;

    public GafferPopGraphVariables(final ConcurrentHashMap<String, Object> variables) {
        this.variables = variables;
    }

    @Override
    public Set<String> keys() {
        return this.variables.keySet();
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) this.variables.get(key));
    }

    @Override
    public void remove(final String key) {
        this.variables.remove(key);
    }

    @Override
    public void set(final String key, final Object value) {
        throw new UnsupportedOperationException("These variables cannot be updated");
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
