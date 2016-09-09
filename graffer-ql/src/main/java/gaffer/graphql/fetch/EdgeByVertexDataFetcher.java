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
package gaffer.graphql.fetch;

import gaffer.graphql.definitions.Constants;
import graphql.schema.DataFetchingEnvironment;

import java.util.Map;

/**
 * Fetch edges based on the source object, which we expect to be a vertex.
 */
public class EdgeByVertexDataFetcher extends EdgeDataFetcher {

    private final boolean outgoing;

    public EdgeByVertexDataFetcher(final String group, final boolean outgoing) {
        super(group);
        this.outgoing = outgoing;
    }

    @Override
    protected String getVertex(final DataFetchingEnvironment environment) {
        return null;
    }

    @Override
    protected String getSource(final DataFetchingEnvironment environment) {
        return outgoing ? getValue(environment, Constants.VALUE) : null;
    }

    @Override
    protected String getDestination(final DataFetchingEnvironment environment) {
        return outgoing ? null : getValue(environment, Constants.VALUE);
    }

    private String getValue(final DataFetchingEnvironment environment, final String key) {
        final Map<String, Object> source = (Map<String, Object>) environment.getSource();
        return source.get(key).toString();
    }
}
