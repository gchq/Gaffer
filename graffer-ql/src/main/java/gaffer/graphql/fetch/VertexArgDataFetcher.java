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

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

/**
 * A vertex doesn't have data associated with it on it's own, so simply return the requested id.
 */
public class VertexArgDataFetcher implements DataFetcher {

    private final String key;

    public VertexArgDataFetcher(final String key) {
        this.key = key;
    }

    @Override
    public Object get(final DataFetchingEnvironment environment) {
        return environment.getArgument(key);
    }
}
