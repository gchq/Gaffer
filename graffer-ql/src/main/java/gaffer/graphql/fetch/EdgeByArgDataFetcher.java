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

/**
 * Will fetch an edge based on the arguments to a query.
 */
public class EdgeByArgDataFetcher extends EdgeDataFetcher {

    public EdgeByArgDataFetcher(final String group) {
        super(group);
    }

    @Override
    protected String getVertex(final DataFetchingEnvironment environment) {
        return environment.getArgument(Constants.VERTEX);
    }

    @Override
    protected String getSource(final DataFetchingEnvironment environment) {
        return environment.getArgument(Constants.SOURCE);
    }

    @Override
    protected String getDestination(final DataFetchingEnvironment environment) {
        return environment.getArgument(Constants.DESTINATION);
    }
}
