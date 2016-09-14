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

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.graphql.definitions.Constants;
import gaffer.operation.OperationChain;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import graphql.schema.DataFetchingEnvironment;

import java.util.Map;

/**
 * A Data Fetcher that uses a Gaffer Graph to look for Entities give a specific seed.
 */
public abstract class EntityDataFetcher extends ElementDataFetcher<Entity> {

    public EntityDataFetcher(final String group) {
        super(group, Entity.class);
    }

    protected abstract String getVertex(final DataFetchingEnvironment environment);

    protected OperationChain<CloseableIterable<Entity>> getOperationChain(final DataFetchingEnvironment environment,
                                                                          final StringBuilder keyBuilder) {
        final String vertexArg = getVertex(environment);
        keyBuilder.append(vertexArg);
        final OperationChain<CloseableIterable<Entity>> opChain = new OperationChain.Builder()
                .first(new GetEntitiesBySeed.Builder()
                        .addSeed(new EntitySeed(vertexArg))
                        .view(new View.Builder()
                                .entity(getGroup())
                                .build())
                        .build())
                .build();

        return opChain;
    }

    protected void addFixedValues(final Entity element, final Map<String, Object> result) {
        result.put(Constants.VERTEX_VALUE, element.getVertex().toString());
    }
}
