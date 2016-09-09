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
import gaffer.data.element.Element;
import gaffer.graphql.GrafferQLContext;
import gaffer.graphql.definitions.Constants;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * General form of Gaffer Element Fetcher, hands of to child classes for OperationChain building
 * and to add certain values fixed for those Element Types.
 */
public abstract class ElementDataFetcher<E extends Element> implements DataFetcher {
    protected static final Logger LOGGER = Logger.getLogger(ElementDataFetcher.class);

    private final Class<E> clazz;

    private final String group;

    public ElementDataFetcher(final String group, final Class<E> clazz) {
        this.group = group;
        this.clazz = clazz;
    }

    protected String getGroup() {
        return group;
    }

    protected static final String KEY_DELIMITER = "-";

    protected abstract OperationChain<CloseableIterable<E>> getOperationChain(
            DataFetchingEnvironment environment,
            StringBuilder keyBuilder
    );

    protected abstract void addFixedValues(E element, Map<String, Object> result);

    @Override
    public Object get(final DataFetchingEnvironment environment) {

        if (!(environment.getContext() instanceof GrafferQLContext)) {
            throw new IllegalArgumentException("Context was not a " + GrafferQLContext.class);
        }
        final GrafferQLContext context = (GrafferQLContext) environment.getContext();

        final StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(this.getClass());
        keyBuilder.append(KEY_DELIMITER);
        final OperationChain<CloseableIterable<E>> opChain = getOperationChain(environment, keyBuilder);

        final List<Object> results = new ArrayList<>();
        try {
            CloseableIterable<E> elements = context.fetchCache(keyBuilder.toString(), clazz);
            if (null == elements) {
                elements = context.getGraph().execute(opChain, context.getUser());
                context.registerOperation(opChain, keyBuilder.toString(), elements);
            }

            for (final E e : elements) {
                final Map<String, Object> result = new HashMap<>();
                addFixedValues(e, result);
                for (final Map.Entry<String, Object> p : e.getProperties().entrySet()) {
                    if (p.getValue() != null) {
                        final Map<String, Object> value = new HashMap<>();
                        value.put(Constants.VALUE, p.getValue().toString());
                        result.put(p.getKey(), value);
                    }
                }
                results.add(result);
            }
        } catch (final OperationException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }
        return results;
    }
}
