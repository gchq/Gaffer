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
package gaffer.graphql;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Element;
import gaffer.graph.Graph;
import gaffer.operation.OperationChain;
import gaffer.user.User;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the objects required by the Data Fetchers,
 * effectively allows us to tunnel a Graph and User through GraphQL.
 */
public final class GrafferQLContext {
    private static final Logger LOGGER = Logger.getLogger(GrafferQLContext.class);

    private final Graph graph;
    private final User user;

    /**
     * Operations are cached
     */
    private int cacheUsed = 0;
    private final Map<String, OperationChain<?>> operations;
    private final Map<String, CloseableIterable<? extends Element>> cache;

    private GrafferQLContext(final Graph graph, final User user) {
        this.graph = graph;
        this.user = user;
        this.operations = new HashMap<>();
        this.cache = new HashMap<>();
    }

    public Graph getGraph() {
        return graph;
    }

    public User getUser() {
        return user;
    }

    public int getCacheUsed() {
        return cacheUsed;
    }

    public Map<String, OperationChain<?>> getOperations() {
        return operations;
    }

    public void reset() {
        this.operations.clear();
        this.cache.clear();
    }

    public void registerOperation(final OperationChain<?> operation,
                                  final String key,
                                  final CloseableIterable<? extends Element> result) {
        this.operations.put(key, operation);
        this.cache.put(key, result);
        this.cacheUsed = 0;
    }

    public <E extends Element> CloseableIterable<E> fetchCache(final String key, final Class<E> clazz) {
        CloseableIterable<E> result = null;
        final CloseableIterable<?> iterable = this.cache.get(key);

        if (null != iterable) {
            try {
                result = (CloseableIterable<E>) iterable;
                cacheUsed++;
            } catch (ClassCastException e) {
                LOGGER.warn("Cache: " + key + ", expected: " + clazz + ", had problem" + e.getLocalizedMessage());
            }
        }

        return result;
    }

    public static class Builder {
        private Graph graph;
        private User user;

        public Builder() {
        }

        public Builder graph(final Graph graph) {
            this.graph = graph;
            return this;
        }

        public Builder user(final User user) {
            this.user = user;
            return this;
        }

        public GrafferQLContext build() throws GrafferQLException {
            if (null == graph) {
                throw new GrafferQLException("graph given to context builder is null");
            }
            if (null == user) {
                throw new GrafferQLException("user given to context builder is null");
            }
            return new GrafferQLContext(graph, user);
        }
    }
}
