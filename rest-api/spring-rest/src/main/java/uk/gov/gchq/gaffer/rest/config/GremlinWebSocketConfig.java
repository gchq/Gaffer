/*
 * Copyright 2024-2025 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.rest.handler.GremlinWebSocketHandler;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

@Configuration
@EnableWebSocket
public class GremlinWebSocketConfig implements WebSocketConfigurer {

    private final GafferPopGraph graph;
    private final AbstractUserFactory userFactory;
    private final Long requestTimeout;

    @Autowired
    public GremlinWebSocketConfig(final GafferPopGraph graph, final AbstractUserFactory userFactory, final Long requestTimeout) {
        this.graph = graph;
        this.userFactory = userFactory;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public void registerWebSocketHandlers(final WebSocketHandlerRegistry registry) {
        registry.addHandler(new GremlinWebSocketHandler(graph, userFactory, requestTimeout), "/gremlin");
    }

}
