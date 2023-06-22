/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.gafferpop.service;

import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.service.Service.Type;

import uk.gov.gchq.gaffer.gafferpop.GafferPopGraph;

import java.util.Collections;
import java.util.Map;
import java.util.Set;


public class GafferPopNamedOperationServiceFactory<I, R> implements Service.ServiceFactory<I, R> {
    private final GafferPopGraph graph;

    public GafferPopNamedOperationServiceFactory(final GafferPopGraph graph) {
        this.graph = graph;
    }
    @Override
    public String getName() {
        return "namedoperation";
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Collections.singleton(Type.Start);
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return new GafferPopNamedOperationService<I, R>(graph);
    }

}
