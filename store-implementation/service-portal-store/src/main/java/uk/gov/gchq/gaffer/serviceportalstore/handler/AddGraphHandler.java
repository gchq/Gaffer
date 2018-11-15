/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.serviceportalstore.handler;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements.Builder;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.ArrayList;
import java.util.Set;

public class AddGraphHandler implements OperationHandler<AddGraph> {
    @Override
    public Object doOperation(final AddGraph operation, final Context context, final Store store) throws OperationException {
        processCreatingAGraph();

        String graphId = operation.getGraphId();

        ArrayList<Element> elements = new ArrayList<>();

        processCanAccessEdge(elements, graphId, operation.getGraphAuths());
        processGraphEntity(elements, graphId, operation.getIsPublic());

        AddElements op = new Builder()
                .input(elements)
                .options(operation.getOptions())
                .build();

        store.execute(op, context);

        return null;
    }

    private void processGraphEntity(final ArrayList<Element> elements, final String graphId, final boolean isPublic) {
        /*
         * this is tightly coupled with the graphOfGraph schema
         */
        elements.add(new Entity.Builder()
                .group("graph")
                .vertex(graphId)
                .property("isPublic", isPublic)
                .build());
    }

    private void processCreatingAGraph() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    private void processCanAccessEdge(final ArrayList<Element> elements, final String graphId, final Set<String> graphAuths) {
        /*
         * this is tightly coupled with the graphOfGraph schema
         */
        for (String graphAuth : graphAuths) {
            elements.add(new Edge.Builder()
                    .group("canAccess")
                    .source(graphAuth)
                    .dest(graphId)
                    .build());
        }
    }
}
