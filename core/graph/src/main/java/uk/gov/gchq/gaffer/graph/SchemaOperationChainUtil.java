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

package uk.gov.gchq.gaffer.graph;

import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.SchemaOperationChainValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.koryphe.ValidationResult;

public final class SchemaOperationChainUtil {

    private SchemaOperationChainUtil() {
        // Private constructor to prevent instantiation.
    }

    public static ValidationResult validate(final Schema schema, final OperationChain operationChain) {
        updateOperationChainViews(operationChain, schema);
        SchemaOperationChainValidator validator = new SchemaOperationChainValidator(new ViewValidator(), schema);

        return validator.validate(operationChain, null, null);
    }

    private static void updateOperationChainViews(final Operations<?> operations, final Schema schema) {
        for (final Operation operation : operations.getOperations()) {
            if (operation instanceof Operations) {
                updateOperationChainViews((Operations) operation, schema);
            } else if (operation instanceof OperationView) {
                View opView = ((OperationView) operation).getView();
                if (null == opView) {
                    opView = createView(schema);
                } else if (!(opView instanceof NamedView) && !opView.hasGroups() && !opView.isAllEdges() && !opView.isAllEntities()) {
                    opView = new View.Builder()
                            .merge(createView(schema))
                            .merge(opView)
                            .build();
                } else if (opView.isAllEdges() || opView.isAllEntities()) {
                    View.Builder opViewBuilder = new View.Builder()
                            .merge(opView);
                    if (opView.isAllEdges()) {
                        opViewBuilder.edges(schema.getEdgeGroups());
                    }
                    if (opView.isAllEntities()) {
                        opViewBuilder.entities(schema.getEntityGroups());
                    }
                    opView = opViewBuilder.build();
                }
                opView.expandGlobalDefinitions();
                ((OperationView) operation).setView(opView);
            }
        }
    }

    private static View createView(final Schema schema) {
        return new View.Builder()
                .entities(schema.getEntityGroups())
                .edges(schema.getEdgeGroups())
                .build();
    }
}
