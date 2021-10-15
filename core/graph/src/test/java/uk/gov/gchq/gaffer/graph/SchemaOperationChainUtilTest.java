/*
 * Copyright 2018-2021 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.integration.store.TestStore;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.ValidationResult;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

public class SchemaOperationChainUtilTest {

    Graph graph;
    final StoreProperties storeProperties = new StoreProperties();
    final Schema schema = new Schema.Builder()
            .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .build())
            .type("vertex", new TypeDefinition.Builder()
                    .clazz(String.class)
                    .build())
            .type(DIRECTED_EITHER, Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                    .aggregate(false)
                    .source("vertex")
                    .destination("vertex")
                    .directed(DIRECTED_EITHER)
                    .build())
            .build();
    private static final String GRAPH_ID = "graphId";
    final View view = new View.Builder().allEdges(true).build();
    final OperationChain validOperationChain = new OperationChain.Builder()
            .first(new AddElements())
            .then(new GetElements())
            .build();
    final OperationChain invalidOperationChain = new OperationChain.Builder()
            .first(new AddElementsFromSocket())
            .then(new GetElements())
            .build();

    @BeforeEach
    public void setup() {
        storeProperties.setStoreClass(TestStore.class);
        graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID)
                        .view(view)
                        .build())
                .storeProperties(storeProperties)
                .addSchema(schema)
                .build();
    }

    @Test
    public void shouldValidateValidOperationChainAgainstSchema() {
        // When
        final ValidationResult validationResult = SchemaOperationChainUtil.validate(schema, validOperationChain);

        // Then
        assertTrue(validationResult.isValid());
    }

    @Test
    public void shouldValidateInvalidOperationChainAgainstSchema() {
        // When
        final ValidationResult validationResult = SchemaOperationChainUtil.validate(schema, invalidOperationChain);

        // Then
        assertFalse(validationResult.isValid());
        assertThat(validationResult.getErrorString())
                .contains("elementGenerator is required for: AddElementsFromSocket")
                .contains("hostname is required for: AddElementsFromSocket");
    }
}
