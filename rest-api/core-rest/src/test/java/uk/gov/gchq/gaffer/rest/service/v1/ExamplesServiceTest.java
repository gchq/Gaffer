/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v1;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.service.v1.example.ExamplesService;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class ExamplesServiceTest {
    @InjectMocks
    private ExamplesService service;

    @Mock
    private GraphFactory graphFactory;

    @Mock
    private UserFactory userFactory;

    private Schema schema;

    @Before
    public void setup() {
        schema = new Schema.Builder()
                .type("string", String.class)
                .type("true", Boolean.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property("entityProperties", "string")
                        .vertex("string")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property("edgeProperties", "string")
                        .source("string")
                        .destination("string")
                        .directed("true")
                        .build())
                .build();

        final Store store = mock(Store.class);
        given(store.getSchema()).willReturn(schema);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(store.getOriginalSchema()).willReturn(schema);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .store(store)
                .build();
        given(graphFactory.getGraph()).willReturn(graph);
    }

    @Test
    public void shouldSerialiseAndDeserialiseAddElements() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.addElements());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetElementsBySeed() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getElementsBySeed());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetRelatedElements() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getRelatedElements());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetAllElements() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getAllElements());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGenerateObjects() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.generateObjects());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGenerateElements() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.generateElements());
    }

    @Test
    public void shouldSerialiseAndDeserialiseOperationChain() throws IOException {
        //Given
        final OperationChain opChain = service.execute();

        // When
        byte[] bytes = JSONSerialiser.serialise(opChain);
        final OperationChain deserialisedOp = JSONSerialiser.deserialise(bytes, opChain.getClass());

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    public void shouldCreateViewForEdges() {
        final View.Builder builder = service.generateViewBuilder();
        final View view = builder.build();
        assertNotNull(view);

        final ViewValidator viewValidator = new ViewValidator();
        assertTrue(viewValidator.validate(view, schema, Sets.newHashSet(StoreTrait.values())).isValid());
    }

    private void shouldSerialiseAndDeserialiseOperation(final Operation operation) throws IOException {
        //Given

        // When
        byte[] bytes = JSONSerialiser.serialise(operation);
        final Operation deserialisedOp = JSONSerialiser.deserialise(bytes, operation.getClass());

        // Then
        assertNotNull(deserialisedOp);
    }
}
