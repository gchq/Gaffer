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

package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MigrationIT extends AbstractStoreIT {

    private static final String INTEGER_TYPE_DESCRIPTION = "Integer type description";
    private static final String LONG_TYPE_DESCRIPTION = "Long type description";
    private static final String STRING_TYPE_DESCRIPTION = "String type description";
    private Graph graph;
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private final User user = new User();

    private final Schema schema = new Schema.Builder()
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source(TestTypes.ID_STRING)
                    .destination(TestTypes.ID_STRING)
                    .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                    .property(TestPropertyNames.PROP_2, TestTypes.PROP_STRING)
                    .aggregate(false)
                    .build())
            .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                    .source(TestTypes.ID_STRING)
                    .destination(TestTypes.ID_STRING)
                    .property(TestPropertyNames.PROP_1, TestTypes.PROP_LONG)
                    .property(TestPropertyNames.PROP_2, TestTypes.PROP_STRING)
                    .aggregate(false)
                    .build())
            .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .description(STRING_TYPE_DESCRIPTION)
                    .build())
            .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                    .clazz(Integer.class)
                    .description(INTEGER_TYPE_DESCRIPTION)
                    .build())
            .type(TestTypes.PROP_LONG, new TypeDefinition.Builder()
                    .clazz(Long.class)
                    .description(LONG_TYPE_DESCRIPTION)
                    .build())
            .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .description(STRING_TYPE_DESCRIPTION)
                    .build())
            .build();

    @Override
    @Before
    public void setup() throws Exception {
        final File graphHooks = tempFolder.newFile("hooks.json");
        FileUtils.writeLines(graphHooks, IOUtils.readLines(StreamUtil.openStream(getClass(), "hooks.json")));

        graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("integrationTestGraph")
                        .addHooks(Paths.get(graphHooks.getPath()))
                        .build())
                .addStoreProperties(getStoreProperties())
                .addSchema(schema)
                .build();

        addElements();
    }

    @Test
    public void shouldUpdateElements() throws Exception {
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .properties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .postAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new IsLessThan("10"))
                                .build())
                        .build())
                .build();
        final GetAllElements getAllElements = new GetAllElements.Builder()
                .view(view)
                .build();

        final Element expectedEdge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("source")
                .dest("dest")
                .directed(true)
                .property(TestPropertyNames.PROP_1, 7L)
                .property(TestPropertyNames.PROP_2, "TESTPROPVALUE2")
                .build();

        final Edge expectedEdge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("source")
                .dest("dest")
                .directed(true)
                .property(TestPropertyNames.PROP_1, 2L)
                .property(TestPropertyNames.PROP_2, "")
                .build();

        final Iterable<? extends Element> results = graph.execute(getAllElements, user);
        final List<Element> resultList = Lists.newArrayList(results);

        assertEquals(2, resultList.size());
        assertThat(resultList, IsCollectionContaining.hasItems(expectedEdge1, expectedEdge2));
    }

    private void addElements() throws OperationException {
        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("source")
                .dest("dest")
                .directed(true)
                .property(TestPropertyNames.PROP_1, 7)
                .property(TestPropertyNames.PROP_2, "testPropValue2")
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("source")
                .dest("dest")
                .directed(true)
                .property(TestPropertyNames.PROP_1, 2L)
                .property(TestPropertyNames.PROP_3, "testPropValue3")
                .build();

        final AddElements op = new AddElements.Builder()
                .input(edge1, edge2)
                .build();

        graph.execute(op, user);
    }
}
