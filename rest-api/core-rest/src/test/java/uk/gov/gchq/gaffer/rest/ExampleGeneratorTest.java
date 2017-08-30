/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromKafka;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.operation.impl.output.ToArray;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToStream;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.service.v2.example.DefaultExamplesFactory;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class ExampleGeneratorTest {

    private final DefaultExamplesFactory generator = new DefaultExamplesFactory();
    private final GraphFactory graphFactory = new DefaultGraphFactory();
    private final Class<? extends Operation> opClass;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    public ExampleGeneratorTest(final Class<? extends Operation> opClass) {
        this.opClass = opClass;
    }

    @Parameters
    public static Collection<Object[]> instancesToTest() {
        return Arrays.asList(
                new Object[]{AddElements.class},
                new Object[]{AddElementsFromFile.class},
                new Object[]{AddElementsFromKafka.class},
                new Object[]{AddElementsFromSocket.class},
                new Object[]{Max.class},
                new Object[]{Min.class},
                new Object[]{Sort.class},
                new Object[]{ExportToGafferResultCache.class},
                new Object[]{GetGafferResultCacheExport.class},
                new Object[]{ExportToSet.class},
                new Object[]{GetSetExport.class},
                new Object[]{GetExports.class},
                new Object[]{GenerateElements.class},
                new Object[]{GenerateObjects.class},
                new Object[]{GetAdjacentIds.class},
                new Object[]{GetAllElements.class},
                new Object[]{GetElements.class},
                new Object[]{GetAllJobDetails.class},
                new Object[]{GetJobDetails.class},
                new Object[]{GetJobResults.class},
                new Object[]{ToArray.class},
                new Object[]{ToCsv.class},
                new Object[]{ToEntitySeeds.class},
                new Object[]{ToList.class},
                new Object[]{ToMap.class},
                new Object[]{ToSet.class},
                new Object[]{ToStream.class},
                new Object[]{ToVertices.class},
                new Object[]{Count.class},
                new Object[]{CountGroups.class},
                new Object[]{DiscardOutput.class},
                new Object[]{Limit.class},
                new Object[]{ScoreOperationChain.class},
                new Object[]{SplitStore.class},
                new Object[]{Validate.class}
        );
    }

//    @BeforeClass
//    public static void beforeClass() throws IOException {
//        final File storePropertiesFile = tempFolder.newFile("store.properties");
//        FileUtils.writeLines(storePropertiesFile, IOUtils.readLines(StreamUtil.openStream(ExampleGeneratorTest.class, "store.properties")));
//        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropertiesFile.getAbsolutePath());
//
//        final File schemaFile = tempFolder.newFile("schema.json");
//        FileUtils.writeLines(schemaFile, IOUtils.readLines(StreamUtil.openStream(ExampleGeneratorTest.class, "/schema/schema.json")));
//        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaFile.getAbsolutePath());
//
////        System.getProperties().put("gaffer.storeProperties", StreamUtil.storeProps(ExampleGeneratorTest.class));
//        System.getProperties().put("gaffer.graph.id", "graphId");
////        System.getProperties().put("gaffer.schemas", StreamUtil.schema(ExampleGeneratorTest.class));
//    }

    @Before
    public void before() throws IllegalAccessException, NoSuchFieldException, IOException {
        final File storePropertiesFile = tempFolder.newFile("store.properties");
        FileUtils.writeLines(storePropertiesFile, IOUtils.readLines(StreamUtil.openStream(ExampleGeneratorTest.class, "store.properties")));
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropertiesFile.getAbsolutePath());

        final File schemaFile = tempFolder.newFile("schema.json");
        FileUtils.writeLines(schemaFile, IOUtils.readLines(StreamUtil.openStream(ExampleGeneratorTest.class, "/schema/schema.json")));
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaFile.getAbsolutePath());

        System.setProperty(SystemProperty.GRAPH_ID, "graphId");

        // Manually inject GraphFactory
        final Field field = generator.getClass().getDeclaredField("graphFactory");

        field.setAccessible(true);
        field.set(generator, graphFactory);
    }

    @Test
    public void shouldBuildOperation() throws InstantiationException, IllegalAccessException, JsonProcessingException {
        // Given
        final Operation operation = generator.generateExample(opClass);

        // Then
        assertThat(operation, notNullValue());
    }

}
