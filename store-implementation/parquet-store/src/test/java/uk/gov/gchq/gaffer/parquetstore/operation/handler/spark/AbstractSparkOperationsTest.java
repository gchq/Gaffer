/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.StandaloneIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.List;

public abstract class AbstractSparkOperationsTest extends StandaloneIT {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected User user = getUser();

    protected abstract RDD<Element> getInputDataForGetAllElementsTest();

    protected abstract List<Element> getInputDataForGetAllElementsTestAsList();

    protected abstract int getNumberOfItemsInInputDataForGetAllElementsTest();

    protected abstract List<Element> getResultsForGetAllElementsTest();

    protected abstract List<ElementSeed> getSeeds();

    protected abstract List<Element> getResultsForGetElementsWithSeedsRelatedTest();

    protected abstract List<Element> convertRowsToElements(List<Row> rows);

    @Override
    public User getUser() {
        return new User();
    }

    @Override
    public StoreProperties createStoreProperties() {
        try {
            return TestUtils.getParquetStoreProperties(testFolder);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Graph createGraph(final int numOutputFiles) throws IOException {
        final ParquetStoreProperties storeProperties = TestUtils.getParquetStoreProperties(testFolder);
        storeProperties.setAddElementsOutputFilesPerGroup(numOutputFiles);
        return createGraph(storeProperties);
    }

    @Test
    public void getAllElementsAfterImportElementsFromRDDTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final RDD<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new ImportRDDOfElements.Builder().input(elements).build(), user);

        // When
        final CloseableIterable<? extends Element> results = graph.execute(
                new GetAllElements.Builder().build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetAllElementsTest(), results);
    }

    @Test
    public void getElementsWithSeedsRelatedAfterImportElementsFromRDDTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final RDD<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new ImportRDDOfElements.Builder().input(elements).build(), user);

        // When
        final List<ElementSeed> seeds = getSeeds();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder()
                        .input(seeds)
                        .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                        .build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetElementsWithSeedsRelatedTest(), results);
    }

    @Test
    public void getElementsWithSeedsRelatedAfterImportElementsFromRDDTestWhenMoreFilesThanElements() throws IOException, OperationException {
        // Given
        final int numFiles = 2 * getNumberOfItemsInInputDataForGetAllElementsTest();
        final Graph graph = createGraph(numFiles);
        final RDD<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new ImportRDDOfElements.Builder().input(elements).build(), user);

        // When
        final List<ElementSeed> seeds = getSeeds();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder()
                        .input(seeds)
                        .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                        .build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetElementsWithSeedsRelatedTest(), results);
    }

    @Test
    public void shouldReturnCorrectResultsWhenGetDataFrameOfElementsCalledWithNoView() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTestAsList();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final Dataset<Row> results = graph.execute(new GetDataFrameOfElements.Builder().build(), user);

        // Then
        final List<Element> elementsFromRows = convertRowsToElements(results.collectAsList());
        ElementUtil.assertElementEquals(getResultsForGetAllElementsTest(), elementsFromRows);
    }
}
