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

package uk.gov.gchq.gaffer.hbasestore;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.QUERY_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;

public class HBaseStoreTest {
    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(HBaseStoreTest.class));
    private static final HBaseProperties PROPERTIES = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(HBaseStoreTest.class));
    private static MiniHBaseStore store;

    @BeforeClass
    public static void setup() throws StoreException, IOException {
        store = new MiniHBaseStore();
        store.initialise(schema, PROPERTIES);
    }

    @AfterClass
    public static void tearDown() {
        store = null;
    }

    @Test
    public void shouldCreateTableWhenInitialised() throws StoreException, IOException {
        final Connection connection = store.getConnection();
        final TableName tableName = store.getProperties().getTable();
        try (final Admin admin = connection.getAdmin()) {
            assertTrue(admin.tableExists(tableName));
        }
    }

    @Test
    public void shouldBeAnOrderedStore() {
        assertTrue(store.hasTrait(StoreTrait.ORDERED));
    }

    @Test
    public void validationShouldNotBeRequired() {
        assertFalse(store.isValidationRequired());
    }

    @Test
    public void shouldThrowExceptionForUnhandledOperation() {
        try {
            store.doUnhandledOperation(new AddElements(), null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testStoreReturnsHandlersForRegisteredOperations() throws StoreException {
        // Then
        assertNotNull(store.getOperationHandlerExposed(Validate.class));
        assertTrue(store.getOperationHandlerExposed(GetElements.class) instanceof GetElementsHandler);
        assertTrue(store.getOperationHandlerExposed(GetAllElements.class) instanceof GetAllElementsHandler);
        assertTrue(store.getOperationHandlerExposed(AddElements.class) instanceof AddElementsHandler);
        assertTrue(store.getOperationHandlerExposed(AddElementsFromHdfs.class) instanceof AddElementsFromHdfsHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateElements.class) instanceof GenerateElementsHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateObjects.class) instanceof GenerateObjectsHandler);
    }

    @Test
    public void testRequestForNullHandlerManaged() {
        final OperationHandler returnedHandler = store.getOperationHandlerExposed(null);
        assertNull(returnedHandler);
    }

    @Test
    public void testStoreTraits() {
        final Collection<StoreTrait> traits = store.getTraits();
        assertNotNull(traits);
        assertTrue("Collection size should be 8", traits.size() == 9);
        assertTrue("Collection should contain INGEST_AGGREGATION trait", traits.contains(INGEST_AGGREGATION));
        assertTrue("Collection should contain QUERY_AGGREGATION trait", traits.contains(QUERY_AGGREGATION));
        assertTrue("Collection should contain PRE_AGGREGATION_FILTERING trait", traits.contains(PRE_AGGREGATION_FILTERING));
        assertTrue("Collection should contain POST_AGGREGATION_FILTERING trait", traits.contains(POST_AGGREGATION_FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain POST_TRANSFORMATION_FILTERING trait", traits.contains(POST_TRANSFORMATION_FILTERING));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));
        assertTrue("Collection should contain ORDERED trait", traits.contains(ORDERED));
        assertTrue("Collection should contain VISIBILITY trait", traits.contains(VISIBILITY));
    }

}
