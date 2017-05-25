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

package uk.gov.gchq.gaffer.hbasestore.util;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.hbasestore.HBaseProperties;
import uk.gov.gchq.gaffer.hbasestore.MiniHBaseStore;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.GafferCoprocessor;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TableUtilsTest {
    public static final String TABLE_NAME = "table1";

    @Test
    public void shouldCreateTableAndValidateIt() throws Exception {
        // Given
        final MiniHBaseStore store = new MiniHBaseStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .aggregateFunction(new StringConcat())
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        final HBaseProperties props = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));
        props.setTable(TABLE_NAME);
        store.initialise(schema, props);

        // When
        TableUtils.createTable(store);
        TableUtils.ensureTableExists(store);

        // Then - no exceptions
    }

    @Test
    public void shouldFailTableValidationWhenTableDoesntHaveCoprocessor() throws Exception {
        // Given
        final MiniHBaseStore store = new MiniHBaseStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .aggregateFunction(new StringConcat())
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        final HBaseProperties props = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));
        props.setTable(TABLE_NAME);
        store.initialise(schema, props);

        // Remove coprocessor
        final TableName tableName = store.getProperties().getTable();
        try (final Admin admin = store.getConnection().getAdmin()) {
            final HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
            descriptor.removeCoprocessor(GafferCoprocessor.class.getName());
            admin.modifyTable(tableName, descriptor);
        } catch (final StoreException | IOException e) {
            throw new RuntimeException(e);
        }

        // When / Then
        try {
            TableUtils.ensureTableExists(store);
            fail("Exception expected");
        } catch (final StoreException e) {
            assertNotNull(e.getMessage());
        }
    }
}
