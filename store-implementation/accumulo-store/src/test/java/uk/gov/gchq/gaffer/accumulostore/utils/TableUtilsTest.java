/*
 * Copyright 2016-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.utils;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloRuntimeException;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ValidatorFilter;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class TableUtilsTest {
    private static final String GRAPH_ID = "graph1";
    private static final String GRAPH_ID_2 = "graph2";
    private static final String LOCALITY_GRAPH_ID = "localityTest";
    private static final String NO_AGGREGATORS_GRAPH_ID = "noAggGraph";

    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));
    private static final AccumuloProperties PROPERTIES_BASIC = new AccumuloProperties();

    private static final Schema SCHEMA = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .aggregateFunction(new StringConcat())
                        .validateFunctions(new Exists())
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

    @BeforeAll
    static void setup() {
        PROPERTIES_BASIC.setStoreClass(PROPERTIES.getStoreClass());
        PROPERTIES_BASIC.setZookeepers(PROPERTIES.getZookeepers());
    }

    @Test
    public void shouldCreateTableWithAllRequiredIterators() throws Exception {
        // Given
        final AccumuloStore store = new SingleUseMiniAccumuloStore();
        store.initialise(GRAPH_ID, SCHEMA, PROPERTIES);

        // When
        TableUtils.createTable(store);

        // Then - this call will check the table is configured properly
        TableUtils.ensureTableExists(store);
    }

    @Test
    public void shouldFailTableValidationWhenMissingValidatorIterator() throws Exception {
        final AccumuloStore store = new SingleUseMiniAccumuloStore();
        store.initialise(GRAPH_ID, SCHEMA, PROPERTIES);

        final Runnable invalidateTable = () -> {
            try {
                AddUpdateTableIterator.removeIterator(store, AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME);
            } catch (final StoreException e) {
                throw new RuntimeException(e);
            }
        };

        shouldFailTableValidationWhenTableInvalid(store, invalidateTable);
    }

    @Test
    public void shouldFailTableValidationWhenMissingAggregatorIterator() throws Exception {
        final AccumuloStore store = new SingleUseMiniAccumuloStore();
        store.initialise(GRAPH_ID, SCHEMA, PROPERTIES);

        final Runnable invalidateTable = () -> {
            try {
                AddUpdateTableIterator.removeIterator(store, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME);
            } catch (final StoreException e) {
                throw new RuntimeException(e);
            }
        };

        shouldFailTableValidationWhenTableInvalid(store, invalidateTable);
    }

    public void shouldFailTableValidationWhenTableInvalid(final AccumuloStore store, final Runnable invalidateTable) throws Exception {
        // Given
        final AccumuloProperties props = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));
        store.initialise(GRAPH_ID, SCHEMA, props);

        invalidateTable.run();

        // When / Then
        assertThatExceptionOfType(StoreException.class).isThrownBy(() -> TableUtils.ensureTableExists(store)).extracting("message").isNotNull();
    }

    @Test
    public void shouldCreateTableWithCorrectLocalityGroups() throws Exception {
        final AccumuloStore store = new SingleUseMiniAccumuloStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        store.initialise(LOCALITY_GRAPH_ID, schema, PROPERTIES);

        // When
        TableUtils.createTable(store);

        final Map<String, Set<Text>> localityGroups = store.getConnection().tableOperations().getLocalityGroups(LOCALITY_GRAPH_ID);
        assertThat(localityGroups).hasSize(1);
        Set<Text> localityGroup = localityGroups.get(TestGroups.EDGE);
        assertThat(localityGroup).hasSize(1);
        assertThat(localityGroup.toArray()[0]).isEqualTo(new Text(TestGroups.EDGE));
    }

    @Test
    public void shouldCreateTableCorrectlyIfSchemaContainsNoAggregators() throws Exception {
        // Given
        final AccumuloStore store = new SingleUseMiniAccumuloStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .validateFunctions(new Exists())
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .aggregate(false)
                        .build())
                .build();

        store.initialise(NO_AGGREGATORS_GRAPH_ID, schema, PROPERTIES);

        // When
        TableUtils.createTable(store);

        // Then
        final Map<String, EnumSet<IteratorScope>> itrs = store.getConnection().tableOperations().listIterators(NO_AGGREGATORS_GRAPH_ID);
        assertThat(itrs).hasSize(1);

        final EnumSet<IteratorScope> validator = itrs.get(AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME);
        assertThat(validator).isEqualTo(EnumSet.allOf(IteratorScope.class));
        final IteratorSetting validatorSetting = store.getConnection().tableOperations().getIteratorSetting(NO_AGGREGATORS_GRAPH_ID, AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME, IteratorScope.majc);
        assertThat(validatorSetting.getPriority()).isEqualTo(AccumuloStoreConstants.VALIDATOR_ITERATOR_PRIORITY);
        assertThat(validatorSetting.getIteratorClass()).isEqualTo(ValidatorFilter.class.getName());
        final Map<String, String> validatorOptions = validatorSetting.getOptions();
        assertThat(Schema.fromJson(validatorOptions.get(AccumuloStoreConstants.SCHEMA).getBytes(StandardCharsets.UTF_8)).getEdge(TestGroups.EDGE)).isNotNull();
        assertThat(validatorOptions.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS)).isEqualTo(ByteEntityAccumuloElementConverter.class.getName());

        final EnumSet<IteratorScope> aggregator = itrs.get(AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME);
        assertThat(aggregator).isNull();
        final IteratorSetting aggregatorSetting = store.getConnection().tableOperations().getIteratorSetting(NO_AGGREGATORS_GRAPH_ID, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME, IteratorScope.majc);
        assertThat(aggregatorSetting).isNull();

        final Map<String, String> tableProps = new HashMap<>();
        for (final Map.Entry<String, String> entry : store.getConnection()
                .tableOperations().getProperties(NO_AGGREGATORS_GRAPH_ID)) {
            tableProps.put(entry.getKey(), entry.getValue());
        }

        assertThat(Integer.parseInt(tableProps.get(Property.TABLE_FILE_REPLICATION.getKey()))).isZero();
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsNotSpecified() throws StoreException {
        // Given
        final AccumuloStore store = new MiniAccumuloStore();
        assertThatIllegalArgumentException().isThrownBy(() -> store.initialise(null, SCHEMA, PROPERTIES_BASIC));

        // When
        assertThatExceptionOfType(AccumuloRuntimeException.class).isThrownBy(() -> TableUtils.ensureTableExists(store));
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsNotSpecifiedWhenCreatingTable() throws StoreException, TableExistsException {
        // Given
        final AccumuloStore store = new MiniAccumuloStore();
        assertThatIllegalArgumentException().isThrownBy(() -> store.initialise(null, SCHEMA, PROPERTIES_BASIC));

        // When
        assertThatExceptionOfType(AccumuloRuntimeException.class).isThrownBy(() -> TableUtils.createTable(store));
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsNotSpecifiedWhenCreatingAGraph() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(null)
                        .build())
                .addSchema(SCHEMA)
                .storeProperties(PROPERTIES_BASIC)
                .build());
    }

    @Test
    public void shouldRenameTable() throws Exception {
        // Given
        final AccumuloStore store = new SingleUseMiniAccumuloStore();
        store.initialise(GRAPH_ID, SCHEMA, PROPERTIES);

        // When
        TableUtils.renameTable(PROPERTIES, GRAPH_ID, GRAPH_ID_2);

        // Then
        boolean tableExists = TableUtils.getConnector(PROPERTIES).tableOperations().exists(GRAPH_ID_2);
        assertThat(tableExists).isTrue();
    }

    @Test
    public void shouldThrowExceptionIfTableRenameFails() throws Exception {
        // Given
        final AccumuloStore store1 = new SingleUseMiniAccumuloStore();
        store1.initialise(GRAPH_ID, SCHEMA, PROPERTIES);
        final AccumuloStore store2 = new SingleUseMiniAccumuloStore();
        store2.initialise(GRAPH_ID_2, SCHEMA, PROPERTIES);

        // When / Then
        assertThatExceptionOfType(StoreException.class).isThrownBy(() -> TableUtils.renameTable(PROPERTIES, GRAPH_ID, GRAPH_ID_2));
    }
}
