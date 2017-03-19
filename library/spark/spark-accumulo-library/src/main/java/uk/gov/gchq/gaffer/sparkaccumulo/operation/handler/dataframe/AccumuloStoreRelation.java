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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ClassTagConstants;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ConvertElementToRow;
import uk.gov.gchq.gaffer.spark.operation.dataframe.FiltersToOperationConverter;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.Converter;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema.SchemaToStructTypeConverter;
import uk.gov.gchq.gaffer.spark.operation.scalardd.AbstractGetRDD;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Allows Apache Spark to retrieve data from an {@link AccumuloStore} as a <code>DataFrame</code>. Spark's Java API
 * does not expose the <code>DataFrame</code> class, but it is just a type alias for a {@link
 * org.apache.spark.sql.Dataset} of {@link Row}s. The schema of the <code>DataFrame</code> is formed from the schemas
 * of the groups specified in the view.
 * <p>
 * If two of the specified groups have properties with the same name, then the types of those properties must be
 * the same.
 * <p>
 * <code>AccumuloStoreRelation</code> implements the {@link TableScan} interface which allows all {@link Element}s to
 * of the specified groups to be returned to the <code>DataFrame</code>.
 * <p>
 * <code>AccumuloStoreRelation</code> implements the {@link PrunedScan} interface which allows all {@link Element}s
 * of the specified groups to be returned to the <code>DataFrame</code> but with only the specified columns returned.
 * Currently, {@link AccumuloStore} does not allow projection of properties in the tablet server, so this projection
 * is performed within the Spark executors, rather than in Accumulo's tablet servers. Once {@link AccumuloStore}
 * supports this projection in the tablet servers, then this will become more efficient.
 * <p>
 * <code>AccumuloStoreRelation</code> implements the {@link PrunedFilteredScan} interface which allows only
 * {@link Element}s that match the the provided {@link Filter}s to be returned. The majority of these are implemented
 * by adding them to the {@link View}, which causes them to be applied on Accumulo's tablet server (i.e. before
 * the data is sent to a Spark executor). If a {@link Filter} is specified that specifies either the vertex in an
 * <code>Entity</code> or either the source or destination vertex in an <code>Edge</code> then this is applied by
 * using the appropriate range scan on Accumulo. Queries against this <code>DataFrame</code> that do this should be
 * very quick.
 */
public class AccumuloStoreRelation extends BaseRelation implements TableScan, PrunedScan, PrunedFilteredScan {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloStoreRelation.class);

    private final SQLContext sqlContext;
    private final LinkedHashSet<String> groups;
    private final View view;
    private final AccumuloStore store;
    private final User user;
    private final LinkedHashSet<String> usedProperties;
    private final Map<String, Boolean> propertyNeedsConversion;
    private final Map<String, Converter> converterByProperty;
    private StructType structType;
    private SchemaToStructTypeConverter schemaConverter;

    public AccumuloStoreRelation(final SQLContext sqlContext,
                                 final List<Converter> converters,
                                 final View view,
                                 final AccumuloStore store,
                                 final User user) {
        this.sqlContext = sqlContext;
        this.view = view;
        this.store = store;
        this.user = user;
        this.schemaConverter = new SchemaToStructTypeConverter(store.getSchema(), view, converters);
        this.groups = this.schemaConverter.getGroups();
        this.structType = this.schemaConverter.getStructType();
        this.usedProperties = this.schemaConverter.getUsedProperties();
        this.propertyNeedsConversion = this.schemaConverter.getPropertyNeedsConversion();
        this.converterByProperty = this.schemaConverter.getConverterByProperty();
    }

    @Override
    public SQLContext sqlContext() {
        return sqlContext;
    }

    @Override
    public StructType schema() {
        return structType;
    }

    /**
     * Creates a <code>DataFrame</code> of all {@link Element}s from the specified groups.
     *
     * @return An {@link RDD} of {@link Row}s containing {@link Element}s whose group is in <code>groups</code>.
     */
    @Override
    public RDD<Row> buildScan() {
        try {
            LOGGER.info("Building GetRDDOfAllElements with view set to groups {}", StringUtils.join(groups, ','));
            final GetRDDOfAllElements operation = new GetRDDOfAllElements(sqlContext.sparkContext());
            operation.setView(view);
            final RDD<Element> rdd = store.execute(operation, user);
            return rdd.map(new ConvertElementToRow(usedProperties, propertyNeedsConversion, converterByProperty),
                    ClassTagConstants.ROW_CLASS_TAG);
        } catch (final OperationException e) {
            LOGGER.error("OperationException while executing operation: {}", e);
            return null;
        }
    }

    /**
     * Creates a <code>DataFrame</code> of all {@link Element}s from the specified groups with columns that are not
     * required filtered out.
     * <p>
     * Currently this does not push the projection down to the store (i.e. it should be implemented in an iterator,
     * not in the transform). Issue 320 refers to this.
     *
     * @param requiredColumns The columns to return.
     * @return An {@link RDD} of {@link Row}s containing the requested columns.
     */
    @Override
    public RDD<Row> buildScan(final String[] requiredColumns) {
        try {
            LOGGER.info("Building scan with required columns: {}", StringUtils.join(requiredColumns, ','));
            LOGGER.info("Building GetRDDOfAllElements with view set to groups {}", StringUtils.join(groups, ','));
            final GetRDDOfAllElements operation = new GetRDDOfAllElements(sqlContext.sparkContext());
            operation.setView(view);
            final RDD<Element> rdd = store.execute(operation, user);
            return rdd.map(new ConvertElementToRow(new LinkedHashSet<>(Arrays.asList(requiredColumns)),
                            propertyNeedsConversion, converterByProperty),
                    ClassTagConstants.ROW_CLASS_TAG);
        } catch (final OperationException e) {
            LOGGER.error("OperationException while executing operation {}", e);
            return null;
        }
    }

    /**
     * Creates a <code>DataFrame</code> of all {@link Element}s from the specified groups with columns that are not
     * required filtered out and with (some of) the supplied {@link Filter}s applied.
     * <p>
     * Note that Spark also applies the provided {@link Filter}s - applying them here is an optimisation to reduce
     * the amount of data transferred from the store to Spark's executors (this is known as "predicate pushdown").
     * <p>
     * Currently this does not push the projection down to the store (i.e. it should be implemented in an iterator,
     * not in the transform). Issue 320 refers to this.
     *
     * @param requiredColumns The columns to return.
     * @param filters         The {@link Filter}s to apply (these are applied before aggregation).
     * @return An {@link RDD} of {@link Row}s containing the requested columns.
     */
    @Override
    public RDD<Row> buildScan(final String[] requiredColumns, final Filter[] filters) {
        LOGGER.info("Building scan with required columns {} and {} filters ({})",
                StringUtils.join(requiredColumns, ','),
                filters.length,
                StringUtils.join(filters, ','));
        AbstractGetRDD<?> operation = new FiltersToOperationConverter(sqlContext, view, store.getSchema(), filters)
                .getOperation();
        if (operation == null) {
            // Null indicates that the filters resulted in no data (e.g. if group = X and group = Y, or if group = X
            // and there is no group X in the schema).
            return sqlContext.emptyDataFrame().rdd();
        }
        try {
            final RDD<Element> rdd = store.execute(operation, user);
            return rdd.map(new ConvertElementToRow(new LinkedHashSet<>(Arrays.asList(requiredColumns)),
                            propertyNeedsConversion, converterByProperty),
                    ClassTagConstants.ROW_CLASS_TAG);
        } catch (final OperationException e) {
            LOGGER.error("OperationException while executing operation {}", e);
            return null;
        }
    }

}
