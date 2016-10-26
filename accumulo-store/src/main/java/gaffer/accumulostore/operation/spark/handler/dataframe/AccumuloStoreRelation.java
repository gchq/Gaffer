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
package gaffer.accumulostore.operation.spark.handler.dataframe;

import gaffer.accumulostore.AccumuloStore;
import gaffer.data.element.Element;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.FilterFunction;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.simple.filter.Exists;
import gaffer.function.simple.filter.IsEqual;
import gaffer.function.simple.filter.IsIn;
import gaffer.function.simple.filter.IsLessThan;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.function.simple.filter.Not;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.simple.spark.AbstractGetRDD;
import gaffer.operation.simple.spark.Converter;
import gaffer.operation.simple.spark.GetRDDOfAllElements;
import gaffer.operation.simple.spark.GetRDDOfElements;
import gaffer.user.User;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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

    enum EntityOrEdge {
        ENTITY, EDGE
    }

    public static final String GROUP = "group";
    public static final String VERTEX_COL_NAME = "vertex";
    public static final String SRC_COL_NAME = "src";
    public static final String DST_COL_NAME = "dst";

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
        // If any of the filters can be translated into Accumulo queries (i.e. specifying ranges rather than a full
        // table scan) then do this.
        AbstractGetRDD<?> operation = null;
        for (final Filter filter : filters) {
            if (filter instanceof EqualTo) {
                final EqualTo equalTo = (EqualTo) filter;
                final String attribute = equalTo.attribute();
                if (attribute.equals(SRC_COL_NAME) || attribute.equals(DST_COL_NAME) || attribute.equals(VERTEX_COL_NAME)) {
                    LOGGER.debug("Found EqualTo filter with attribute {}, creating GetRDDOfElements", attribute);
                    operation = new GetRDDOfElements<>(sqlContext.sparkContext(), new EntitySeed(equalTo.value()));
                }
                break;
            }
        }
        if (operation == null) {
            LOGGER.debug("Creating GetRDDOfAllElements");
            operation = new GetRDDOfAllElements(sqlContext.sparkContext());
        }
        // Create view based on filters and add to operation
        final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> filterList = new ArrayList<>();
        for (final Filter filter : filters) {
            filterList.addAll(getFunctionsFromFilter(filter));
        }
        View.Builder builder = new View.Builder();
        for (final String group : view.getEntityGroups()) {
            final ViewElementDefinition ved = view.getEntity(group);
            ved.addPostAggregationElementFilterFunctions(filterList);
            builder = builder.entity(group, ved);
        }
        for (final String group : view.getEdgeGroups()) {
            final ViewElementDefinition ved = view.getEdge(group);
            ved.addPostAggregationElementFilterFunctions(filterList);
            builder = builder.edge(group, ved);
        }
        operation.setView(builder.build());
        // Create RDD
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

    /**
     * Converts a Spark {@link Filter} to a Gaffer {@link ConsumerFunctionContext}.
     * <p>
     * Note that Spark also applies all the filters provided to the {@link #buildScan(String[], Filter[])} method so
     * not implementing some of the provided {@link Filter}s in Gaffer will not cause errors. However, as many as
     * possible should be implemented so that as much filtering as possible happens in iterators running in Accumulo's
     * tablet servers (this avoids unnecessary data transfer from Accumulo to Spark).
     *
     * @param filter The {@link Filter} to transform.
     * @return Gaffer function(s) implementing the provided {@link Filter}.
     */
    private List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> getFunctionsFromFilter(final Filter filter) {
        final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> functions = new ArrayList<>();
        if (filter instanceof EqualTo) {
            // Not dealt with as requires a FilterFunction that returns null if either the controlValue or the
            // test value is null - the API of FilterFunction doesn't permit this.
        } else if (filter instanceof EqualNullSafe) {
            final EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
            final FilterFunction isEqual = new IsEqual(equalNullSafe.value());
            final List<ElementComponentKey> ecks = Collections.singletonList(new ElementComponentKey(equalNullSafe.attribute()));
            functions.add(new ConsumerFunctionContext<>(isEqual, ecks));
            LOGGER.debug("Converted {} to IsEqual ({})", filter, ecks.get(0));
        } else if (filter instanceof GreaterThan) {
            final GreaterThan greaterThan = (GreaterThan) filter;
            final FilterFunction isMoreThan = new IsMoreThan((Comparable<?>) greaterThan.value(), false);
            final List<ElementComponentKey> ecks = Collections.singletonList(new ElementComponentKey(greaterThan.attribute()));
            functions.add(new ConsumerFunctionContext<>(isMoreThan, ecks));
            LOGGER.debug("Converted {} to isMoreThan ({})", filter, ecks.get(0));
        } else if (filter instanceof GreaterThanOrEqual) {
            final GreaterThanOrEqual greaterThan = (GreaterThanOrEqual) filter;
            final FilterFunction isMoreThan = new IsMoreThan((Comparable<?>) greaterThan.value(), true);
            final List<ElementComponentKey> ecks = Collections.singletonList(new ElementComponentKey(greaterThan.attribute()));
            functions.add(new ConsumerFunctionContext<>(isMoreThan, ecks));
            LOGGER.debug("Converted {} to IsMoreThan ({})", filter, ecks.get(0));
        } else if (filter instanceof LessThan) {
            final LessThan lessThan = (LessThan) filter;
            final FilterFunction isLessThan = new IsLessThan((Comparable<?>) lessThan.value(), false);
            final List<ElementComponentKey> ecks = Collections.singletonList(new ElementComponentKey(lessThan.attribute()));
            functions.add(new ConsumerFunctionContext<>(isLessThan, ecks));
            LOGGER.debug("Converted {} to IsLessThan ({})", filter, ecks.get(0));
        } else if (filter instanceof LessThanOrEqual) {
            final LessThanOrEqual lessThan = (LessThanOrEqual) filter;
            final FilterFunction isLessThan = new IsLessThan((Comparable<?>) lessThan.value(), true);
            final List<ElementComponentKey> ecks = Collections.singletonList(new ElementComponentKey(lessThan.attribute()));
            functions.add(new ConsumerFunctionContext<>(isLessThan, ecks));
            LOGGER.debug("Converted {} to LessThanOrEqual ({})", filter, ecks.get(0));
        } else if (filter instanceof In) {
            final In in = (In) filter;
            final FilterFunction isIn = new IsIn(new HashSet<>(Arrays.asList(in.values())));
            final List<ElementComponentKey> ecks = Collections.singletonList(new ElementComponentKey(in.attribute()));
            functions.add(new ConsumerFunctionContext<>(isIn, ecks));
            LOGGER.debug("Converted {} to IsIn ({})", filter, ecks.get(0));
        } else if (filter instanceof IsNull) {
            final IsNull isNull = (IsNull) filter;
            final FilterFunction doesntExist = new Not(new Exists());
            final List<ElementComponentKey> ecks = Collections.singletonList(new ElementComponentKey(isNull.attribute()));
            functions.add(new ConsumerFunctionContext<>(doesntExist, ecks));
            LOGGER.debug("Converted {} to Not(Exists) ({})", filter, ecks.get(0));
        } else if (filter instanceof IsNotNull) {
            final IsNotNull isNotNull = (IsNotNull) filter;
            final FilterFunction exists = new Exists();
            final List<ElementComponentKey> ecks = Collections.singletonList(new ElementComponentKey(isNotNull.attribute()));
            functions.add(new ConsumerFunctionContext<>(exists, ecks));
            LOGGER.debug("Converted {} to Exists ({})", filter, ecks.get(0));
        } else if (filter instanceof And) {
            final And and = (And) filter;
            final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> left = getFunctionsFromFilter(and.left());
            final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> right = getFunctionsFromFilter(and.right());
            functions.addAll(left);
            functions.addAll(right);
            LOGGER.debug("Converted {} to 2 filters ({} and {})", filter, StringUtils.join(left, ','), StringUtils.join(right, ','));
        }
        return functions;
    }

}
