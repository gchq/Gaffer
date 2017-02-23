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
package uk.gov.gchq.gaffer.spark.operation.dataframe;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.And;
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
import org.apache.spark.sql.sources.Or;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.filter.Exists;
import uk.gov.gchq.gaffer.function.filter.IsEqual;
import uk.gov.gchq.gaffer.function.filter.IsIn;
import uk.gov.gchq.gaffer.function.filter.IsLessThan;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.function.filter.Not;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema.SchemaToStructTypeConverter;
import uk.gov.gchq.gaffer.spark.operation.scalardd.AbstractGetRDD;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Converts a give {@link View} and array of Spark {@link Filter}s to an operation that returns data with as many
 * of the filters as possible converted to Gaffer filters and added to the view. This ensures that as much data
 * as possible is filtered out by the store.
 */
public class FiltersToOperationConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FiltersToOperationConverter.class);

    private final SQLContext sqlContext;
    private final View view;
    private final Schema schema;
    private final Filter[] filters;

    public FiltersToOperationConverter(final SQLContext sqlContext,
                                       final View view,
                                       final Schema schema,
                                       final Filter[] filters) {
        this.sqlContext = sqlContext;
        this.view = view;
        this.schema = schema;
        this.filters = Arrays.copyOf(filters, filters.length);
    }

    /**
     * Creates an operation to return an RDD in which as much filtering as possible has been carried out by Gaffer
     * in Accumulo's tablet servers before the data is sent to a Spark executor.
     * <p>
     * Note that when this is used within an operation to return a Dataframe, Spark will also carry out the
     * filtering itself, and therefore it is not essential for all filters to be applied. As many as possible
     * should be applied to reduce the amount of data sent from the data store to Spark's executors.
     * <p>
     * The following logic is used to create an operation and a view which removes as much data as possible as early
     * as possible:
     * - If the filters specify a particular group or groups is/are required then the view should only contain those
     * groups.
     * - If the filters specify a particular value for the vertex, source or destination then an operation to return
     * those directly is created (i.e. a GetRDDOfElements operation rather than a GetRDDOfAllElements operation). In
     * this case the view is created to ensure that only entities or only edges are returned as appropriate.
     * - Other filters are converted to Gaffer filters which are applied to the view.
     *
     * @return an operation to return the required data.
     */
    public AbstractGetRDD<?> getOperation() {
        // Check whether the filters specify any groups
        View derivedView = applyGroupFilters(view);
        if (derivedView == null) {
            return null;
        }
        // Check whether the filters specify a value for the vertex, source or destination.
        AbstractGetRDD<?> operation = applyVertexSourceDestinationFilters(derivedView);
        // Check whether the filters specify a property - if so can ignore groups that don't contain that property
        derivedView = operation.getView();
        operation = applyPropertyFilters(derivedView, operation);
        return operation;
    }

    private View applyGroupFilters(final View view) {
        View derivedView = View.fromJson(view.toCompactJson());
        final Set<String> groups = checkForGroups();
        if (groups == null) {
            // None of the filters specify a group or groups.
            return derivedView;
        } else if (groups.isEmpty()) {
            // Return null to indicate that groups are specified but no data can be returned
            return null;
        } else {
            View.Builder derivedViewBuilder = new View.Builder();
            boolean updated = false;
            for (final String group : groups) {
                // group might be an entity group, an edge group or neither (if the user specifies a filter with
                // "group=X" and X is not a group in the schema) - if neither then it can be ignored.
                if (view.isEntity(group)) {
                    updated = true;
                    LOGGER.info("Updating derived view with entity group {} and ViewElementDefinition {}",
                            group, view.getEntity(group));
                    derivedViewBuilder = derivedViewBuilder.entity(group, view.getEntity(group));
                } else if (view.isEdge(group)) {
                    updated = true;
                    LOGGER.info("Updating derived view with edge group {} and ViewElementDefinition {}",
                            group, view.getEdge(group));
                    derivedViewBuilder = derivedViewBuilder.edge(group, view.getEdge(group));
                }
            }
            if (!updated) {
                // None of the groups were found in the view, so no data should be returned.
                return null;
            } else {
                derivedView = derivedViewBuilder.build();
            }
        }
        return derivedView;
    }

    private AbstractGetRDD<?> applyVertexSourceDestinationFilters(final View view) {
        View clonedView = view.clone();
        AbstractGetRDD<?> operation = null;
        for (final Filter filter : filters) {
            if (filter instanceof EqualTo) {
                final EqualTo equalTo = (EqualTo) filter;
                final String attribute = equalTo.attribute();
                if (attribute.equals(SchemaToStructTypeConverter.VERTEX_COL_NAME)) {
                    // Only entities are relevant, so remove any edge groups from the view
                    LOGGER.info("Found EqualTo filter with attribute {}, setting views to only contain entity groups",
                            attribute);
                    View.Builder viewBuilder = new View.Builder();
                    for (final String entityGroup : view.getEntityGroups()) {
                        viewBuilder = viewBuilder.entity(entityGroup);
                    }
                    clonedView = viewBuilder.build();
                    LOGGER.info("Setting operation to GetRDDOfElements");
                    operation = new GetRDDOfElements<>(sqlContext.sparkContext(), new EntitySeed(equalTo.value()));
                    operation.setView(clonedView);
                    break;
                } else if (attribute.equals(SchemaToStructTypeConverter.SRC_COL_NAME)
                        || attribute.equals(SchemaToStructTypeConverter.DST_COL_NAME)) {
                    // Only edges are relevant, so remove any entity groups from the view
                    LOGGER.info("Found EqualTo filter with attribute {}, setting views to only contain edge groups",
                            attribute);
                    View.Builder viewBuilder = new View.Builder();
                    for (final String edgeGroup : view.getEdgeGroups()) {
                        viewBuilder = viewBuilder.edge(edgeGroup);
                    }
                    clonedView = viewBuilder.build();
                    LOGGER.info("Setting operation to GetRDDOfElements");
                    operation = new GetRDDOfElements<>(sqlContext.sparkContext(), new EntitySeed(equalTo.value()));
                    operation.setView(clonedView);
                    break;
                }
            }
        }
        if (operation == null) {
            LOGGER.debug("Setting operation to GetRDDOfAllElements");
            operation = new GetRDDOfAllElements(sqlContext.sparkContext());
            operation.setView(clonedView);
        }
        return operation;
    }

    private AbstractGetRDD<?> applyPropertyFilters(final View derivedView, final AbstractGetRDD<?> operation) {
        final List<Set<String>> groupsRelatedToFilters = new ArrayList<>();
        for (final Filter filter : filters) {
            final Set<String> groupsRelatedToFilter = getGroupsFromFilter(filter);
            if (groupsRelatedToFilter != null && !groupsRelatedToFilter.isEmpty()) {
                groupsRelatedToFilters.add(groupsRelatedToFilter);
            }
            LOGGER.info("Groups {} are related to filter {}", StringUtils.join(groupsRelatedToFilter, ','), filter);
        }
        LOGGER.info("Groups related to filters are: {}", StringUtils.join(groupsRelatedToFilters, ','));
        // Take the intersection of this list of groups - only these groups can be related to the query
        final Set<String> intersection = new HashSet<>(derivedView.getEntityGroups());
        intersection.addAll(derivedView.getEdgeGroups());
        for (final Set<String> groupsRelatedToFilter : groupsRelatedToFilters) {
            intersection.retainAll(groupsRelatedToFilter);
        }
        LOGGER.info("Groups that can be returned are: {}", StringUtils.join(intersection, ','));
        // Update view with filters and add to operation
        final Map<String, List<ConsumerFunctionContext<String, FilterFunction>>> groupToFunctions = new HashMap<>();
        for (final Filter filter : filters) {
            final Map<String, List<ConsumerFunctionContext<String, FilterFunction>>> map = getFunctionsFromFilter(filter);
            for (final Entry<String, List<ConsumerFunctionContext<String, FilterFunction>>> entry : map.entrySet()) {
                if (!groupToFunctions.containsKey(entry.getKey())) {
                    groupToFunctions.put(entry.getKey(), new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                }
                groupToFunctions.get(entry.getKey()).addAll(entry.getValue());
            }
        }
        LOGGER.info("The following functions will be applied for the given group:");
        for (final Entry<String, List<ConsumerFunctionContext<String, FilterFunction>>> entry : groupToFunctions.entrySet()) {
            LOGGER.info("Group = {}: ", entry.getKey());
            for (final ConsumerFunctionContext<String, FilterFunction> cfc : entry.getValue()) {
                LOGGER.info("\t{} {}", StringUtils.join(cfc.getSelection(), ','), cfc.getFunction());
            }
        }
        boolean updated = false;
        View.Builder builder = new View.Builder();

        for (final String group : derivedView.getEntityGroups()) {
            if (intersection.contains(group)) {
                if (groupToFunctions.get(group) != null) {
                    final ViewElementDefinition ved = new ViewElementDefinition.Builder()
                            .merge(derivedView.getEntity(group))
                            .postAggregationFilterFunctions(groupToFunctions.get(group))
                            .build();
                    LOGGER.info("Adding the following filter functions to the view for group {}:", group);
                    for (final ConsumerFunctionContext<String, FilterFunction> cfc : groupToFunctions.get(group)) {
                        LOGGER.info("\t{} {}", StringUtils.join(cfc.getSelection(), ','), cfc.getFunction());
                    }
                    builder = builder.entity(group, ved);
                    updated = true;
                } else {
                    LOGGER.info("Not adding any filter functions to the view for group {}", group);
                }
            }
        }
        for (final String group : derivedView.getEdgeGroups()) {
            if (intersection.contains(group)) {
                if (groupToFunctions.get(group) != null) {
                    final ViewElementDefinition ved = new ViewElementDefinition.Builder()
                            .merge(derivedView.getEdge(group))
                            .postAggregationFilterFunctions(groupToFunctions.get(group))
                            .build();
                    LOGGER.info("Adding the following filter functions to the view for group {}:", group);
                    for (final ConsumerFunctionContext<String, FilterFunction> cfc : groupToFunctions.get(group)) {
                        LOGGER.info("\t{} {}", StringUtils.join(cfc.getSelection(), ','), cfc.getFunction());
                    }
                    builder = builder.edge(group, ved);
                    updated = true;
                } else {
                    LOGGER.info("Not adding any filter functions to the view for group {}", group);
                }
            }
        }
        if (updated) {
            operation.setView(builder.build());
        } else {
            operation.setView(derivedView);
        }
        return operation;
    }

    private Set<String> getGroupsThatHaveProperty(final String property) {
        final Set<String> groups = new HashSet<>();
        for (final String entityGroup : schema.getEntityGroups()) {
            if (schema.getEntity(entityGroup).getProperties().contains(property)) {
                groups.add(entityGroup);
            }
        }
        for (final String edgeGroup : schema.getEdgeGroups()) {
            if (schema.getEdge(edgeGroup).getProperties().contains(property)) {
                groups.add(edgeGroup);
            }
        }
        return groups;
    }

    private Set<String> getGroupsFromFilter(final Filter filter) {
        if (filter instanceof EqualTo) {
            return getGroupsThatHaveProperty(((EqualTo) filter).attribute());
        } else if (filter instanceof EqualNullSafe) {
            return getGroupsThatHaveProperty(((EqualNullSafe) filter).attribute());
        } else if (filter instanceof GreaterThan) {
            return getGroupsThatHaveProperty(((GreaterThan) filter).attribute());
        } else if (filter instanceof GreaterThanOrEqual) {
            return getGroupsThatHaveProperty(((GreaterThanOrEqual) filter).attribute());
        } else if (filter instanceof LessThan) {
            return getGroupsThatHaveProperty(((LessThan) filter).attribute());
        } else if (filter instanceof LessThanOrEqual) {
            return getGroupsThatHaveProperty(((LessThanOrEqual) filter).attribute());
        } else if (filter instanceof In) {
            return getGroupsThatHaveProperty(((In) filter).attribute());
        } else if (filter instanceof IsNull) {
            // Return null to indicate all groups as the user could be deliberately finding rows for one group by
            // specifying that a field from another group should be null
            return null;
        } else if (filter instanceof IsNotNull) {
            return getGroupsThatHaveProperty(((IsNotNull) filter).attribute());
        } else if (filter instanceof And) {
            final And and = (And) filter;
            final Set<String> groups = new HashSet<>();
            final Set<String> leftGroups = getGroupsFromFilter(and.left());
            final Set<String> rightGroups = getGroupsFromFilter(and.right());
            if (leftGroups != null) {
                groups.addAll(leftGroups);
            }
            if (rightGroups != null) {
                groups.retainAll(rightGroups);
            }
            return groups;
        }
        return null;
    }

    /**
     * Converts a Spark {@link Filter} to a map from group to a list of Gaffer {@link ConsumerFunctionContext}s.
     * <p>
     * Note that Spark also applies all the filters provided to the <code>buildScan(String[], Filter[])</code> method
     * so not implementing some of the provided {@link Filter}s in Gaffer will not cause errors. However, as many as
     * possible should be implemented so that as much filtering as possible happens in iterators running in Accumulo's
     * tablet servers (this avoids unnecessary data transfer from Accumulo to Spark).
     *
     * @param filter The {@link Filter} to transform.
     * @return A map from {@link String} to {@link ConsumerFunctionContext}s implementing the provided {@link Filter}.
     */
    private Map<String, List<ConsumerFunctionContext<String, FilterFunction>>> getFunctionsFromFilter(final Filter filter) {
        final Map<String, List<ConsumerFunctionContext<String, FilterFunction>>> map = new HashMap<>();
        if (filter instanceof EqualTo) {
            // Not dealt with as requires a FilterFunction that returns null if either the controlValue or the
            // test value is null - the API of FilterFunction doesn't permit this.
        } else if (filter instanceof EqualNullSafe) {
            final EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
            final FilterFunction isEqual = new IsEqual(equalNullSafe.value());
            final List<String> properties = Collections.singletonList(equalNullSafe.attribute());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).add(new ConsumerFunctionContext<>(isEqual, properties));
                }
            }
            LOGGER.debug("Converted {} to IsEqual ({})", filter, properties.get(0));
        } else if (filter instanceof GreaterThan) {
            final GreaterThan greaterThan = (GreaterThan) filter;
            final FilterFunction isMoreThan = new IsMoreThan((Comparable<?>) greaterThan.value(), false);
            final List<String> properties = Collections.singletonList(greaterThan.attribute());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).add(new ConsumerFunctionContext<>(isMoreThan, properties));
                }
            }
            LOGGER.debug("Converted {} to isMoreThan ({})", filter, properties.get(0));
        } else if (filter instanceof GreaterThanOrEqual) {
            final GreaterThanOrEqual greaterThan = (GreaterThanOrEqual) filter;
            final FilterFunction isMoreThan = new IsMoreThan((Comparable<?>) greaterThan.value(), true);
            final List<String> properties = Collections.singletonList(greaterThan.attribute());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).add(new ConsumerFunctionContext<>(isMoreThan, properties));
                }
            }
            LOGGER.debug("Converted {} to IsMoreThan ({})", filter, properties.get(0));
        } else if (filter instanceof LessThan) {
            final LessThan lessThan = (LessThan) filter;
            final FilterFunction isLessThan = new IsLessThan((Comparable<?>) lessThan.value(), false);
            final List<String> properties = Collections.singletonList(lessThan.attribute());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).add(new ConsumerFunctionContext<>(isLessThan, properties));
                }
            }
            LOGGER.debug("Converted {} to IsLessThan ({})", filter, properties.get(0));
        } else if (filter instanceof LessThanOrEqual) {
            final LessThanOrEqual lessThan = (LessThanOrEqual) filter;
            final FilterFunction isLessThan = new IsLessThan((Comparable<?>) lessThan.value(), true);
            final List<String> properties = Collections.singletonList(lessThan.attribute());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).add(new ConsumerFunctionContext<>(isLessThan, properties));
                }
            }
            LOGGER.debug("Converted {} to LessThanOrEqual ({})", filter, properties.get(0));
        } else if (filter instanceof In) {
            final In in = (In) filter;
            final FilterFunction isIn = new IsIn(new HashSet<>(Arrays.asList(in.values())));
            final List<String> properties = Collections.singletonList(in.attribute());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).add(new ConsumerFunctionContext<>(isIn, properties));
                }
            }
            LOGGER.debug("Converted {} to IsIn ({})", filter, properties.get(0));
        } else if (filter instanceof IsNull) {
            final IsNull isNull = (IsNull) filter;
            final FilterFunction doesntExist = new Not(new Exists());
            final List<String> properties = Collections.singletonList(isNull.attribute());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).add(new ConsumerFunctionContext<>(doesntExist, properties));
                }
            }
            LOGGER.debug("Converted {} to Not(Exists) ({})", filter, properties.get(0));
        } else if (filter instanceof IsNotNull) {
            final IsNotNull isNotNull = (IsNotNull) filter;
            final FilterFunction exists = new Exists();
            final List<String> properties = Collections.singletonList(isNotNull.attribute());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).add(new ConsumerFunctionContext<>(exists, properties));
                }
            }
            LOGGER.debug("Converted {} to Exists ({})", filter, properties.get(0));
        } else if (filter instanceof And) {
            final And and = (And) filter;
            final Map<String, List<ConsumerFunctionContext<String, FilterFunction>>> left = getFunctionsFromFilter(and.left());
            final Map<String, List<ConsumerFunctionContext<String, FilterFunction>>> right = getFunctionsFromFilter(and.right());
            final Set<String> relevantGroups = getGroupsFromFilter(filter);
            if (relevantGroups != null) {
                for (final String group : relevantGroups) {
                    final List<ConsumerFunctionContext<String, FilterFunction>> concatFilters = new ArrayList<>();
                    if (left.get(group) != null) {
                        concatFilters.addAll(left.get(group));
                    }
                    if (right.get(group) != null) {
                        concatFilters.addAll(right.get(group));
                    }
                    if (!map.containsKey(group)) {
                        map.put(group, new ArrayList<ConsumerFunctionContext<String, FilterFunction>>());
                    }
                    map.get(group).addAll(concatFilters);
                }
            }
            LOGGER.debug("Converted {} to list of filters ({})",
                    filter,
                    StringUtils.join(map.entrySet(), ','));
        }
        return map;
    }

    /**
     * Iterates through all the filters looking for ones that specify a group or groups. The intersection of all of
     * these sets of groups is formed as all the filters are 'AND'ed together before data is provided to a Dataframe.
     * Only a group in the set of groups returned by this method can be returned from this query.
     * <p>
     * This method needs to distinguish between the following cases:
     * - None of the filters specify a group (in which case null is returned);
     * - One or more of the filters specify a group (in which case the intersection of the sets of groups specified
     * by the different filters is returned);
     * - Incompatible groups are specified (this is a special case of the above bullet where an empty set is returned).
     *
     * @return A set of strings containing the required groups.
     */
    private Set<String> checkForGroups() {
        final List<Set<String>> listOfGroups = new ArrayList<>();
        for (final Filter filter : filters) {
            final Set<String> groups = checkForGroups(filter);
            if (groups != null && !groups.isEmpty()) {
                listOfGroups.add(groups);
            }
        }
        if (listOfGroups.isEmpty()) {
            LOGGER.info("None of the filters specify a group");
            return null;
        }
        final Set<String> finalGroups = new HashSet<>();
        boolean first = true;
        for (final Set<String> groups : listOfGroups) {
            if (first) {
                finalGroups.addAll(groups);
                first = false;
            } else {
                finalGroups.retainAll(groups);
            }
        }
        LOGGER.info("The following groups are specified by the filters: {}", StringUtils.join(finalGroups, ','));
        return finalGroups;
    }

    /**
     * Returns the set of all groups in the filter, if the filter specifies that the group must be equal to a certain
     * value.
     *
     * @param filter The {@link Filter} that will be checked for groups.
     * @return A set of strings containing the required groups, <code>null</code> if no groups are specified in the
     * filter.
     */
    private Set<String> checkForGroups(final Filter filter) {
        if (filter instanceof EqualTo) {
            final EqualTo equalTo = (EqualTo) filter;
            if (equalTo.attribute().equals(SchemaToStructTypeConverter.GROUP)) {
                LOGGER.info("Filter {} specifies that {} should be {}", filter, SchemaToStructTypeConverter.GROUP,
                        equalTo.value());
                return Collections.singleton((String) equalTo.value());
            }
        } else if (filter instanceof Or) {
            final Or or = (Or) filter;
            if (or.left() instanceof EqualTo
                    && or.right() instanceof EqualTo
                    && ((EqualTo) or.left()).attribute().equals(SchemaToStructTypeConverter.GROUP)
                    && ((EqualTo) or.right()).attribute().equals(SchemaToStructTypeConverter.GROUP)) {
                final Set<String> groups = new HashSet<>();
                groups.add((String) ((EqualTo) or.left()).value());
                groups.add((String) ((EqualTo) or.right()).value());
                LOGGER.info("Filter {} specifies that {} should be {} or {}", filter, SchemaToStructTypeConverter.GROUP,
                        ((EqualTo) or.left()).value(), ((EqualTo) or.right()).value());
                return groups;
            }
        } else if (filter instanceof In) {
            final In in = (In) filter;
            if (in.attribute().equals(SchemaToStructTypeConverter.GROUP)) {
                final Set<String> groups = new HashSet<>();
                for (final Object o : in.values()) {
                    groups.add((String) o);
                }
                LOGGER.info("Filter {} specifies that {} should be in {}", filter, SchemaToStructTypeConverter.GROUP,
                        StringUtils.join(in.values(), ','));
                return groups;
            }
        }
        return null;
    }
}
