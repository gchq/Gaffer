/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.query;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;

public class QueryGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryGenerator.class);

    private final ParquetStore store;
    private final SchemaUtils schemaUtils;

    public QueryGenerator(final ParquetStore store) {
        this.store = store;
        this.schemaUtils = new SchemaUtils(store.getSchema());
    }

    public ParquetQuery getParquetQuery(final Operation operation) throws IOException, OperationException {
        if (operation instanceof GetAllElements) {
            return getPathsAndFiltersForAllElements((GetAllElements) operation);
        } else if (operation instanceof GetElements) {
            return getPathsAndFiltersForGetElements((GetElements) operation);
        } else {
            throw new OperationException("QueryGenerator can only handle GetAllElements and GetElements operations");
        }
    }

    private ParquetQuery getPathsAndFiltersForAllElements(final GetAllElements getAllElements)
            throws IOException, OperationException {
        // Stage 1: Use the view to identify all groups that might contain data
        final Set<String> allRelevantGroups = getRelevantGroups(getAllElements.getView());

        // Stage 2: Create map from group to list of files containing data for that group
        final Map<String, List<Path>> groupToPaths = new HashMap<>();
        for (final String group : allRelevantGroups) {
            groupToPaths.put(group, store.getFilesForGroup(group));
        }

        // Stage 3: For each of the above groups, create a Parquet predicate from the view and directedType
        final Map<String, Pair<FilterPredicate, Boolean>> groupToPredicate = new HashMap<>();
        for (final String group : groupToPaths.keySet()) {
            Pair<FilterPredicate, Boolean> filter = getPredicateFromView(getAllElements.getView(), group, schemaUtils.getEntityGroups().contains(group));
            if (schemaUtils.getEdgeGroups().contains(group)) {
                final FilterPredicate directedTypeFilter = getPredicateFromDirectedType(getAllElements.getDirectedType());
                if (null != filter) {
                    filter.setFirst(FilterPredicateUtils.and(filter.getFirst(), directedTypeFilter));
                } else {
                    filter = new Pair<>(directedTypeFilter, false);
                }
            }
            if (null != filter) {
                groupToPredicate.put(group, filter);
            }
        }

        // Stage 4: Build a ParquetQuery by iterating through the map from group to list of Paths
        final ParquetQuery parquetQuery = new ParquetQuery();
        for (final Map.Entry<String, List<Path>> entry : groupToPaths.entrySet()) {
            for (final Path path : entry.getValue()) {
                final String group = entry.getKey();
                final ParquetFileQuery fileQuery = groupToPredicate.containsKey(group) ?
                        new ParquetFileQuery(path, groupToPredicate.get(group).getFirst(), groupToPredicate.get(group).getSecond())
                                : new ParquetFileQuery(path, null, false);
                parquetQuery.add(group, fileQuery);
            }
        }
        LOGGER.info("Created ParquetQuery of {}", parquetQuery);
        return parquetQuery;
    }

    private Set<String> getRelevantGroups(final View view) {
        final Set<String> allRelevantGroups = new HashSet<>();
        if (null != view) {
            allRelevantGroups.addAll(view.getEntityGroups());
            allRelevantGroups.addAll(view.getEdgeGroups());
        } else {
            allRelevantGroups.addAll(schemaUtils.getEntityGroups());
            allRelevantGroups.addAll(schemaUtils.getEdgeGroups());
        }
        return allRelevantGroups;
    }

    private ParquetQuery getPathsAndFiltersForGetElements(final GetElements getElements)
            throws SerialisationException, OperationException {
        final Iterable<? extends ElementId> seeds = getElements.getInput();
        if (null == seeds || !seeds.iterator().hasNext()) {
            return new ParquetQuery();
        }

        // Stage 1: Use the view to identify all groups that might contain data
        final Set<String> allRelevantGroups = getRelevantGroups(getElements.getView());

        // Stage 2: For each of the above groups, create a Parquet predicate from the view and directedType
        final Map<String, Pair<FilterPredicate, Boolean>> groupToPredicate = new HashMap<>();
        for (final String group : allRelevantGroups) {
            Pair<FilterPredicate, Boolean> filter = getPredicateFromView(getElements.getView(), group, schemaUtils.getEntityGroups().contains(group));
            if (schemaUtils.getEdgeGroups().contains(group)) {
                final FilterPredicate directedTypeFilter = getPredicateFromDirectedType(getElements.getDirectedType());
                filter.setFirst(FilterPredicateUtils.and(filter.getFirst(), directedTypeFilter));
            }
            groupToPredicate.put(group, filter);
        }

        // Stage 3: Convert seeds to ParquetElementSeeds and create Stream of <group, ParquetElementSeed> pairs where
        // each seed appears once for each of the relevant groups
        final Stream<Pair<String, ParquetElementSeed>> groupAndSeeds = StreamSupport
                .stream(seeds.spliterator(), false)
                .flatMap(seed -> {
                    try {
                        return seedToParquetObject(seed, allRelevantGroups).stream();
                    } catch (final SerialisationException e) {
                        throw new RuntimeException("SerialisationException converting seed into a Parquet object", e);
                    }
                });

        // Stage 4: Convert stream of <group, ParquetElementSeed> pars to stream of tuples
        // <group, ParquetElementSeed, List<PathInfo>>
        final Stream<Tuple3<String, ParquetElementSeed, Set<PathInfo>>> groupSeedsAndPaths = groupAndSeeds
                .map(pair -> getRelevantFiles(pair.getFirst(), pair.getSecond()));

        // Stage 5: Create map from path to list of <group, reversed edge flag, Parquet seeds>
        // TODO: Currently this consumes the entire stream - need to do this in batches
        final List<Tuple3<String, ParquetElementSeed, Set<PathInfo>>> groupSeedsAndPathsList =
                groupSeedsAndPaths.collect(Collectors.toList());
        final Map<PathInfo, List<Tuple3<String, Boolean, ParquetElementSeed>>> pathToSeeds = new HashMap<>();
        for (final Tuple3<String, ParquetElementSeed, Set<PathInfo>> tuple : groupSeedsAndPathsList) {
            Set<PathInfo> paths = tuple.get2();
            for (final PathInfo pathInfo : paths) {
                if (!pathToSeeds.containsKey(pathInfo)) {
                    pathToSeeds.put(pathInfo, new ArrayList<>());
                }
                pathToSeeds.get(pathInfo).add(new Tuple3<>(tuple.get0(), pathInfo.isReversed(),  tuple.get1()));
            }
        }

        // Stage 6: Create ParquetQuery
        final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType = getElements.getIncludeIncomingOutGoing();
        final SeedMatching.SeedMatchingType seedMatchingType = getElements.getSeedMatching();
        final ParquetQuery parquetQuery = new ParquetQuery();
        for (final PathInfo pathInfo : pathToSeeds.keySet()) {
            List<Tuple3<String, Boolean, ParquetElementSeed>> seedList = pathToSeeds.get(pathInfo);
            FilterPredicate filterPredicate = seedsToPredicate(seedList, includeIncomingOutgoingType, seedMatchingType);
            if (null != filterPredicate) {
                final String group = pathInfo.getGroup();
                final Pair<FilterPredicate, Boolean> viewFilterPredicate = groupToPredicate.get(group);
                if (null != viewFilterPredicate) {
                    // Put view predicate first as filter for checking whether it matches one of many seeds could be complex
                    filterPredicate = FilterPredicateUtils.and(viewFilterPredicate.getFirst(), filterPredicate);
                }
                final ParquetFileQuery fileQuery = new ParquetFileQuery(pathInfo.getPath(), filterPredicate, viewFilterPredicate.getSecond());
                parquetQuery.add(group, fileQuery);
            }
        }
        LOGGER.info("Created ParquetQuery of {}", parquetQuery);
        return parquetQuery;
    }

    // TODO raise issue saying that could optimise so that only the filters that have not been fully applied
    // are reapplied, and it should be able to return the fact that all filters have been applied
    // Either the result is:
    //  - a filter and that fully applies the view
    //  - a filter and that doesn't fully apply the view
    //  - no filter and that doesn't fully apply the view
    //  - no filter and that fully applies the view
    // Boolean indicates whether logic was fully applied
    private Pair<FilterPredicate, Boolean> getPredicateFromView(final View view,
                                                                final String group,
                                                                final boolean isEntityGroup) throws SerialisationException, OperationException {
        if (null == view) {
            return new Pair<>(null, true);
        }
        final ViewElementDefinition ved = view.getElement(group);
        FilterPredicate filterPredicate = null;
        boolean fullyAppliedInAll = true;
        if (null != ved) {
            List<TupleAdaptedPredicate<String, ?>> preAggFilterFunctions = ved.getPreAggregationFilterFunctions();
            if (null != preAggFilterFunctions) {
                for (final TupleAdaptedPredicate<String, ?> filterFunctionContext : preAggFilterFunctions) {
                    final JavaPredicateToParquetPredicate predicateConverter =
                            new JavaPredicateToParquetPredicate(schemaUtils, filterFunctionContext.getPredicate(),
                                    filterFunctionContext.getSelection(), group);
                    filterPredicate = FilterPredicateUtils.and(filterPredicate, predicateConverter.getParquetPredicate());
                    if (!predicateConverter.isFullyApplied()) {
                        fullyAppliedInAll = false;
                    }
                }
            }
        }
        return new Pair<>(filterPredicate, fullyAppliedInAll);
    }

    private FilterPredicate getPredicateFromDirectedType(final DirectedType directedType) {
        if (DirectedType.DIRECTED == directedType) {
            return eq(booleanColumn(ParquetStore.DIRECTED), true);
        } else if (DirectedType.UNDIRECTED == directedType) {
            return eq(booleanColumn(ParquetStore.DIRECTED), false);
        }
        return null;
    }

    private List<Pair<String, ParquetElementSeed>> seedToParquetObject(final ElementId seed,
                                                                       final Set<String> groups) throws SerialisationException {
        final List<Pair<String, ParquetElementSeed>> groupToSeed = new ArrayList<>();
        for (final String group : groups) {
            if (this.schemaUtils.getEntityGroups().contains(group)) {
                groupToSeed.add(new Pair<>(group, seedToParquetObject(seed, group, true)));
            } else {
                groupToSeed.add(new Pair<>(group, seedToParquetObject(seed, group, false)));
            }
        }
        return groupToSeed;
    }

    private ParquetElementSeed seedToParquetObject(final ElementId seed, final String group, final boolean isEntityGroup) throws SerialisationException {
        final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
        final String column;
        if (isEntityGroup) {
            column = ParquetStore.VERTEX;
        } else {
            column = ParquetStore.SOURCE;
        }
        if (seed instanceof EntitySeed) {
            return new ParquetEntitySeed(seed, converter.gafferObjectToParquetObjects(column, ((EntitySeed) seed).getVertex()));

        } else {
            return converter.edgeIdToParquetObjects((EdgeSeed) seed);
        }
    }

    private Tuple3<String, ParquetElementSeed, Set<PathInfo>> getRelevantFiles(final String group,
                                                                               final ParquetElementSeed seed) {
        final Set<PathInfo> paths = getPathsForSeed(seed, group);
        return new Tuple3<>(group, seed, paths);
    }

    private Set<PathInfo> getPathsForSeed(final ParquetElementSeed parquetElementSeed, final String group) {
        final GraphPartitioner graphPartitioner = store.getGraphPartitioner();
        final boolean isEntityGroup = store.getSchema().getEntityGroups().contains(group);
        final List<Object[]> seeds = new ArrayList<>();
        if (parquetElementSeed instanceof ParquetEntitySeed) {
            seeds.add(((ParquetEntitySeed) parquetElementSeed).getSeed());
        } else {
            final ParquetEdgeSeed edgeSeed = (ParquetEdgeSeed) parquetElementSeed;
            if (!isEntityGroup) {
                Object[] seed = new Object[edgeSeed.getSource().length + edgeSeed.getDestination().length];
                for (int i = 0; i < edgeSeed.getSource().length; i++) {
                    seed[i] = edgeSeed.getSource()[i];
                }
                for (int i = edgeSeed.getSource().length; i < seed.length; i++) {
                    seed[i] = edgeSeed.getDestination()[i - edgeSeed.getSource().length];
                }
                seeds.add(seed);
            } else {
                seeds.add(edgeSeed.getSource());
                seeds.add(edgeSeed.getDestination());
            }
        }

        final List<PathInfo> paths = new ArrayList<>();
        for (final Object[] seed : seeds) {
            final List<Integer> partitionIds = graphPartitioner.getGroupPartitioner(group).getPartitionIds(seed);
            LOGGER.debug("Partition ids for seed {} in group {}: {}", seed, group, partitionIds);
            final PathInfo.FILETYPE fileType = isEntityGroup ? PathInfo.FILETYPE.ENTITY : PathInfo.FILETYPE.EDGE;
            partitionIds.forEach(id -> paths.add(new PathInfo(new Path(store.getFile(group, id)), group, fileType)));
            if (!isEntityGroup && parquetElementSeed instanceof ParquetEntitySeed) {
                final List<Integer> partitionIdsFromReversed = graphPartitioner.getGroupPartitionerForReversedEdges(group).getPartitionIds(seed);
                partitionIdsFromReversed.forEach(id -> paths.add(new PathInfo(new Path(store.getFileForReversedEdges(group, id)), group, PathInfo.FILETYPE.REVERSED_EDGE)));
            }
        }
        LOGGER.debug("Returning {} paths for seed {} and group {} (paths are {})",
                paths.size(), parquetElementSeed, group, paths);
        return paths.stream().collect(Collectors.toSet());
    }


    private FilterPredicate seedsToPredicate(final List<Tuple3<String, Boolean, ParquetElementSeed>> seedList,
                                             final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
                                             final SeedMatching.SeedMatchingType seedMatchingType) throws SerialisationException {
        FilterPredicate predicate = null;
        for (final Tuple3<String, Boolean, ParquetElementSeed> pair : seedList) {
            FilterPredicate pred = seedToPredicate(
                    pair.get2(), includeIncomingOutgoingType, seedMatchingType, pair.get0(), pair.get1());
            if (null != pred) {
                predicate = FilterPredicateUtils.or(predicate, pred);
            }
        }
        return predicate;
    }

    private FilterPredicate getIsEqualFilter(final String colName,
                                             final Object[] parquetObjects,
                                             final String group) {
        String[] paths = schemaUtils.getPaths(group, colName);
        if (null == paths) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            FilterPredicate tempFilter;
            if (parquetObjects[i] instanceof String) {
                tempFilter = eq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Boolean) {
                tempFilter = eq(booleanColumn(path), (Boolean) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = eq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = eq(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = eq(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = eq(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = eq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = eq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = eq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = eq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsEqual filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        return filter;
    }

    private FilterPredicate seedToPredicate(final ParquetElementSeed seed,
                                            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
                                            final SeedMatching.SeedMatchingType seedMatchingType,
                                            final String group,
                                            final boolean reversed) {
        final boolean isEntityGroup = schemaUtils.getEntityGroups().contains(group);
        FilterPredicate filter = null;
        final ElementId elementId = seed.getElementId();
        // Is it an entity group?
        if (isEntityGroup) {
            // EntityId case
            if (elementId instanceof EntityId) {
                filter = getIsEqualFilter(ParquetStore.VERTEX, ((ParquetEntitySeed) seed).getSeed(), group);
            } else {
                // EdgeId case
                // Does the seed type need to match the group type?
                final ParquetEdgeSeed edgeSeed = (ParquetEdgeSeed) seed;
                if (seedMatchingType != SeedMatching.SeedMatchingType.EQUAL) {
                    // Vertex = source of edge seed or Vertex = destination of edge seed
                    // look in partition 0 with filter src = A and partition 1 with filter src = B
                    filter = getIsEqualFilter(ParquetStore.VERTEX, edgeSeed.getSource(), group);
                    if (null != ((ParquetEdgeSeed) seed).getDestination()) {
                        filter = FilterPredicateUtils.or(filter, getIsEqualFilter(ParquetStore.VERTEX,
                                edgeSeed.getDestination(), group));
                    }
                }
            }
        } else {
            // Edge group
            // EntityId case
            if (elementId instanceof EntityId) {
                // If seedMatchingType is EQUAL then we can't find anything in an edge group
                if (seedMatchingType != SeedMatching.SeedMatchingType.EQUAL) {
                    if (includeIncomingOutgoingType == SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING) {
                        if (reversed) {
                            // Dst is seed
                            filter = getIsEqualFilter(ParquetStore.DESTINATION, ((ParquetEntitySeed) seed).getSeed(), group);
                        } else {
                            // Src is seed and edge is undirected
                            filter = getIsEqualFilter(ParquetStore.SOURCE, ((ParquetEntitySeed) seed).getSeed(), group);
                            filter = FilterPredicateUtils.and(filter, getIsEqualFilter(ParquetStore.DIRECTED, new Object[]{false}, group));
                        }
                    } else if (includeIncomingOutgoingType == SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING) {
                        if (reversed) {
                            // Dst is seed and edge is undirected
                            filter = getIsEqualFilter(ParquetStore.DESTINATION, ((ParquetEntitySeed) seed).getSeed(), group);
                            filter = FilterPredicateUtils.and(filter, getIsEqualFilter(ParquetStore.DIRECTED, new Object[]{false}, group));
                        } else {
                            // Src is seed
                            filter = getIsEqualFilter(ParquetStore.SOURCE, ((ParquetEntitySeed) seed).getSeed(), group);
                        }
                    } else {
                        if (reversed) {
                            // Dst is seed
                            filter = getIsEqualFilter(ParquetStore.DESTINATION, ((ParquetEntitySeed) seed).getSeed(), group);
                        } else {
                            // Src is seed
                            filter = getIsEqualFilter(ParquetStore.SOURCE, ((ParquetEntitySeed) seed).getSeed(), group);
                        }
                    }
                }
            } else {
                // EdgeId case
                final ParquetEdgeSeed edgeSeed = (ParquetEdgeSeed) seed;
                if (!reversed) {
                    // Src is source of edge seed and destination is destination of edge seed
                    filter = getIsEqualFilter(ParquetStore.SOURCE, edgeSeed.getSource(), group);
                    filter = FilterPredicateUtils.and(filter, getIsEqualFilter(ParquetStore.DESTINATION, edgeSeed.getDestination(), group)); // WRONG seed is already serialised source and dest - now fixed?
                    final DirectedType directedType = edgeSeed.getDirectedType();
                    if (directedType == DirectedType.DIRECTED) {
                        filter = FilterPredicateUtils.and(filter, getIsEqualFilter(ParquetStore.DIRECTED, new Object[]{true}, group));
                    } else if (directedType == DirectedType.UNDIRECTED) {
                        filter = FilterPredicateUtils.and(filter, getIsEqualFilter(ParquetStore.DIRECTED, new Object[]{false}, group));
                    }
                } else {
                    // TODO Optimise this - there are times this is unnecessary
                    filter = getIsEqualFilter(ParquetStore.DESTINATION, edgeSeed.getSource(), group);
                    filter = FilterPredicateUtils.and(filter, getIsEqualFilter(ParquetStore.SOURCE, edgeSeed.getDestination(), group));
                    final DirectedType directedType = edgeSeed.getDirectedType();
                    if (directedType == DirectedType.DIRECTED) {
                        filter = FilterPredicateUtils.and(filter, getIsEqualFilter(ParquetStore.DIRECTED, new Object[]{true}, group));
                    } else if (directedType == DirectedType.UNDIRECTED) {
                        filter = FilterPredicateUtils.and(filter, getIsEqualFilter(ParquetStore.DIRECTED, new Object[]{false}, group));
                    }

                }
            }
        }
        LOGGER.debug("Returning {} from seedToPredicate", filter);
        return filter;
    }
}
