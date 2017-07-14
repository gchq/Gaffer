/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GroupIndex;
import uk.gov.gchq.gaffer.parquetstore.index.MinMaxPath;
import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsFalse;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

public final class ParquetFilterUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFilterUtils.class);

    private ParquetFilterUtils() {
    }

    public static Tuple2<Map<Path, FilterPredicate>, Boolean> buildPathToFilterMap(
            final SchemaUtils schemaUtils,
            final View view,
            final DirectedType directedType,
            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
            final SeedMatching.SeedMatchingType seedMatchingType,
            final Iterable<? extends ElementId> seeds,
            final String dataDir,
            final GraphIndex graphIndex)
            throws SerialisationException, OperationException {
        if (view == null) {
            return noViewPathToFilter(includeIncomingOutgoingType, seedMatchingType, seeds, schemaUtils, graphIndex, dataDir);
        } else {
            final Set<String> viewEdgeGroups = view.getEdgeGroups();
            final Set<String> viewEntityGroups = view.getEntityGroups();
            boolean needsValidation = false;
            if (viewEdgeGroups != null || viewEntityGroups != null) {
                HashMap<Path, FilterPredicate> pathToFilter = new HashMap<>();
                if (viewEdgeGroups != null) {
                    final Tuple2<Map<Path, FilterPredicate>, Boolean> results = edgeViewPathToFilter(
                            includeIncomingOutgoingType, seedMatchingType, directedType, seeds, schemaUtils, graphIndex, view, dataDir);
                    if (results.get1()) {
                        needsValidation = true;
                    }
                    pathToFilter.putAll(results.get0());
                }
                if (viewEntityGroups != null) {
                    final Tuple2<Map<Path, FilterPredicate>, Boolean> results = entityViewPathToFilter(
                            includeIncomingOutgoingType, seedMatchingType, directedType, seeds, schemaUtils, graphIndex, view, dataDir);
                    if (results.get1()) {
                        needsValidation = true;
                    }
                    pathToFilter.putAll(results.get0());
                }
                return new Tuple2<>(pathToFilter, needsValidation);
            } else {
                return noViewPathToFilter(includeIncomingOutgoingType, seedMatchingType, seeds, schemaUtils, graphIndex, dataDir);
            }
        }
    }

    private static Tuple2<Map<Path, FilterPredicate>, Boolean> noViewPathToFilter(
            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
            final SeedMatching.SeedMatchingType seedMatchingType,
            final Iterable<? extends ElementId> seeds,
            final SchemaUtils schemaUtils,
            final GraphIndex graphIndex,
            final String dataDir) throws SerialisationException, OperationException {
        final Map<Path, FilterPredicate> pathToFilter = new HashMap<>();
        final Set<String> indexKeys = graphIndex.groupsIndexed();
        for (final String group : schemaUtils.getEntityGroups()) {
            if (indexKeys.contains(group)) {
                pathToFilter.putAll(buildSeedFilter(includeIncomingOutgoingType, seedMatchingType, seeds, schemaUtils, group, true, graphIndex, dataDir));
            }
        }
        for (final String group : schemaUtils.getEdgeGroups()) {
            if (indexKeys.contains(group)) {
                pathToFilter.putAll(buildSeedFilter(includeIncomingOutgoingType, seedMatchingType, seeds, schemaUtils, group, false, graphIndex, dataDir));
            }
        }
        if (seeds != null && pathToFilter.isEmpty()) {
            return new Tuple2<>(new HashMap<>(), false);
        }
        if (pathToFilter.isEmpty()) {
            pathToFilter.put(new Path(dataDir + "/" + ParquetStoreConstants.GRAPH), null);
        }
        return new Tuple2<>(pathToFilter, false);
    }

    private static Tuple2<Map<Path, FilterPredicate>, Boolean> edgeViewPathToFilter(
            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
            final SeedMatching.SeedMatchingType seedMatchingType,
            final DirectedType directedType,
            final Iterable<? extends ElementId> seeds,
            final SchemaUtils schemaUtils,
            final GraphIndex graphIndex,
            final View view,
            final String dataDir) throws SerialisationException, OperationException {
        final Map<Path, FilterPredicate> pathToFilter = new HashMap<>();
        final Set<String> indexKeys = graphIndex.groupsIndexed();
        boolean needValidation = false;
        for (final String edgeGroup : view.getEdgeGroups()) {
            if (indexKeys.contains(edgeGroup)) {
                // Build group filter
                final Tuple2<FilterPredicate, Boolean> groupFilter = buildGroupFilter(view, schemaUtils, edgeGroup, directedType, false);
                if (groupFilter != null && groupFilter.get1()) {
                    needValidation = true;
                }
                // Build seed filter
                final Map<Path, FilterPredicate> tempPathToFilter = buildSeedFilter(
                        includeIncomingOutgoingType, seedMatchingType, seeds, schemaUtils, edgeGroup, false, graphIndex, dataDir);
                if (seeds != null && tempPathToFilter.isEmpty()) {
                    return new Tuple2<>(new HashMap<>(), false);
                }
                // Add filter to map
                if (tempPathToFilter.isEmpty()) {
                    if (groupFilter != null) {
                        pathToFilter.put(new Path(ParquetStore.getGroupDirectory(edgeGroup, ParquetStoreConstants.SOURCE, dataDir)),
                                groupFilter.get0());
                    } else {
                        pathToFilter.put(new Path(ParquetStore.getGroupDirectory(edgeGroup, ParquetStoreConstants.SOURCE, dataDir)), null);
                    }
                } else {
                    if (groupFilter != null && groupFilter.get0() != null) {
                        for (final Map.Entry<Path, FilterPredicate> entry : tempPathToFilter.entrySet()) {
                            pathToFilter.put(entry.getKey(), andFilter(entry.getValue(), groupFilter.get0()));
                        }
                    } else {
                        pathToFilter.putAll(tempPathToFilter);
                    }
                }
            }
        }
        return new Tuple2<>(pathToFilter, needValidation);
    }

    private static Tuple2<Map<Path, FilterPredicate>, Boolean> entityViewPathToFilter(
            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
            final SeedMatching.SeedMatchingType seedMatchingType,
            final DirectedType directedType,
            final Iterable<? extends ElementId> seeds,
            final SchemaUtils schemaUtils,
            final GraphIndex graphIndex,
            final View view,
            final String dataDir) throws SerialisationException, OperationException {
        final Map<Path, FilterPredicate> pathToFilter = new HashMap<>();
        final Set<String> indexKeys = graphIndex.groupsIndexed();
        boolean needValidation = false;
        for (final String entityGroup : view.getEntityGroups()) {
            if (indexKeys.contains(entityGroup)) {
                // Build group filter
                final Tuple2<FilterPredicate, Boolean> groupFilter = buildGroupFilter(view, schemaUtils, entityGroup, directedType, true);
                if (groupFilter != null && groupFilter.get1()) {
                    needValidation = true;
                }
                // Build seed filter
                final Map<Path, FilterPredicate> tempPathToFilter = buildSeedFilter(
                        includeIncomingOutgoingType, seedMatchingType, seeds, schemaUtils, entityGroup, true, graphIndex, dataDir);
                if (seeds != null && tempPathToFilter.isEmpty()) {
                    return new Tuple2<>(new HashMap<>(), false);
                }
                // Add filter to map
                if (tempPathToFilter.isEmpty()) {
                    if (groupFilter != null) {
                        pathToFilter.put(new Path(ParquetStore.getGroupDirectory(entityGroup, ParquetStoreConstants.VERTEX, dataDir)),
                                groupFilter.get0());
                    } else {
                        pathToFilter.put(new Path(ParquetStore.getGroupDirectory(entityGroup, ParquetStoreConstants.VERTEX, dataDir)), null);
                    }
                } else {
                    if (groupFilter != null && groupFilter.get0() != null) {
                        for (final Map.Entry<Path, FilterPredicate> entry : tempPathToFilter.entrySet()) {
                            pathToFilter.put(entry.getKey(), andFilter(entry.getValue(), groupFilter.get0()));
                        }
                    } else {
                        pathToFilter.putAll(tempPathToFilter);
                    }
                }
            }
        }
        return new Tuple2<>(pathToFilter, needValidation);
    }

    private static Map<Path, FilterPredicate> buildSeedFilter(
            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
            final SeedMatching.SeedMatchingType seedMatchingType,
            final Iterable<? extends ElementId> seeds,
            final SchemaUtils schemaUtils,
            final String group,
            final boolean isEntityGroup,
            final GraphIndex graphIndex,
            final String rootDir)
            throws SerialisationException, OperationException {
        final Map<Path, FilterPredicate> pathToFilter = new HashMap<>();
        if (seeds != null) {
            final Iterator<? extends ElementId> seedIter = seeds.iterator();
            if (seedIter.hasNext()) {
                SeedComparator comparator = new SeedComparator();
                final String identifier;
                if (isEntityGroup) {
                    identifier = ParquetStoreConstants.VERTEX;
                } else {
                    identifier = ParquetStoreConstants.SOURCE;
                }
                final Tuple2<ArrayList<Object[]>, HashMap<Object[], Tuple2<Object, DirectedType>>> prepSeedsResult =
                        prepSeeds(seeds, identifier, schemaUtils, group, comparator);
                final ArrayList<Object[]> sortedSeeds = prepSeedsResult.get0();
                final HashMap<Object[], Tuple2<Object, DirectedType>> seed2Parts = prepSeedsResult.get1();
                // Build graph path to filter
                pathToFilter.putAll(buildSeedFilterForIndex(includeIncomingOutgoingType,
                        seedMatchingType, sortedSeeds, identifier, graphIndex.getGroup(group), schemaUtils, group,
                        isEntityGroup, seed2Parts, comparator, rootDir));
                if (!isEntityGroup) {
                    // Build reverseEdges path to filter
                    final Map<Path, FilterPredicate> reverseEdgePathToFilter = buildSeedFilterForIndex(includeIncomingOutgoingType,
                            seedMatchingType, sortedSeeds, ParquetStoreConstants.DESTINATION, graphIndex.getGroup(group), schemaUtils, group,
                            false, seed2Parts, comparator, rootDir);
                    // Merge results
                    pathToFilter.putAll(reverseEdgePathToFilter);
                }

            }
        }
        return pathToFilter;
    }

    private static Map<Path, FilterPredicate> buildSeedFilterForIndex(
            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
            final SeedMatching.SeedMatchingType seedMatchingType,
            final List<Object[]> sortedSeeds,
            final String identifier,
            final GroupIndex groupIndex,
            final SchemaUtils schemaUtils,
            final String group,
            final boolean isEntityGroup,
            final Map<Object[], Tuple2<Object, DirectedType>> seed2Parts,
            final SeedComparator comparator,
            final String rootDir) throws OperationException, SerialisationException {
        Map<Path, FilterPredicate> newPathToFilter = new HashMap<>();
        final Iterator<Object[]> sortedSeedsIter = sortedSeeds.iterator();
        final Iterator<MinMaxPath> indexIter = groupIndex.getColumn(identifier).getIterator();
        Object[] currentSeed;
        if (indexIter.hasNext()) {
            MinMaxPath indexEntry = indexIter.next();
            while (sortedSeedsIter.hasNext()) {
                currentSeed = sortedSeedsIter.next();
                boolean nextSeed = false;
                while (!nextSeed && indexEntry != null) {
                    final Object min = indexEntry.getMin();
                    final Object max = indexEntry.getMax();
                    final String file = indexEntry.getPath();
                    LOGGER.debug("Current file: {}", file);
                    // If min <= seed && max >= seed
                    final int min2seed = comparator.compare(min, currentSeed);
                    LOGGER.debug("min2seed comparator: {}", min2seed);
                    final int max2seed = comparator.compare(max, currentSeed);
                    LOGGER.debug("max2seed comparator: {}", max2seed);
                    if (min2seed < 1 && max2seed >= 0) {
                        final String fullFilePath = ParquetStore.getGroupDirectory(group, identifier, rootDir) + "/" + file;
                        newPathToFilter = addPathToSeedFilter(includeIncomingOutgoingType, seedMatchingType,
                                new Path(fullFilePath), currentSeed, identifier, schemaUtils, group,
                                seed2Parts.get(currentSeed), newPathToFilter, isEntityGroup);
                        nextSeed = true;
                    } else if (min2seed > 0) {
                        nextSeed = true;
                    } else {
                        if (indexIter.hasNext()) {
                            indexEntry = indexIter.next();
                        } else {
                            indexEntry = null;
                        }
                    }
                }
            }
        }
        return newPathToFilter;
    }

    /**
     * Returns a {@link Tuple2} in which the first entry is a sorted {@link List} of the seeds converted to the form in
     * which they appear in the Parquet files, and the second entry is a {@link Map} from the seeds to a {@link Tuple2}
     * which is <code>null</code> if the seed is an {@link EntitySeed} and consists of the destination vertex and
     * directed type if the seed is an {@link EdgeSeed}.
     *
     * @param seeds the {@link ElementId}s to query for
     * @param identifier the column that the seed relates to
     * @param schemaUtils a {@link SchemaUtils} used to get a {@link GafferGroupObjectConverter} to convert the seeds to
     *                    their equivalent Parquet versions
     * @param group the group that is currently being queried
     * @param comparator the {@link SeedComparator} used to order the seeds
     * @return a {@link Tuple2} in which the first entry is a sorted {@link List} of the seeds converted to the form in
     * which they appear in the Parquet files, and the second entry is a {@link Map} from the seeds to a {@link Tuple2}
     * which is <code>null</code> if the seed is an {@link EntitySeed} and consists of the destination vertex and
     * directed type if the seed is an {@link EdgeSeed}.
     * @throws SerialisationException if the conversion from the seed to corresponding Parquet objects fails
     */
    private static Tuple2<ArrayList<Object[]>, HashMap<Object[], Tuple2<Object, DirectedType>>> prepSeeds(
            final Iterable<? extends ElementId> seeds,
            final String identifier,
            final SchemaUtils schemaUtils,
            final String group,
            final SeedComparator comparator) throws SerialisationException {
        final HashMap<Object[], Tuple2<Object, DirectedType>> seed2parts = new HashMap<>();
        final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
        for (final ElementId elementSeed : seeds) {
            if (elementSeed instanceof EntitySeed) {
                final Object[] serialisedSeed = converter
                        .gafferObjectToParquetObjects(identifier, ((EntitySeed) elementSeed).getVertex());
                seed2parts.put(serialisedSeed, null);
            } else {
                final EdgeSeed edgeSeed = (EdgeSeed) elementSeed;
                final Object[] serialisedSeed = converter.gafferObjectToParquetObjects(identifier, edgeSeed.getSource());
                seed2parts.put(serialisedSeed, new Tuple2<>(edgeSeed.getDestination(), edgeSeed.getDirectedType()));
            }
        }
        final ArrayList<Object[]> sortedSeeds = new ArrayList<>();
        for (final Object key : seed2parts.keySet()) {
            sortedSeeds.add((Object[]) key);
        }
        sortedSeeds.sort(comparator);
        return new Tuple2<>(sortedSeeds, seed2parts);
    }

    private static Map<Path, FilterPredicate> addPathToSeedFilter(
            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
            final SeedMatching.SeedMatchingType seedMatchingType,
            final Path path,
            final Object[] currentSeed,
            final String identifier,
            final SchemaUtils schemaUtils,
            final String group,
            final Tuple2<Object, DirectedType> parts,
            final Map<Path, FilterPredicate> newPathToFilter,
            final Boolean isEntityGroup) throws OperationException, SerialisationException {
        FilterPredicate filter = null;
        // Is it an entity group?
        if (isEntityGroup) {
            // Is it an entityId?
            if (parts == null) {
                filter = createSeedFilter(currentSeed, ParquetStoreConstants.VERTEX, schemaUtils.getColumnToPaths(group));
            } else {
                // Does the seed type need to match the group type?
                if (seedMatchingType != SeedMatching.SeedMatchingType.EQUAL) {
                    filter = createSeedFilter(currentSeed, ParquetStoreConstants.VERTEX, schemaUtils.getColumnToPaths(group));
                    filter = orFilter(filter, addIsEqualFilter(ParquetStoreConstants.VERTEX, parts.get0(), schemaUtils, group));
                }
            }
        } else {
            // Is it an entityId?
            if (parts == null) {
                if (seedMatchingType != SeedMatching.SeedMatchingType.EQUAL) {
                    // Does it matter if the vertex is the incoming or outgoing edge?
                    if (includeIncomingOutgoingType == SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING) {
                        if (ParquetStoreConstants.DESTINATION.equals(identifier)) {
                            // Add filter dir == false
                            filter = createSeedFilter(currentSeed, ParquetStoreConstants.DESTINATION, schemaUtils.getColumnToPaths(group));
                            filter = andFilter(filter, not(createSeedFilter(currentSeed, ParquetStoreConstants.SOURCE, schemaUtils.getColumnToPaths(group))));
                        } else {
                            filter = createSeedFilter(currentSeed, ParquetStoreConstants.SOURCE, schemaUtils.getColumnToPaths(group));
                            filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DIRECTED, false, schemaUtils, group));
                        }
                    } else if (includeIncomingOutgoingType == SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING) {
                        if (ParquetStoreConstants.SOURCE.equals(identifier)) {
                            // Add filter dir == true
                            filter = createSeedFilter(currentSeed, ParquetStoreConstants.SOURCE, schemaUtils.getColumnToPaths(group));
                        } else {
                            filter = createSeedFilter(currentSeed, ParquetStoreConstants.DESTINATION, schemaUtils.getColumnToPaths(group));
                            filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DIRECTED, false, schemaUtils, group));
                            filter = andFilter(filter, not(createSeedFilter(currentSeed, ParquetStoreConstants.SOURCE, schemaUtils.getColumnToPaths(group))));
                        }
                    } else {
                        filter = createSeedFilter(currentSeed, identifier, schemaUtils.getColumnToPaths(group));
                        if (ParquetStoreConstants.DESTINATION.equals(identifier)) {
                            filter = andFilter(filter, not(createSeedFilter(currentSeed, ParquetStoreConstants.SOURCE, schemaUtils.getColumnToPaths(group))));
                        }
                    }
                }
            } else {
                if (ParquetStoreConstants.SOURCE.equals(identifier)) {
                    filter = createSeedFilter(currentSeed, ParquetStoreConstants.SOURCE, schemaUtils.getColumnToPaths(group));
                    filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DESTINATION, parts.get0(), schemaUtils, group));
                    final DirectedType directedType = parts.get1();
                    if (directedType == DirectedType.DIRECTED) {
                        filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DIRECTED, true, schemaUtils, group));
                    } else if (directedType == DirectedType.UNDIRECTED) {
                        filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DIRECTED, false, schemaUtils, group));
                    }
                }
            }
        }
        if (filter != null) {
            newPathToFilter.put(path, orFilter(filter, newPathToFilter.get(path)));
        }
        return newPathToFilter;
    }

    private static FilterPredicate andFilter(final FilterPredicate a, final FilterPredicate b) {
        if (a == null && b == null) {
            return null;
        } else if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        } else {
            return and(a, b);
        }
    }

    private static FilterPredicate orFilter(final FilterPredicate a, final FilterPredicate b) {
        if (a == null && b == null) {
            return null;
        } else if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        } else {
            return or(a, b);
        }
    }

    private static Tuple2<FilterPredicate, Boolean> andFilter(final Tuple2<FilterPredicate, Boolean> filter, final Tuple2<FilterPredicate, Boolean> newFilter) {
        final Tuple2<FilterPredicate, Boolean> outputFilter;
        if (filter != null) {
            if (filter.get1()) {
                if (filter.get0() != null && newFilter.get0() != null) {
                    outputFilter = new Tuple2<>(and(filter.get0(), newFilter.get0()), true);
                } else if (filter.get0() != null) {
                    outputFilter = new Tuple2<>(filter.get0(), true);
                } else if (newFilter.get0() != null) {
                    outputFilter = new Tuple2<>(newFilter.get0(), true);
                } else {
                    outputFilter = new Tuple2<>(null, true);
                }
            } else {
                if (newFilter.get1()) {
                    if (filter.get0() != null && newFilter.get0() != null) {
                        outputFilter = new Tuple2<>(and(filter.get0(), newFilter.get0()), true);
                    } else if (filter.get0() != null) {
                        outputFilter = new Tuple2<>(filter.get0(), true);
                    } else if (newFilter.get0() != null) {
                        outputFilter = new Tuple2<>(newFilter.get0(), true);
                    } else {
                        outputFilter = new Tuple2<>(null, true);
                    }
                } else {
                    if (filter.get0() != null && newFilter.get0() != null) {
                        outputFilter = new Tuple2<>(and(filter.get0(), newFilter.get0()), false);
                    } else if (filter.get0() != null) {
                        outputFilter = new Tuple2<>(filter.get0(), false);
                    } else if (newFilter.get0() != null) {
                        outputFilter = new Tuple2<>(newFilter.get0(), false);
                    } else {
                        outputFilter = new Tuple2<>(null, false);
                    }
                }
            }
        } else {
            outputFilter = newFilter;
        }
        return outputFilter;
    }

    private static Tuple2<FilterPredicate, Boolean> orFilter(final Tuple2<FilterPredicate, Boolean> filter, final Tuple2<FilterPredicate, Boolean> newFilter) {
        final Tuple2<FilterPredicate, Boolean> outputFilter;
        if (filter != null) {
            if (filter.get1()) {
                if (filter.get0() != null && newFilter.get0() != null) {
                    outputFilter = new Tuple2<>(or(filter.get0(), newFilter.get0()), true);
                } else if (filter.get0() != null) {
                    outputFilter = new Tuple2<>(filter.get0(), true);
                } else if (newFilter.get0() != null) {
                    outputFilter = new Tuple2<>(newFilter.get0(), true);
                } else {
                    outputFilter = new Tuple2<>(null, true);
                }
            } else {
                if (newFilter.get1()) {
                    if (filter.get0() != null && newFilter.get0() != null) {
                        outputFilter = new Tuple2<>(or(filter.get0(), newFilter.get0()), true);
                    } else if (filter.get0() != null) {
                        outputFilter = new Tuple2<>(filter.get0(), true);
                    } else if (newFilter.get0() != null) {
                        outputFilter = new Tuple2<>(newFilter.get0(), true);
                    } else {
                        outputFilter = new Tuple2<>(null, true);
                    }
                } else {
                    if (filter.get0() != null && newFilter.get0() != null) {
                        outputFilter = new Tuple2<>(or(filter.get0(), newFilter.get0()), true);
                    } else if (filter.get0() != null) {
                        outputFilter = new Tuple2<>(filter.get0(), true);
                    } else if (newFilter.get0() != null) {
                        outputFilter = new Tuple2<>(newFilter.get0(), true);
                    } else {
                        outputFilter = new Tuple2<>(null, true);
                    }
                }
            }
        } else {
            outputFilter = newFilter;
        }
        return outputFilter;
    }

    private static FilterPredicate createSeedFilter(final Object[] seed, final String colName,
                                             final Map<String, String[]> colToPaths) throws OperationException {
        String[] columns = colToPaths.get(colName);
        if (columns == null) {
            columns = new String[1];
            columns[0] = colName;
        }
        FilterPredicate seedFilter = null;
        for (int i = 0; i < columns.length; i++) {
            final Object currentSeed = seed[i];
            final String col = columns[i];
            final String type = currentSeed.getClass().getCanonicalName();
            FilterPredicate filter;
            if (currentSeed instanceof byte[]) {
                filter = eq(binaryColumn(col), Binary.fromReusedByteArray((byte[]) currentSeed));
            } else if (currentSeed instanceof Long) {
                filter = eq(longColumn(col), (Long) currentSeed);
            } else if (currentSeed instanceof Integer) {
                filter = eq(intColumn(col), (Integer) currentSeed);
            } else if (currentSeed instanceof Double) {
                filter = eq(doubleColumn(col), (Double) currentSeed);
            } else if (currentSeed instanceof Float) {
                filter = eq(floatColumn(col), (Float) currentSeed);
            } else if (currentSeed instanceof Boolean) {
                filter = eq(booleanColumn(col), (Boolean) currentSeed);
            } else if (currentSeed instanceof String) {
                filter = eq(binaryColumn(col), Binary.fromString((String) currentSeed));
            } else {
                throw new OperationException("Vertex type " + type + " is not supported yet");
            }
            if (seedFilter != null) {
                filter = and(seedFilter, filter);
            }
            seedFilter = filter;
            LOGGER.debug("SeedFilter: {}", seedFilter);
        }
        return seedFilter;
    }

    protected static Tuple2<FilterPredicate, Boolean> buildGroupFilter(final View view,
                                                                       final SchemaUtils schemaUtils,
                                                                       final String group,
                                                                       final DirectedType directedType,
                                                                       final boolean isEntity) throws SerialisationException {
        Tuple2<FilterPredicate, Boolean> groupFilter = null;
        final ViewElementDefinition groupView = view.getElement(group);
        if (groupView != null) {
            List<TupleAdaptedPredicate<String, ?>> preAggFilterFunctions = groupView.getPreAggregationFilterFunctions();
            if (preAggFilterFunctions != null) {
                for (final TupleAdaptedPredicate<String, ?> filterFunctionContext : preAggFilterFunctions) {
                    final Tuple2<FilterPredicate, Boolean> filter = buildFilter(filterFunctionContext.getPredicate(), filterFunctionContext.getSelection(), schemaUtils, group);
                    groupFilter = andFilter(groupFilter, filter);
                }
            }
        }
        if (!isEntity) {
            final FilterPredicate directedFilter;
            if (directedType == DirectedType.DIRECTED) {
                directedFilter = eq(booleanColumn(ParquetStoreConstants.DIRECTED), true);
            } else if (directedType == DirectedType.UNDIRECTED) {
                directedFilter = eq(booleanColumn(ParquetStoreConstants.DIRECTED), false);
            } else {
                directedFilter = null;
            }
            if (groupFilter != null || directedFilter != null) {
                groupFilter = andFilter(groupFilter, new Tuple2<>(directedFilter, false));
            }
        }
        return groupFilter;
    }

    private static Tuple2<FilterPredicate, Boolean> buildFilter(final Predicate filterFunction, final String[] selection, final SchemaUtils schemaUtils, final String group) throws SerialisationException {
        if (filterFunction instanceof And) {
            return addAndFilter(((And) filterFunction).getComponents(), selection, schemaUtils, group);
        } else if (filterFunction instanceof Or) {
            return addOrFilter(((Or) filterFunction).getComponents(), selection, schemaUtils, group);
        } else if (filterFunction instanceof Not) {
            Tuple2<FilterPredicate, Boolean> filter = buildFilter(((Not) filterFunction).getPredicate(), selection, schemaUtils, group);
            if (filter.get0() != null) {
                return new Tuple2<>(not(filter.get0()), filter.get1());
            } else {
                return new Tuple2<>(null, true);
            }
        } else {
            final FilterPredicate newFilter = addPrimitiveFilter(filterFunction, selection[0], schemaUtils, group);
            return new Tuple2<>(newFilter, newFilter == null);
        }
    }

    private static Tuple2<FilterPredicate, Boolean> addOrFilter(final List<Predicate> predicateList,
                                                                final String[] selection,
                                                                final SchemaUtils schemaUtils,
                                                                final String group) throws SerialisationException {

        Tuple2<FilterPredicate, Boolean> filter = null;
        for (final Predicate functionContext : predicateList) {
            final Predicate filterFunction;
            final String[] newSelections;
            if (functionContext instanceof TupleAdaptedPredicate) {
                filterFunction = ((TupleAdaptedPredicate) functionContext).getPredicate();
                // Build new selections
                final Integer[] ints = (Integer[]) ((TupleAdaptedPredicate) functionContext).getSelection();
                newSelections = new String[ints.length];
                for (int x = 0; x < ints.length; x++) {
                    newSelections[x] = selection[ints[x]];
                }
            } else {
                filterFunction = functionContext;
                newSelections = selection;
            }
            filter = orFilter(filter, buildFilter(filterFunction, newSelections, schemaUtils, group));
        }
        return filter;
    }

    private static Tuple2<FilterPredicate, Boolean> addAndFilter(final List<Predicate> predicateList,
                                                                 final String[] selection,
                                                                 final SchemaUtils schemaUtils,
                                                                 final String group) throws SerialisationException {
        Tuple2<FilterPredicate, Boolean> filter = null;
        for (final Predicate functionContext : predicateList) {
            final Predicate filterFunction;
            final String[] newSelections;
            if (functionContext instanceof TupleAdaptedPredicate) {
                filterFunction = ((TupleAdaptedPredicate) functionContext).getPredicate();
                // Build new selections
                final Integer[] ints = (Integer[]) ((TupleAdaptedPredicate) functionContext).getSelection();
                newSelections = new String[ints.length];
                for (int x = 0; x < ints.length; x++) {
                    newSelections[x] = selection[ints[x]];
                }
            } else {
                filterFunction = functionContext;
                newSelections = selection;
            }
            filter = andFilter(filter, buildFilter(filterFunction, newSelections, schemaUtils, group));
        }
        return filter;
    }

    private static FilterPredicate addPrimitiveFilter(final Predicate filterFunction,
                                                      final String selection,
                                                      final SchemaUtils schemaUtils,
                                                      final String group) throws SerialisationException {
        // All supported filters will be in the if else statement below
        if (filterFunction instanceof IsEqual) {
            return addIsEqualFilter(selection, ((IsEqual) filterFunction).getControlValue(), schemaUtils, group);
        } else if (filterFunction instanceof IsLessThan) {
            if (((IsLessThan) filterFunction).getOrEqualTo()) {
                return addIsLessThanOrEqualToFilter(selection, ((IsLessThan) filterFunction).getControlValue(), schemaUtils, group);
            } else {
                return addIsLessThanFilter(selection, ((IsLessThan) filterFunction).getControlValue(), schemaUtils, group);
            }
        } else if (filterFunction instanceof IsMoreThan) {
            if (((IsMoreThan) filterFunction).getOrEqualTo()) {
                return addIsMoreThanOrEqualToFilter(selection, ((IsMoreThan) filterFunction).getControlValue(), schemaUtils, group);
            } else {
                return addIsMoreThanFilter(selection, ((IsMoreThan) filterFunction).getControlValue(), schemaUtils, group);
            }
        } else if (filterFunction instanceof IsTrue) {
            return eq(booleanColumn(selection), Boolean.TRUE);
        } else if (filterFunction instanceof IsFalse) {
            return eq(booleanColumn(selection), Boolean.FALSE);
        } else {
            LOGGER.warn(filterFunction.getClass().getCanonicalName() +
                    " is not a natively supported filter by the Parquet store, therefore execution will take longer to perform this filter.");
            return null;
        }
    }

    private static FilterPredicate addIsEqualFilter(final String colName,
                                                    final Object controlValue,
                                                    final SchemaUtils schemaUtils,
                                                    final String group) throws SerialisationException {
        String[] paths = schemaUtils.getPaths(group, colName);
        Object[] parquetObjects = schemaUtils.getConverter(group).gafferObjectToParquetObjects(colName, controlValue);
        if (paths == null) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            final String type = parquetObjects[i].getClass().getCanonicalName();
            FilterPredicate tempFilter;
            if ("java.lang.String".equals(type)) {
                tempFilter = eq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if ("java.lang.Boolean".equals(type)) {
                tempFilter = eq(booleanColumn(path), (Boolean) parquetObjects[i]);
            } else if ("java.lang.Double".equals(type)) {
                tempFilter = eq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if ("java.lang.Float".equals(type)) {
                tempFilter = eq(floatColumn(path), (Float) parquetObjects[i]);
            } else if ("java.lang.Integer".equals(type)) {
                tempFilter = eq(intColumn(path), (Integer) parquetObjects[i]);
            } else if ("java.lang.Long".equals(type)) {
                tempFilter = eq(longColumn(path), (Long) parquetObjects[i]);
            } else if ("java.util.Date".equals(type)) {
                tempFilter = eq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if ("java.sql.Date".equals(type)) {
                tempFilter = eq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if ("java.lang.Short".equals(type)) {
                tempFilter = eq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if ("byte[]".equals(type)) {
                tempFilter = eq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsEqual filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (filter == null) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        //TODO add logic to make use of index when the colName is an element identifier
        return filter;
    }

    private static FilterPredicate addIsLessThanOrEqualToFilter(final String colName,
                                                                final Comparable controlValue,
                                                                final SchemaUtils schemaUtils,
                                                                final String group) throws SerialisationException {
        String[] paths = schemaUtils.getPaths(group, colName);
        Object[] parquetObjects = schemaUtils.getConverter(group).gafferObjectToParquetObjects(colName, controlValue);
        if (paths == null) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            final String type = parquetObjects[i].getClass().getCanonicalName();
            FilterPredicate tempFilter;
            if ("java.lang.String".equals(type)) {
                tempFilter = ltEq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if ("java.lang.Double".equals(type)) {
                tempFilter = ltEq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if ("java.lang.Float".equals(type)) {
                tempFilter = ltEq(floatColumn(path), (Float) parquetObjects[i]);
            } else if ("java.lang.Integer".equals(type)) {
                tempFilter = ltEq(intColumn(path), (Integer) parquetObjects[i]);
            } else if ("java.lang.Long".equals(type)) {
                tempFilter = ltEq(longColumn(path), (Long) parquetObjects[i]);
            } else if ("java.util.Date".equals(type)) {
                tempFilter = ltEq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if ("java.sql.Date".equals(type)) {
                tempFilter = ltEq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if ("java.lang.Short".equals(type)) {
                tempFilter = ltEq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if ("byte[]".equals(type)) {
                tempFilter = ltEq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsLessThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (filter == null) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        //TODO add logic to make use of index when the colName is an element identifier
        return filter;
    }

    private static FilterPredicate addIsLessThanFilter(final String colName,
                                                       final Comparable controlValue,
                                                       final SchemaUtils schemaUtils,
                                                       final String group) throws SerialisationException {
        String[] paths = schemaUtils.getPaths(group, colName);
        Object[] parquetObjects = schemaUtils.getConverter(group).gafferObjectToParquetObjects(colName, controlValue);
        if (paths == null) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            final String type = parquetObjects[i].getClass().getCanonicalName();
            FilterPredicate tempFilter;
            if ("java.lang.String".equals(type)) {
                tempFilter = lt(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if ("java.lang.Double".equals(type)) {
                tempFilter = lt(doubleColumn(path), (Double) parquetObjects[i]);
            } else if ("java.lang.Float".equals(type)) {
                tempFilter = lt(floatColumn(path), (Float) parquetObjects[i]);
            } else if ("java.lang.Integer".equals(type)) {
                tempFilter = lt(intColumn(path), (Integer) parquetObjects[i]);
            } else if ("java.lang.Long".equals(type)) {
                tempFilter = lt(longColumn(path), (Long) parquetObjects[i]);
            } else if ("java.util.Date".equals(type)) {
                tempFilter = lt(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if ("java.sql.Date".equals(type)) {
                tempFilter = lt(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if ("java.lang.Short".equals(type)) {
                tempFilter = lt(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if ("byte[]".equals(type)) {
                tempFilter = lt(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName() + " is not a natively supported type for the IsLessThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (filter == null) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        //TODO add logic to make use of index when the colName is an element identifier
        return filter;
    }

    private static FilterPredicate addIsMoreThanOrEqualToFilter(final String colName,
                                                                final Comparable controlValue,
                                                                final SchemaUtils schemaUtils,
                                                                final String group) throws SerialisationException {
        String[] paths = schemaUtils.getPaths(group, colName);
        Object[] parquetObjects = schemaUtils.getConverter(group).gafferObjectToParquetObjects(colName, controlValue);
        if (paths == null) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            final String type = parquetObjects[i].getClass().getCanonicalName();
            FilterPredicate tempFilter;
            if ("java.lang.String".equals(type)) {
                tempFilter = gtEq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if ("java.lang.Double".equals(type)) {
                tempFilter = gtEq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if ("java.lang.Float".equals(type)) {
                tempFilter = gtEq(floatColumn(path), (Float) parquetObjects[i]);
            } else if ("java.lang.Integer".equals(type)) {
                tempFilter = gtEq(intColumn(path), (Integer) parquetObjects[i]);
            } else if ("java.lang.Long".equals(type)) {
                tempFilter = gtEq(longColumn(path), (Long) parquetObjects[i]);
            } else if ("java.util.Date".equals(type)) {
                tempFilter = gtEq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if ("java.sql.Date".equals(type)) {
                tempFilter = gtEq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if ("java.lang.Short".equals(type)) {
                tempFilter = gtEq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if ("byte[]".equals(type)) {
                tempFilter = gtEq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName() + " is not a natively supported type for the IsMoreThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (filter == null) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        //TODO add logic to make use of index when the colName is an element identifier
        return filter;
    }

    private static FilterPredicate addIsMoreThanFilter(final String colName,
                                                       final Comparable controlValue,
                                                       final SchemaUtils schemaUtils,
                                                       final String group) throws SerialisationException {
        String[] paths = schemaUtils.getPaths(group, colName);
        Object[] parquetObjects = schemaUtils.getConverter(group).gafferObjectToParquetObjects(colName, controlValue);
        if (paths == null) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            final String type = parquetObjects[i].getClass().getCanonicalName();
            FilterPredicate tempFilter;
            if ("java.lang.String".equals(type)) {
                tempFilter = gt(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if ("java.lang.Double".equals(type)) {
                tempFilter = gt(doubleColumn(path), (Double) parquetObjects[i]);
            } else if ("java.lang.Float".equals(type)) {
                tempFilter = gt(floatColumn(path), (Float) parquetObjects[i]);
            } else if ("java.lang.Integer".equals(type)) {
                tempFilter = gt(intColumn(path), (Integer) parquetObjects[i]);
            } else if ("java.lang.Long".equals(type)) {
                tempFilter = gt(longColumn(path), (Long) parquetObjects[i]);
            } else if ("java.util.Date".equals(type)) {
                tempFilter = gt(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if ("java.sql.Date".equals(type)) {
                tempFilter = gt(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if ("java.lang.Short".equals(type)) {
                tempFilter = gt(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if ("byte[]".equals(type)) {
                tempFilter = gt(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsLessThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (filter == null) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        //TODO add logic to make use of index when the colName is an element identifier
        return filter;
    }
}
