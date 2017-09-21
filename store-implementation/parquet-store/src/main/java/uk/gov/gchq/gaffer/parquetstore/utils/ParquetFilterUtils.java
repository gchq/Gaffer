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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
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
import uk.gov.gchq.gaffer.parquetstore.index.MinValuesWithPath;
import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsFalse;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

/**
 * This class converts all the inputs to a get elements operation and turns them into a file/directory path to Parquet
 * filter mapping which can be iterated through to retrieve the filtered elements.
 */
public final class ParquetFilterUtils {
    private static final SeedComparator COMPARATOR = new SeedComparator();
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFilterUtils.class);

    private final String rootDir;
    private final SchemaUtils schemaUtils;
    private String dataDir;
    private View view;
    private DirectedType directedType;
    private SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType;
    private SeedMatching.SeedMatchingType seedMatchingType;
    private Iterable<? extends ElementId> seeds;
    private GraphIndex graphIndex;
    private final Map<Path, FilterPredicate> pathToFilterMap;
    private boolean requiresValidation;

    /**
     * The constructor which sets up this object so it is ready to convert the inputs for get elements operations and
     * generate a mapping of which paths to apply which Parquet filters.
     *
     * @param store is the {@link ParquetStore} in use
     */
    public ParquetFilterUtils(final ParquetStore store) {
        this.rootDir = store.getDataDir();
        this.schemaUtils = store.getSchemaUtils();
        this.pathToFilterMap = new HashMap<>();
        this.requiresValidation = false;
    }

    public Map<Path, FilterPredicate> getPathToFilterMap() {
        return pathToFilterMap;
    }

    public boolean requiresValidation() {
        return requiresValidation;
    }

    /**
     * Takes in the various inputs to get elements operations and generates a mapping of which paths to apply which
     * Parquet filters.
     *
     * @param view                          the Gaffer {@link View} to be applied
     * @param directedType                  the {@link DirectedType} to be applied
     * @param includeIncomingOutgoingType   the {@link uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType} to be applied
     * @param seedMatchingType              the {@link uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType} to be applied
     * @param seeds                         the seeds to be applied
     * @param graphIndex                    the {@link GraphIndex} to use
     * @throws SerialisationException   If any of the Gaffer objects are unable to be serialised to Parquet objects
     * @throws OperationException   If a serialiser is used which serialises objects to a type not supported
     */
    public void buildPathToFilterMap(
            final View view,
            final DirectedType directedType,
            final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
            final SeedMatching.SeedMatchingType seedMatchingType,
            final Iterable<? extends ElementId> seeds,
            final GraphIndex graphIndex)
            throws SerialisationException, OperationException {
        // Setup global variables to build a new PathToFilter mapping
        this.dataDir = rootDir + "/" + graphIndex.getSnapshotTimestamp();
        this.view = view;
        this.directedType = directedType;
        this.includeIncomingOutgoingType = includeIncomingOutgoingType;
        this.seedMatchingType = seedMatchingType;
        this.seeds = seeds;
        this.graphIndex = graphIndex;
        final Set<String> edgeGroups;
        final Set<String> entityGroups;
        if (null != view) {
            edgeGroups = new HashSet<>();
            entityGroups = new HashSet<>();
            final Set<String> indexedGroups = graphIndex.groupsIndexed();
            for (final String group : view.getEdgeGroups()) {
                if (indexedGroups.contains(group)) {
                    edgeGroups.add(group);
                }
            }
            for (final String group : view.getEntityGroups()) {
                if (indexedGroups.contains(group)) {
                    entityGroups.add(group);
                }
            }
        } else {
            edgeGroups = schemaUtils.getEdgeGroups();
            entityGroups = schemaUtils.getEntityGroups();
        }
        this.pathToFilterMap.clear();
        this.requiresValidation = false;

        if (null == seeds && (null == view || schemaUtils.getEmptyView().equals(view))) {
            // get all elements
            for (final String group : entityGroups) {
                pathToFilterMap.put(new Path(ParquetStore.getGroupDirectory(group, ParquetStoreConstants.VERTEX, dataDir)), null);
            }
            for (final String group : edgeGroups) {
                pathToFilterMap.put(new Path(ParquetStore.getGroupDirectory(group, ParquetStoreConstants.SOURCE, dataDir)), null);
            }
        } else {
            // Build up the path to filters based on the seeds and then apply the view (group) filters
            for (final String edgeGroup : edgeGroups) {
                buildSeedFilter(edgeGroup, false);
                applyGroupFilter(edgeGroup, false);
            }
            for (final String entityGroup : entityGroups) {
                buildSeedFilter(entityGroup, true);
                applyGroupFilter(entityGroup, true);
            }
        }
    }

    /**
     * For the given group, this method adds to the pathToFilterMap all the relevant mappings based on the seeds
     *
     * @param group         Gaffer group to apply seed filters to
     * @param isEntityGroup Whether the provided group is an Entity group
     * @throws SerialisationException   If any of the Gaffer objects are unable to be serialised to Parquet objects
     * @throws OperationException   If a serialiser is used which serialises objects to a type not supported
     */
    private void buildSeedFilter(final String group, final boolean isEntityGroup) throws SerialisationException, OperationException {
        if (null != seeds) {
            final Iterator<? extends ElementId> seedIter = seeds.iterator();
            if (seedIter.hasNext()) {
                final String identifier;
                if (isEntityGroup) {
                    identifier = ParquetStoreConstants.VERTEX;
                } else {
                    identifier = ParquetStoreConstants.SOURCE;
                }
                final Pair<List<Object[]>, Map<Object[], Pair<Object[], DirectedType>>> prepSeedsResult = prepSeeds(identifier, group, isEntityGroup);
                final List<Object[]> sortedSeeds = prepSeedsResult.getFirst();
                final Map<Object[], Pair<Object[], DirectedType>> seed2Parts = prepSeedsResult.getSecond();
                // Build graph path to filter
                buildSeedFilterForIndex(sortedSeeds, identifier, group, isEntityGroup, seed2Parts);
                if (!isEntityGroup) {
                    // Build reverseEdges path to filter
                    buildSeedFilterForIndex(sortedSeeds, ParquetStoreConstants.DESTINATION, group, false, seed2Parts);
                }
            }
        }
    }

    /**
     * Returns a {@link Pair} in which the first entry is a sorted {@link Set} of the seeds converted to the form in
     * which they appear in the Parquet files, and the second entry is a {@link Map} from the seeds to a {@link Pair}
     * which is {@code null} if the seed is an {@link EntitySeed} and consists of the destination vertex and
     * directed type if the seed is an {@link EdgeSeed}.
     *
     * @param identifier the column that the seed relates to
     * @param group the group that is currently being queried
     * @param isEntityGroup is the group provided an entity group
     * @return a {@link Pair} in which the first entry is a sorted {@link Set} of the seeds converted to the form in
     * which they appear in the Parquet files, and the second entry is a {@link Map} from the seeds to a {@link Pair}
     * which is {@code null} if the seed is an {@link EntitySeed} and consists of the destination vertex and
     * directed type if the seed is an {@link EdgeSeed}.
     * @throws SerialisationException if the conversion from the seed to corresponding Parquet objects fails
     */
    private Pair<List<Object[]>, Map<Object[], Pair<Object[], DirectedType>>> prepSeeds(final String identifier,
                                                                                        final String group,
                                                                                        final boolean isEntityGroup)
            throws SerialisationException {
        final HashMap<Object[], Pair<Object[], DirectedType>> seed2parts = new HashMap<>();
        final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
        final List<Object[]> sortedSeeds = new ArrayList<>();
        for (final ElementId elementSeed : seeds) {
            if (elementSeed instanceof EntitySeed) {
                sortedSeeds.add(converter
                        .gafferObjectToParquetObjects(identifier, ((EntitySeed) elementSeed).getVertex()));
            } else {
                final EdgeSeed edgeSeed = (EdgeSeed) elementSeed;
                final Object[] serialisedSeed = converter.gafferObjectToParquetObjects(identifier, edgeSeed.getSource());
                sortedSeeds.add(serialisedSeed);
                final Object[] serialisedDest = converter.gafferObjectToParquetObjects(identifier, edgeSeed.getDestination());
                if (isEntityGroup && seedMatchingType != SeedMatching.SeedMatchingType.EQUAL) {
                    sortedSeeds.add(serialisedDest);
                }
                seed2parts.put(serialisedSeed, new Pair<>(serialisedDest, edgeSeed.getDirectedType()));
            }
        }
        sortedSeeds.sort(COMPARATOR);
        return new Pair<>(sortedSeeds, seed2parts);
    }

    /**
     * This method adds to the pathToFilterMap all the relevant mappings for the given group and indexedColumn
     * based on the seeds
     *
     * @param sortedSeeds   a {@link Set} of Parquet Objects representing the seeds sorted using the {@link SeedComparator}
     * @param indexedColumn the name of the indexed column to apply the seed filters to
     * @param group         the name of the group to apply the seed filters to
     * @param isEntityGroup whether the provided group is an Entity group
     * @param seed2Parts    a mapping from Parquet Objects representing the source object to the destination Parquet Objects and
     *                      {@link DirectedType} of just the {@link EdgeSeed}'s
     * @throws SerialisationException   If any of the Gaffer objects are unable to be serialised to Parquet objects
     * @throws OperationException   If a serialiser is used which serialises objects to a type not supported
     */
    private void buildSeedFilterForIndex(final List<Object[]> sortedSeeds, final String indexedColumn, final String group,
                                         final boolean isEntityGroup, final Map<Object[], Pair<Object[], DirectedType>> seed2Parts)
            throws OperationException, SerialisationException {
        final Map<Object[], Set<Path>> seedsToPaths = getIndexedPathsForSeeds(sortedSeeds, indexedColumn, group);
        for (final Map.Entry<Object[], Set<Path>> entry : seedsToPaths.entrySet()) {
            final Set<Path> paths = entry.getValue();
            final Object[] currentSeed = entry.getKey();
            for (final Path path : paths) {
                addPathToSeedFilter(path, currentSeed, indexedColumn, group, seed2Parts.getOrDefault(currentSeed, null), isEntityGroup);
            }
        }
    }

    /**
     * This method adds the required filters to the pathToFiltersMap for a single path and seed.
     *
     * @param path          The {@link Path} to apply the filter to
     * @param currentSeed   The Parquet objects to build the filter from
     * @param indexedColumn The name of the Gaffer column that was used to identify the relevant paths from the index
     * @param group         The Gaffer group name
     * @param parts         The Destination Parquet objects and the {@link DirectedType} if the seed was an {@link EdgeSeed}
     * @param isEntityGroup Whether the Gaffer group is an Entity group
     * @throws SerialisationException   If any of the Gaffer objects are unable to be serialised to Parquet objects
     * @throws OperationException   If a serialiser is used which serialises objects to a type not supported
     */
    private void addPathToSeedFilter(
            final Path path,
            final Object[] currentSeed,
            final String indexedColumn,
            final String group,
            final Pair<Object[], DirectedType> parts,
            final Boolean isEntityGroup) throws OperationException, SerialisationException {
        FilterPredicate filter = null;
        // Is it an entity group?
        if (isEntityGroup) {
            // Is it an entityId?
            if (null == parts) {
                filter = addIsEqualFilter(ParquetStoreConstants.VERTEX, currentSeed, group, true).getFirst();
            } else {
                // Does the seed type need to match the group type?
                if (seedMatchingType != SeedMatching.SeedMatchingType.EQUAL) {
                    // Vertex = source of edge seed or Vertex = destination of edge seed
                    filter = addIsEqualFilter(ParquetStoreConstants.VERTEX, currentSeed, group, true).getFirst();
//                    filter = orFilter(filter, addIsEqualFilter(ParquetStoreConstants.VERTEX, parts.getFirst(), group, true).getFirst());
                }
            }
        } else {
            // Is it an entityId?
            if (null == parts) {
                if (seedMatchingType != SeedMatching.SeedMatchingType.EQUAL) {
                    // Does it matter if the vertex is the incoming or outgoing edge?
                    if (includeIncomingOutgoingType == SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING) {
                        if (ParquetStoreConstants.DESTINATION.equals(indexedColumn)) {
                            // Destination = vertex of seed and source != vertex of seed as that row of data will be retrieved when querying the source indexed directory
                            filter = addIsEqualFilter(ParquetStoreConstants.DESTINATION, currentSeed, group, true).getFirst();
                            filter = andFilter(filter, addIsNotEqualFilter(ParquetStoreConstants.SOURCE, currentSeed, group, true).getFirst());
                        } else {
                            // Source = vertex of seed and edge is not directed
                            filter = addIsEqualFilter(ParquetStoreConstants.SOURCE, currentSeed, group, true).getFirst();
                            filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DIRECTED, new Object[]{false}, group, true).getFirst());
                        }
                    } else if (includeIncomingOutgoingType == SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING) {
                        if (ParquetStoreConstants.DESTINATION.equals(indexedColumn)) {
                            // Destination = vertex of seed and edge is directed and source != vertex of seed as that row of data will be retrieved when querying the source indexed directory
                            filter = addIsEqualFilter(ParquetStoreConstants.DESTINATION, currentSeed, group, true).getFirst();
                            filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DIRECTED, new Object[]{false}, group, true).getFirst());
                            filter = andFilter(filter, addIsNotEqualFilter(ParquetStoreConstants.SOURCE, currentSeed, group, true).getFirst());
                        } else {
                            // Source = vertex of seed
                            filter = addIsEqualFilter(ParquetStoreConstants.SOURCE, currentSeed, group, true).getFirst();
                        }
                    } else {
                        // indexed column = vertex of seed
                        filter = addIsEqualFilter(indexedColumn, currentSeed, group, true).getFirst();
                        if (ParquetStoreConstants.DESTINATION.equals(indexedColumn)) {
                            // if the indexed column is the destination then check that source != vertex of seed as that row of data will be retrieved when querying the source indexed directory
                            filter = andFilter(filter, addIsNotEqualFilter(ParquetStoreConstants.SOURCE, currentSeed, group, true).getFirst());
                        }
                    }
                }
            } else {
                if (ParquetStoreConstants.SOURCE.equals(indexedColumn)) {
                    // Source = source of edge seed and destination = destination of edge seed
                    filter = addIsEqualFilter(ParquetStoreConstants.SOURCE, currentSeed, group, true).getFirst();
                    filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DESTINATION, parts.getFirst(), group, true).getFirst());
                    final DirectedType directedType = parts.getSecond();
                    // add directed flag filter where applicable
                    if (directedType == DirectedType.DIRECTED) {
                        filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DIRECTED, new Object[]{true}, group, true).getFirst());
                    } else if (directedType == DirectedType.UNDIRECTED) {
                        filter = andFilter(filter, addIsEqualFilter(ParquetStoreConstants.DIRECTED, new Object[]{false}, group, true).getFirst());
                    }
                }
            }
        }
        if (null != filter) {
            pathToFilterMap.put(path, orFilter(filter, pathToFilterMap.get(path)));
        }
    }

    /**
     * This method allows for looking up of which paths to target a filter at if the target Gaffer column has been indexed
     *
     * @param sortedSeeds   A set of sorted Parquet object[] which represent the Gaffer objects used in the seeds or view filters
     * @param indexedColumn The name of the Gaffer column that has been indexed
     * @param group         The name of the Gaffer group to apply the filter to
     * @return a mapping of Parquet Objects representing the input seeds to a set of Paths that may contain that seed
     */
    private Map<Object[], Set<Path>> getIndexedPathsForSeeds(final List<Object[]> sortedSeeds, final String indexedColumn,
                                                             final String group) {
        final Map<Object[], Set<Path>> seedsToPaths = new HashMap<>();
        final Iterator<Object[]> sortedSeedsIter = sortedSeeds.iterator();
        final GroupIndex groupIndex = graphIndex.getGroup(group);
        if (null != groupIndex && groupIndex.columnsIndexed().contains(indexedColumn)) {
            final Iterator<MinValuesWithPath> indexIter = groupIndex.getColumn(indexedColumn).getIterator();
            Object[] currentSeed = null;
            MinValuesWithPath indexEntry;
            if (indexIter.hasNext()) {
                indexEntry = indexIter.next();
                if (indexIter.hasNext()) {
                    MinValuesWithPath nextIndexEntry = indexIter.next();
                    while (sortedSeedsIter.hasNext() && null != nextIndexEntry) {
                        currentSeed = sortedSeedsIter.next();
                        boolean nextSeed = false;
                        while (!nextSeed && null != nextIndexEntry) {
                            final Object min = indexEntry.getMin();
                            final Object max = nextIndexEntry.getMin();
                            final String file = indexEntry.getPath();
                            LOGGER.debug("Current file: {}", file);
                            // If min <= seed && max >= seed
                            final int min2seed = COMPARATOR.compare(min, currentSeed);
                            LOGGER.debug("min2seed comparator: {}", min2seed);
                            final int max2seed = COMPARATOR.compare(max, currentSeed);
                            LOGGER.debug("max2seed comparator: {}", max2seed);
                            if (min2seed < 1 && max2seed >= 0) {
                                final Path fullFilePath = new Path(ParquetStore.getGroupDirectory(group, indexedColumn, dataDir) + "/" + file);
                                final Set<Path> paths = seedsToPaths.getOrDefault(currentSeed, new HashSet<>());
                                paths.add(fullFilePath);
                                seedsToPaths.put(currentSeed, paths);
                                if (max2seed == 0) {
                                    indexEntry = nextIndexEntry;
                                    if (indexIter.hasNext()) {
                                        nextIndexEntry = indexIter.next();
                                    } else {
                                        nextIndexEntry = null;
                                    }
                                } else {
                                    nextSeed = true;
                                }
                            } else if (min2seed > 0) {
                                nextSeed = true;
                            } else {
                                indexEntry = nextIndexEntry;
                                if (indexIter.hasNext()) {
                                    nextIndexEntry = indexIter.next();
                                } else {
                                    nextIndexEntry = null;
                                }
                            }
                        }
                    }
                }
                if (null == currentSeed && sortedSeedsIter.hasNext()) {
                    currentSeed = sortedSeedsIter.next();
                }
                final String file = indexEntry.getPath();
                final Path fullFilePath = new Path(ParquetStore.getGroupDirectory(group, indexedColumn, dataDir) + "/" + file);
                do {
                    final Set<Path> paths = seedsToPaths.getOrDefault(currentSeed, new HashSet<>());
                    paths.add(fullFilePath);
                    seedsToPaths.put(currentSeed, paths);
                    if (sortedSeedsIter.hasNext()) {
                        currentSeed = sortedSeedsIter.next();
                    } else {
                        currentSeed = null;
                    }
                } while (null != currentSeed);
            }
        }
        return seedsToPaths;
    }

    private Set<Path> getIndexedPathsForSeeds(final Object[] seed, final String indexedColumn,
                                                             final String group) {
        if (graphIndex.groupsIndexed().contains(group)) {
            if (graphIndex.getGroup(group).columnsIndexed().contains(indexedColumn)) {
                final List<Object[]> seeds = new ArrayList<>();
                seeds.add(seed);
                final Map<Object[], Set<Path>> seedToPaths = getIndexedPathsForSeeds(seeds, indexedColumn, group);
                return seedToPaths.get(seed);
            }
        }
        return null;
    }

    private Set<Path> getAllPathsForColumn(final String group) {
        final Set<Path> paths = new HashSet<>();
        final String graphColumn;
        if (schemaUtils.getEntityGroups().contains(group)) {
            graphColumn = ParquetStoreConstants.VERTEX;
        } else {
            graphColumn = ParquetStoreConstants.SOURCE;
        }
        if (graphIndex.groupsIndexed().contains(group)) {
            final Iterator<MinValuesWithPath> minValuesWithPathIterator = graphIndex.getGroup(group).getColumn(graphColumn).getIterator();
            while (minValuesWithPathIterator.hasNext()) {
                paths.add(new Path(ParquetStore.getGroupDirectory(group, graphColumn, dataDir) + "/" + minValuesWithPathIterator.next().getPath()));
            }
        }
        return paths;
    }

    private Set<Path> getAllPathsForColumnBeforeOrAfterGivenPaths(final String indexedColumn, final String group, final Set<Path> givenPaths, final boolean selectBefore) {
        final Set<Path> paths = new HashSet<>();

        final Iterator<MinValuesWithPath> minValuesWithPathIterator = graphIndex.getGroup(group).getColumn(indexedColumn).getIterator();
        boolean foundGivenPaths = false;
        while (minValuesWithPathIterator.hasNext()) {
            final Path currentPath = new Path(ParquetStore.getGroupDirectory(group, indexedColumn, dataDir) + "/" + minValuesWithPathIterator.next().getPath());
            if (givenPaths.contains(currentPath)) {
                foundGivenPaths = true;
                paths.add(currentPath);
            } else {
                if (selectBefore && !foundGivenPaths) {
                    paths.add(currentPath);
                } else if (!selectBefore && !foundGivenPaths) {
                    paths.add(currentPath);
                }
            }
        }
        return paths;
    }

    /**
     * For any filters in the pathToFilterMap that apply to the provided Gaffer group then the filter should also require
     * that the group filter is true.
     *
     * @param group     The Gaffer group that the group filter applies to
     * @param isEntity  Whether the group is an Entity group
     * @throws SerialisationException   If any of the Gaffer objects are unable to be serialised to Parquet objects
     */
    private void applyGroupFilter(final String group, final boolean isEntity) throws SerialisationException {
        if (null == seeds || !pathToFilterMap.isEmpty()) {
            boolean appliedGroup = false;
            final Pair<FilterPredicate, Set<Path>> groupFilterAndPaths = buildGroupFilter(group, isEntity);
            if (null != groupFilterAndPaths) {
                final Set<Path> groupPaths = groupFilterAndPaths.getSecond();
                final FilterPredicate groupFilter = groupFilterAndPaths.getFirst();
                for (final Path path : pathToFilterMap.keySet()) {
                    if (path.getParent().getName().endsWith(group)) {
                        final FilterPredicate seedFilter = pathToFilterMap.get(path);
                        pathToFilterMap.put(path, andFilter(seedFilter, groupFilter));
                        appliedGroup = true;
                    }
                }
                if (!appliedGroup && null == seeds) {
                    for (final Path path : groupPaths) {
                        pathToFilterMap.put(path, groupFilter);
                    }
                }
            } else if (null == seeds) {
                for (final Path path : getAllPathsForColumn(group)) {
                    pathToFilterMap.put(path, andFilter(pathToFilterMap.getOrDefault(path, null), null));
                }
            }
        }
    }

    private static FilterPredicate andFilter(final FilterPredicate a, final FilterPredicate b) {
        if (null == a && null == b) {
            return null;
        } else if (null == a) {
            return b;
        } else if (null == b) {
            return a;
        } else {
            return and(a, b);
        }
    }

    private static FilterPredicate orFilter(final FilterPredicate a, final FilterPredicate b) {
        if (null == a && null == b) {
            return null;
        } else if (null == a) {
            return b;
        } else if (null == b) {
            return a;
        } else {
            return or(a, b);
        }
    }

    private static Pair<FilterPredicate, Set<Path>> andFilter(final Pair<FilterPredicate, Set<Path>> a, final Pair<FilterPredicate, Set<Path>> b, final boolean multiSelection) {
        if (null == a && null == b) {
            return null;
        } else if (null == a) {
            return b;
        } else if (null == b) {
            return a;
        } else {
            final FilterPredicate filter;
            if (null == a.getFirst() && null == b.getFirst()) {
                filter = null;
            } else if (null == a.getFirst()) {
                filter = b.getFirst();
            } else if (null == b.getFirst()) {
                filter = a.getFirst();
            } else {
                filter = and(a.getFirst(), b.getFirst());
            }

            if (null == a.getSecond() && null == b.getSecond()) {
                return new Pair<>(filter, null);
            } else if (null == a.getSecond()) {
                return new Pair<>(filter, b.getSecond());
            } else if (null == b.getSecond()) {
                return new Pair<>(filter, a.getSecond());
            } else {
                final Set<Path> aFilePaths = a.getSecond();
                final Set<Path> bFilePaths = b.getSecond();
                if (multiSelection) {
                    if (aFilePaths.size() > bFilePaths.size()) {
                        return new Pair<>(filter, bFilePaths);
                    } else {
                        return new Pair<>(filter, aFilePaths);
                    }
                } else {
                    for (final Path filePath : aFilePaths) {
                        if (!bFilePaths.contains(filePath)) {
                            aFilePaths.remove(filePath);
                        }
                    }
                    return new Pair<>(filter, aFilePaths);
                }
            }
        }
    }

    private Pair<FilterPredicate, Set<Path>> orFilter(final Pair<FilterPredicate, Set<Path>> a, final Pair<FilterPredicate, Set<Path>> b, final boolean multiSelection, final String group) {
        if (null == a && null == b) {
            return null;
        } else if (null == a) {
            return b;
        } else if (null == b) {
            return a;
        } else {
            final FilterPredicate filter;
            if (null == a.getFirst() && null == b.getFirst()) {
                filter = null;
            } else if (null == a.getFirst()) {
                filter = b.getFirst();
            } else if (null == b.getFirst()) {
                filter = a.getFirst();
            } else {
                filter = or(a.getFirst(), b.getFirst());
            }

            if (null == a.getSecond() && null == b.getSecond()) {
                return new Pair<>(filter, null);
            } else if (null == a.getSecond()) {
                return new Pair<>(filter, b.getSecond());
            } else if (null == b.getSecond()) {
                return new Pair<>(filter, a.getSecond());
            } else {
                final Set<Path> aFilePaths = a.getSecond();
                final Set<Path> bFilePaths = b.getSecond();
                if (multiSelection) {
                    return new Pair<>(filter, getAllPathsForColumn(group));
                } else {
                    aFilePaths.addAll(bFilePaths);
                    return new Pair<>(filter, aFilePaths);
                }
            }
        }
    }

    /**
     * Builds up the filter to be applied to the given group's files based on the pre-aggregation view filters.
     * This method handles the group level looping over all filters for that group in the view
     *
     * @param group     a Gaffer group
     * @param isEntity  is the gaffer group an Entity group
     * @return The Parquet filter based on the provided groups pre-aggregation view filters
     * @throws SerialisationException If any of the Gaffer objects are unable to be serialised to Parquet objects
     */
    protected Pair<FilterPredicate, Set<Path>> buildGroupFilter(final String group, final boolean isEntity) throws SerialisationException {
        Pair<FilterPredicate, Set<Path>> groupFilter = null;
        final ViewElementDefinition groupView = view.getElement(group);
        if (null != groupView) {
            List<TupleAdaptedPredicate<String, ?>> preAggFilterFunctions = groupView.getPreAggregationFilterFunctions();
            if (null != preAggFilterFunctions) {
                for (final TupleAdaptedPredicate<String, ?> filterFunctionContext : preAggFilterFunctions) {
                    final Pair<FilterPredicate, Set<Path>> filter = buildFilter(filterFunctionContext.getPredicate(), filterFunctionContext.getSelection(), group);
                    groupFilter = andFilter(groupFilter, filter, filterFunctionContext.getSelection().length > 0);
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
            if (null != groupFilter && null != directedFilter) {
                groupFilter = new Pair<>(and(groupFilter.getFirst(), directedFilter), groupFilter.getSecond());
            } else if (null == groupFilter && (null != directedFilter || requiresValidation)) {
                groupFilter = new Pair<>(directedFilter, getAllPathsForColumn(group));
            }
        }
        return groupFilter;
    }

    /**
     * Builds the Parquet filters for a given Gaffer filter and the selection of which columns to apply the filter to.
     * This method handles the nested And, Or and Not's.
     *
     * @param filterFunction    A Gaffer filter
     * @param selection         An array of column names, either the Gaffer column names of Parquet column names
     * @param group             A Gaffer group name
     * @return The Parquet filter based on the provided Gaffer filters
     * @throws SerialisationException If any of the Gaffer objects are unable to be serialised to Parquet objects
     */
    private Pair<FilterPredicate, Set<Path>> buildFilter(final Predicate filterFunction, final String[] selection, final String group) throws SerialisationException {
        if (filterFunction instanceof And) {
            return addAndFilter(((And) filterFunction).getComponents(), selection, group);
        } else if (filterFunction instanceof Or) {
            return addOrFilter(((Or) filterFunction).getComponents(), selection, group);
        } else if (filterFunction instanceof Not) {
            final Pair<FilterPredicate, Set<Path>> filterResult = buildFilter(((Not) filterFunction).getPredicate(), selection, group);
            if (null == filterResult) {
                return null;
            } else {
                return new Pair<>(not(filterResult.getFirst()), getAllPathsForColumn(group));
            }
        } else {
            final Pair<FilterPredicate, Set<Path>> filterResult = addPrimitiveFilter(filterFunction, selection[0], group);
            if (null == filterResult) {
                requiresValidation = true;
            }
            return filterResult;
        }
    }

    private Pair<FilterPredicate, Set<Path>> addOrFilter(final List<Predicate> predicateList,
                                        final String[] selection,
                                        final String group) throws SerialisationException {
        Pair<FilterPredicate, Set<Path>> filter = null;
        final boolean multiSelection = selection.length > 0;
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
            filter = orFilter(filter, buildFilter(filterFunction, newSelections, group), multiSelection, group);
        }
        return filter;
    }

    private Pair<FilterPredicate, Set<Path>> addAndFilter(final List<Predicate> predicateList,
                                         final String[] selection,
                                         final String group) throws SerialisationException {
        Pair<FilterPredicate, Set<Path>> filter = null;
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
            filter = andFilter(filter, buildFilter(filterFunction, newSelections, group), selection.length > 0);
        }
        return filter;
    }

    private Pair<FilterPredicate, Set<Path>> addPrimitiveFilter(final Predicate filterFunction,
                                               final String selection,
                                               final String group) throws SerialisationException {
        // All supported filters will be in the if else statement below
        if (filterFunction instanceof IsEqual) {
            return addIsEqualFilter(selection, schemaUtils.getConverter(group).gafferObjectToParquetObjects(selection, ((IsEqual) filterFunction).getControlValue()), group, false);
        } else if (filterFunction instanceof IsLessThan) {
            if (((IsLessThan) filterFunction).getOrEqualTo()) {
                return addIsLessThanOrEqualToFilter(selection, schemaUtils.getConverter(group).gafferObjectToParquetObjects(selection, ((IsLessThan) filterFunction).getControlValue()), group);
            } else {
                return addIsLessThanFilter(selection, schemaUtils.getConverter(group).gafferObjectToParquetObjects(selection, ((IsLessThan) filterFunction).getControlValue()), group);
            }
        } else if (filterFunction instanceof IsMoreThan) {
            if (((IsMoreThan) filterFunction).getOrEqualTo()) {
                return addIsMoreThanOrEqualToFilter(selection, schemaUtils.getConverter(group).gafferObjectToParquetObjects(selection, ((IsMoreThan) filterFunction).getControlValue()), group);
            } else {
                return addIsMoreThanFilter(selection, schemaUtils.getConverter(group).gafferObjectToParquetObjects(selection, ((IsMoreThan) filterFunction).getControlValue()), group);
            }
        } else if (filterFunction instanceof IsTrue) {
            return new Pair<>(eq(booleanColumn(selection), Boolean.TRUE), getAllPathsForColumn(group));
        } else if (filterFunction instanceof IsFalse) {
            return new Pair<>(eq(booleanColumn(selection), Boolean.FALSE), getAllPathsForColumn(group));
        } else {
            LOGGER.warn(filterFunction.getClass().getCanonicalName() +
                    " is not a natively supported filter by the Parquet store, therefore execution will take longer to perform this filter.");
            return null;
        }
    }

    private Pair<FilterPredicate, Set<Path>> addIsNotEqualFilter(final String colName,
                                             final Object[] parquetObjects,
                                             final String group, final boolean skipPaths) throws SerialisationException {
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
                tempFilter = notEq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Boolean) {
                tempFilter = notEq(booleanColumn(path), (Boolean) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = notEq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = notEq(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = notEq(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = notEq(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = notEq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = notEq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = notEq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = notEq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
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
        if (skipPaths) {
            return new Pair<>(filter, null);
        } else {
            return new Pair<>(filter, getAllPathsForColumn(group));
        }
    }

    private Pair<FilterPredicate, Set<Path>> addIsEqualFilter(final String colName,
                                             final Object[] parquetObjects,
                                             final String group, final boolean skipPaths) throws SerialisationException {
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
        final Set<Path> filePaths = getIndexedPathsForSeeds(parquetObjects, colName, group);
        if (skipPaths) {
            return new Pair<>(filter, null);
        } else {
            if (null == filePaths) {
                return new Pair<>(filter, getAllPathsForColumn(group));
            } else {
                return new Pair<>(filter, filePaths);
            }
        }
    }

    private Pair<FilterPredicate, Set<Path>> addIsLessThanOrEqualToFilter(final String colName,
                                                         final Object[] parquetObjects,
                                                         final String group) throws SerialisationException {
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
                tempFilter = ltEq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = ltEq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = ltEq(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = ltEq(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = ltEq(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = ltEq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = ltEq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = ltEq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = ltEq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsLessThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        final Set<Path> filePaths = getIndexedPathsForSeeds(parquetObjects, colName, group);
        if (null == filePaths) {
            return new Pair<>(filter, getAllPathsForColumn(group));
        } else {
            final Set<Path> moreThanFilePaths = getAllPathsForColumnBeforeOrAfterGivenPaths(colName, group, filePaths, true);
            return new Pair<>(filter, moreThanFilePaths);
        }
    }

    private Pair<FilterPredicate, Set<Path>> addIsLessThanFilter(final String colName,
                                                final Object[] parquetObjects,
                                                final String group) throws SerialisationException {
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
                tempFilter = lt(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = lt(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = lt(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = lt(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = lt(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = lt(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = lt(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = lt(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = lt(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName() +
                        " is not a natively supported type for the IsLessThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        final Set<Path> filePaths = getIndexedPathsForSeeds(parquetObjects, colName, group);
        if (null == filePaths) {
            return new Pair<>(filter, getAllPathsForColumn(group));
        } else {
            final Set<Path> moreThanFilePaths = getAllPathsForColumnBeforeOrAfterGivenPaths(colName, group, filePaths, true);
            return new Pair<>(filter, moreThanFilePaths);
        }
    }

    private Pair<FilterPredicate, Set<Path>> addIsMoreThanOrEqualToFilter(final String colName,
                                                         final Object[] parquetObjects,
                                                         final String group) throws SerialisationException {
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
                tempFilter = gtEq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = gtEq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = gtEq(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = gtEq(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = gtEq(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = gtEq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = gtEq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = gtEq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = gtEq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName() +
                        " is not a natively supported type for the IsMoreThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        final Set<Path> filePaths = getIndexedPathsForSeeds(parquetObjects, colName, group);
        if (null == filePaths) {
            return new Pair<>(filter, getAllPathsForColumn(group));
        } else {
            final Set<Path> moreThanFilePaths = getAllPathsForColumnBeforeOrAfterGivenPaths(colName, group, filePaths, false);
            return new Pair<>(filter, moreThanFilePaths);
        }
    }

    private Pair<FilterPredicate, Set<Path>> addIsMoreThanFilter(final String colName,
                                                final Object[] parquetObjects,
                                                final String group) throws SerialisationException {
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
                tempFilter = gt(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = gt(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = gt(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = gt(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = gt(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = gt(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = gt(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = gt(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = gt(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsMoreThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        final Set<Path> filePaths = getIndexedPathsForSeeds(parquetObjects, colName, group);
        if (null == filePaths) {
            return new Pair<>(filter, getAllPathsForColumn(group));
        } else {
            final Set<Path> moreThanFilePaths = getAllPathsForColumnBeforeOrAfterGivenPaths(colName, group, filePaths, false);
            return new Pair<>(filter, moreThanFilePaths);
        }
    }
}
