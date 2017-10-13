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

package uk.gov.gchq.gaffer.parquetstore.operation.getelements.impl;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetFilterUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Converts the inputs for get element operations and converts them to a mapping of files to Parquet filters which is
 * then looped over to retrieve the filtered Elements.
 */
public class ParquetElementRetriever implements CloseableIterable<Element> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetElementRetriever.class);

    private final View view;
    private final DirectedType directedType;
    private final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType;
    private final SeedMatching.SeedMatchingType seedMatchingType;
    private final Iterable<? extends ElementId> seeds;
    private final ParquetFilterUtils parquetFilterUtils;
    private GraphIndex graphIndex;
    private final ParquetStoreProperties properties;
    private final Schema gafferSchema;
    private final User user;

    public ParquetElementRetriever(final View view,
                                   final ParquetStore store,
                                   final DirectedType directedType,
                                   final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
                                   final SeedMatching.SeedMatchingType seedMatchingType,
                                   final Iterable<? extends ElementId> seeds,
                                   final User user) throws OperationException, StoreException {
        this.view = view;
        this.gafferSchema = store.getSchema();
        this.directedType = directedType;
        this.includeIncomingOutgoingType = includeIncomingOutgoingType;
        this.seedMatchingType = seedMatchingType;
        this.seeds = seeds;
        this.graphIndex = store.getGraphIndex();
        if (null == graphIndex) {
            throw new OperationException("Can not perform a Get operation when there is no index set, which is " +
                    "indicative of there being no data or the data ingest failed.");
        }
        this.parquetFilterUtils = new ParquetFilterUtils(store);
        this.properties = store.getProperties();
        this.user = user;
    }

    @Override
    public void close() {
    }

    @Override
    public CloseableIterator<Element> iterator() {
        return new ParquetIterator(view, directedType, includeIncomingOutgoingType,
                seedMatchingType, seeds, parquetFilterUtils, graphIndex, properties, gafferSchema, user);
    }

    protected static class ParquetIterator implements CloseableIterator<Element> {
        private ConcurrentLinkedQueue<Element> queue;
        private List<Future<OperationException>> runningTasks;
        private ExecutorService executorServicePool;

        protected ParquetIterator(final View view,
                                  final DirectedType directedType,
                                  final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
                                  final SeedMatching.SeedMatchingType seedMatchingType,
                                  final Iterable<? extends ElementId> seeds,
                                  final ParquetFilterUtils parquetFilterUtils,
                                  final GraphIndex graphIndex,
                                  final ParquetStoreProperties properties,
                                  final Schema gafferSchema,
                                  final User user) {
            try {
                parquetFilterUtils.buildPathToFilterMap(view, directedType, includeIncomingOutgoingType, seedMatchingType, seeds, graphIndex);
                final Map<Path, FilterPredicate> pathToFilterMap = parquetFilterUtils.getPathToFilterMap();
                LOGGER.debug("pathToFilterMap: {}", pathToFilterMap);
                if (!pathToFilterMap.isEmpty()) {
                    queue = new ConcurrentLinkedQueue<>();
                    executorServicePool = Executors.newFixedThreadPool(properties.getThreadsAvailable());
                    final List<RetrieveElementsFromFile> tasks = new ArrayList<>(pathToFilterMap.size());
                    tasks.addAll(pathToFilterMap.entrySet().stream().map(entry -> new RetrieveElementsFromFile(entry.getKey(), entry.getValue(), gafferSchema, queue, parquetFilterUtils.needsValidatorsAndFiltersApplying(), properties.getSkipValidation(), view, user)).collect(Collectors.toList()));
                    runningTasks = executorServicePool.invokeAll(tasks);
                } else {
                    LOGGER.debug("There are no results for this query");
                }
            } catch (final OperationException | SerialisationException e) {
                LOGGER.error("Exception while creating the mapping of file paths to Parquet filters: {}", e.getMessage());
            } catch (final InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }


        @Override
        public boolean hasNext() {
            if (null != queue) {
                if (queue.isEmpty()) {
                    boolean finishedAllTasks = runningTasks.isEmpty();
                    while (!finishedAllTasks && queue.isEmpty()) {
                        try {
                            finishedAllTasks = hasFinishedAllTasks();
                            if (!finishedAllTasks) {
                                wait(100L);
                            }
                        } catch (final Exception e) {
                            LOGGER.error(e.getMessage(), e);
                            finishedAllTasks = true;
                        }
                    }
                    return !queue.isEmpty();
                } else {
                    return true;
                }
            } else {
                return false;
            }
        }

        private boolean hasFinishedAllTasks() throws ExecutionException, InterruptedException, OperationException {
            final List<Future<OperationException>> completedTasks = new ArrayList<>();
            for (final Future<OperationException> task : runningTasks) {
                if (task.isDone()) {
                    final OperationException taskResult = task.get();
                    if (null != taskResult) {
                        throw taskResult;
                    } else {
                        completedTasks.add(task);
                    }
                }
            }
            runningTasks.removeAll(completedTasks);
            return runningTasks.isEmpty();
        }


        @Override
        public Element next() throws NoSuchElementException {
            Element e;
            while (hasNext()) {
                e = queue.poll();
                if (null != e) {
                    return e;
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public void close() {
            if (null != executorServicePool) {
                executorServicePool.shutdown();
                executorServicePool = null;
            }
            queue = null;
            runningTasks = null;
        }
    }
}
