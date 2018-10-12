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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.query.ParquetQuery;
import uk.gov.gchq.gaffer.parquetstore.query.QueryGenerator;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Converts the inputs for get element operations to a mapping of files to Parquet filters which is
 * then looped over to retrieve the filtered Elements.
 */
public class ParquetElementRetriever implements CloseableIterable<Element> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetElementRetriever.class);

    private final ParquetStore store;
    private final Operation operation;
    private final User user;

    public ParquetElementRetriever(final ParquetStore store, final Operation operation, final User user) {
        if (!(operation instanceof GetElements) && !(operation instanceof GetAllElements)) {
            throw new IllegalArgumentException("Only operations of type GetElements and GetAllElements are supported");
        }
        this.store = store;
        this.operation = operation;
        this.user = user;
    }

    @Override
    public void close() {
    }

    @Override
    public CloseableIterator<Element> iterator() {
        try {
            return new ParquetIterator(store, operation, user);
        } catch (final OperationException e) {
            throw new RuntimeException("Exception in iterator()", e);
        }
    }

    protected static class ParquetIterator implements CloseableIterator<Element> {
        private ConcurrentLinkedQueue<Element> queue;
        private List<Future<OperationException>> runningTasks;
        private ExecutorService executorServicePool;

        protected ParquetIterator(final ParquetStore store, final Operation operation, final User user) throws OperationException {
            final QueryGenerator queryGenerator = new QueryGenerator(store);
            final View view;
            if (operation instanceof GetAllElements) {
                view = ((GetAllElements) operation).getView();
            } else {
                view = ((GetElements) operation).getView();
            }
            try {
                final ParquetQuery parquetQuery = queryGenerator.getParquetQuery(operation);
                LOGGER.debug("Created ParquetQuery {}", parquetQuery);
                if (!parquetQuery.isEmpty()) {
                    queue = new ConcurrentLinkedQueue<>();
                    executorServicePool = Executors.newFixedThreadPool(store.getProperties().getThreadsAvailable());
                    final List<RetrieveElementsFromFile> tasks = new ArrayList<>();
                    tasks.addAll(parquetQuery.getAllParquetFileQueries()
                            .stream()
                            .map(entry -> new RetrieveElementsFromFile(entry.getFile(), entry.getFilter(),
                                    store.getSchema(), queue, !entry.isFullyApplied(),
                                    store.getProperties().getSkipValidation(), view, user))
                            .collect(Collectors.toList()));
                    LOGGER.info("Invoking {} RetrieveElementsFromFile tasks", tasks.size());
                    runningTasks = executorServicePool.invokeAll(tasks);
                } else {
                    LOGGER.warn("No paths found - there will be no results from this query");
                }
            } catch (final IOException | OperationException e) {
                LOGGER.error("Exception while creating the mapping of file paths to Parquet filters: {}", e.getMessage());
                throw new OperationException("Exception creating ParquetIterator", e);
            } catch (final InterruptedException e) {
                LOGGER.error("InterruptedException in ParquetIterator {}", e.getMessage());
                throw new OperationException("InterruptedException in ParquetIterator", e);
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
