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

package uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.Constants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.StoreException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 */
public class GenerateIndices {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateIndices.class);

    public GenerateIndices(final ParquetStore store) throws OperationException, SerialisationException, StoreException {
        final ArrayList<Callable<OperationException>> tasks = new ArrayList<>();
        final SchemaUtils schemaUtils = store.getSchemaUtils();
        for (final String group : store.getSchemaUtils().getEdgeGroups()) {
            LOGGER.info("Generating the index for group " + group);
            final HashMap<String, String[]> columnToPaths = schemaUtils.getColumnToPaths(group);
            tasks.add(new GenerateIndexForGroup(schemaUtils.getGroupDirectory(group, Constants.SOURCE, store.getProperties().getTempFilesDir() + "/sorted"), columnToPaths.get(Constants.SOURCE)));
            tasks.add(new GenerateIndexForGroup(schemaUtils.getGroupDirectory(group, Constants.DESTINATION, store.getProperties().getTempFilesDir() + "/sorted"), columnToPaths.get(Constants.DESTINATION)));
        }
        for (final String group : store.getSchemaUtils().getEntityGroups()) {
            LOGGER.info("Generating the index for group " + group);
            tasks.add(new GenerateIndexForGroup(schemaUtils.getGroupDirectory(group, Constants.VERTEX, store.getProperties().getTempFilesDir() + "/sorted"), schemaUtils.getPaths(group, Constants.VERTEX)));
        }
        final ExecutorService pool = Executors.newFixedThreadPool(store.getProperties().getThreadsAvailable());
        try {
            final List<Future<OperationException>> results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                OperationException result = results.get(i).get();
                if (result != null) {
                    throw result;
                }
            }
        } catch (InterruptedException e) {
            throw new OperationException("AggregateAndSortData was interrupted", e);
        } catch (ExecutionException e) {
            throw new OperationException("AggregateAndSortData had an execution exception thrown", e);
        }
    }
}
