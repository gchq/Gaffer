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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.io.reader.ParquetElementReader;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetFileIterator;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetFilterUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class ParquetElementRetriever implements CloseableIterable<Element> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetElementRetriever.class);

    private final SchemaUtils schemaUtils;
    private final View view;
    private final DirectedType directedType;
    private final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType;
    private final SeedMatching.SeedMatchingType seedMatchingType;
    private final Iterable<? extends ElementId> seeds;
    private final String dataDir;
    private GraphIndex graphIndex;
    private FileSystem fs;

    public ParquetElementRetriever(final View view,
                                   final ParquetStore store,
                                   final DirectedType directedType,
                                   final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
                                   final SeedMatching.SeedMatchingType seedMatchingType,
                                   final Iterable<? extends ElementId> seeds) throws OperationException, StoreException {
        this.view = view;
        this.schemaUtils = store.getSchemaUtils();
        this.directedType = directedType;
        this.includeIncomingOutgoingType = includeIncomingOutgoingType;
        this.seedMatchingType = seedMatchingType;
        this.seeds = seeds;
        this.graphIndex = store.getGraphIndex();
        this.dataDir = store.getProperties().getDataDir() + "/" + store.getGraphIndex().getSnapshotTimestamp();
        this.fs = store.getFS();
    }

    @Override
    public void close() {
    }

    @Override
    public CloseableIterator<Element> iterator() {
        return new ParquetIterator(schemaUtils, view, directedType, includeIncomingOutgoingType,
                seedMatchingType, seeds, dataDir, graphIndex, fs);
    }

    protected static class ParquetIterator implements CloseableIterator<Element> {
        private Element currentElement = null;
        private ParquetReader<Element> reader;
        private SchemaUtils schemaUtils;
        private Map<Path, FilterPredicate> pathToFilterMap;
        private Path currentPath;
        private Iterator<Path> paths;
        private ParquetFileIterator fileIterator;
        private FileSystem fs;
        private Boolean needsValidation;
        private View view;

        protected ParquetIterator(final SchemaUtils schemaUtils,
                                  final View view,
                                  final DirectedType directedType,
                                  final SeededGraphFilters.IncludeIncomingOutgoingType includeIncomingOutgoingType,
                                  final SeedMatching.SeedMatchingType seedMatchingType,
                                  final Iterable<? extends ElementId> seeds,
                                  final String dataDir,
                                  final GraphIndex graphIndex,
                                  final FileSystem fs) {
            try {
                Tuple2<Map<Path, FilterPredicate>, Boolean> results = ParquetFilterUtils
                        .buildPathToFilterMap(schemaUtils,
                                view, directedType, includeIncomingOutgoingType, seedMatchingType, seeds, dataDir, graphIndex);
                this.pathToFilterMap = results.get0();
                this.needsValidation = results.get1();
                LOGGER.debug("pathToFilterMap: {}", pathToFilterMap);
                if (!pathToFilterMap.isEmpty()) {
                    this.fs = fs;
                    this.view = view;
                    this.paths = pathToFilterMap.keySet().stream().sorted().iterator();
                    this.schemaUtils = schemaUtils;
                    this.currentPath = this.paths.next();
                    try {
                        this.fileIterator = new ParquetFileIterator(this.currentPath, this.fs);
                        this.reader = openParquetReader();
                    } catch (final IOException e) {
                        LOGGER.error("Path does not exist");
                    }
                } else {
                    LOGGER.info("There are no results for this query");
                }
            } catch (final OperationException | SerialisationException e) {
                LOGGER.error("Exception while creating the mapping of file paths to Parquet filters: {}", e.getMessage());
            }
        }

        private ParquetReader<Element> openParquetReader() throws IOException {
            if (fileIterator.hasNext()) {
                final Path file = fileIterator.next();
                final String group = file.getParent().getName().split("=")[1];
                final boolean isEntity = schemaUtils.getEntityGroups().contains(group);
                final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
                LOGGER.debug("Opening a new Parquet reader for file: {}", file);
                FilterPredicate filter = pathToFilterMap.get(currentPath);
                if (filter != null) {
                    return new ParquetElementReader.Builder<Element>(file)
                            .isEntity(isEntity)
                            .usingConverter(converter)
                            .withFilter(FilterCompat.get(filter))
                            .build();
                } else {
                    return new ParquetElementReader.Builder<Element>(file)
                            .isEntity(isEntity)
                            .usingConverter(converter)
                            .build();
                }
            } else {
                if (paths.hasNext()) {
                    currentPath = paths.next();
                    fileIterator = new ParquetFileIterator(currentPath, fs);
                    return openParquetReader();
                }
            }
            return null;
        }

        @Override
        public boolean hasNext() {
            if (currentElement == null) {
                try {
                    currentElement = next();
                } catch (final NoSuchElementException e) {
                    return false;
                }
            }
            return true;
        }


        @Override
        public Element next() throws NoSuchElementException {
            Element e = getNextElement();
            if (needsValidation) {
                String group = e.getGroup();
                ElementFilter preAggFilter = view.getElement(group).getPreAggregationFilter();
                if (preAggFilter != null) {
                    while (!preAggFilter.test(e)) {
                        e = getNextElement();
                        if (!group.equals(e.getGroup())) {
                            group = e.getGroup();
                            preAggFilter = view.getElement(group).getPreAggregationFilter();
                        }
                    }
                }
            }
            return e;
        }

        private Element getNextElement() {
            Element element;
            try {
                if (currentElement != null) {
                    element = currentElement;
                    currentElement = null;
                } else {
                    if (reader != null) {
                        Element record = reader.read();
                        if (record != null) {
                            element = record;
                        } else {
                            LOGGER.debug("Closing Parquet reader");
                            reader.close();
                            reader = openParquetReader();
                            if (reader != null) {
                                record = reader.read();
                                if (record != null) {
                                    element = record;
                                } else {
                                    LOGGER.debug("This file has no data");
                                    element = next();
                                }
                            } else {
                                LOGGER.debug("Reached the end of all the files of data");
                                throw new NoSuchElementException();
                            }
                        }
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            } catch (final IOException e) {
                throw new NoSuchElementException();
            }
            return element;
        }

        @Override
        public void close() {
            try {
                if (reader != null) {
                    LOGGER.debug("Closing ParquetReader");
                    reader.close();
                    reader = null;
                }
            } catch (final IOException e) {
                LOGGER.warn("Failed to close {}", getClass().getCanonicalName());
            }
        }
    }
}
