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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.parquetstore.Index;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetFileIterator;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetFilterUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
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
    private Index index;
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
        this.index = store.getIndex();
        this.dataDir = store.getProperties().getDataDir() + "/" + store.getCurrentSnapshot();
        this.fs = store.getFS();
    }

    @Override
    public void close() {
    }

    @Override
    public CloseableIterator<Element> iterator() {
        return new ParquetIterator(schemaUtils, view, directedType, includeIncomingOutgoingType,
                seedMatchingType, seeds, dataDir, index, fs);
    }

    protected static class ParquetIterator implements CloseableIterator<Element> {
        private Element currentElement = null;
        private ParquetReader<GenericRecord> reader;
        private SchemaUtils schemaUtils;
        private Map<String, GafferGroupObjectConverter> groupToObjectConverter;
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
                                  final Index index,
                                  final FileSystem fs) {
            try {
                Tuple2<Map<Path, FilterPredicate>, Boolean> results = ParquetFilterUtils
                        .buildPathToFilterMap(schemaUtils,
                                view, directedType, includeIncomingOutgoingType, seedMatchingType, seeds, dataDir, index);
                this.pathToFilterMap = results.get0();
                this.needsValidation = results.get1();
                LOGGER.debug("pathToFilterMap: {}", pathToFilterMap);
                if (!pathToFilterMap.isEmpty()) {
                    this.fs = fs;
                    this.view = view;
                    this.paths = pathToFilterMap.keySet().stream().sorted().iterator();
                    this.schemaUtils = schemaUtils;
                    this.groupToObjectConverter = new HashMap<>();
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

        private ParquetReader<GenericRecord> openParquetReader() throws IOException {
            if (fileIterator.hasNext()) {
                Path file = fileIterator.next();
                LOGGER.debug("Opening a new Parquet reader for file: {}", file);
                FilterPredicate filter = pathToFilterMap.get(currentPath);
                if (filter != null) {
                    return AvroParquetReader.builder(new AvroReadSupport<GenericRecord>(), file)
                            .withFilter(FilterCompat.get(filter)).build();
                } else {
                    return AvroParquetReader.builder(new AvroReadSupport<GenericRecord>(), file).build();
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
                        GenericRecord record = reader.read();
                        if (record != null) {
                            element = convertGenericRecordToElement(record);
                        } else {
                            LOGGER.debug("Closing Parquet reader");
                            reader.close();
                            reader = openParquetReader();
                            if (reader != null) {
                                record = reader.read();
                                if (record != null) {
                                    element = convertGenericRecordToElement(record);
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
            } catch (final IOException | OperationException e) {
                throw new NoSuchElementException();
            }
            if (element instanceof Edge && currentPath.toString().contains(ParquetStoreConstants.REVERSE_EDGES)) {
                while (((Edge) element).getSource().equals(((Edge) element).getDestination())) {
                    element = next();
                }
            }
            return element;
        }

        private Element convertGenericRecordToElement(final GenericRecord record) throws OperationException, SerialisationException {
            String group = (String) record.get(ParquetStoreConstants.GROUP);
            GafferGroupObjectConverter converter = getConverter(group);
            Element e;
            if (schemaUtils.getEntityGroups().contains(group)) {
                final String[] paths = schemaUtils.getPaths(group, ParquetStoreConstants.VERTEX);
                final Object[] parquetObjects = new Object[paths.length];
                for (int i = 0; i < paths.length; i++) {
                    parquetObjects[i] = recursivelyGetObjectFromRecord(paths[i], (GenericData.Record) record);
                }
                e = new Entity(group, converter.parquetObjectsToGafferObject(ParquetStoreConstants.VERTEX, parquetObjects));
            } else {
                String[] paths = schemaUtils.getPaths(group, ParquetStoreConstants.SOURCE);
                final Object[] srcParquetObjects = new Object[paths.length];
                for (int i = 0; i < paths.length; i++) {
                    srcParquetObjects[i] = recursivelyGetObjectFromRecord(paths[i], (GenericData.Record) record);
                }
                paths = schemaUtils.getPaths(group, ParquetStoreConstants.DESTINATION);
                final Object[] dstParquetObjects = new Object[paths.length];
                for (int i = 0; i < paths.length; i++) {
                    dstParquetObjects[i] = recursivelyGetObjectFromRecord(paths[i], (GenericData.Record) record);
                }
                e = new Edge(group, converter.parquetObjectsToGafferObject(ParquetStoreConstants.SOURCE, srcParquetObjects),
                        converter.parquetObjectsToGafferObject(ParquetStoreConstants.DESTINATION, dstParquetObjects),
                        (boolean) record.get(ParquetStoreConstants.DIRECTED));
            }

            for (final String column : schemaUtils.getGafferSchema().getElement(group).getProperties()) {
                final String[] paths = schemaUtils.getPaths(group, column);
                final Object[] parquetObjects = new Object[paths.length];
                for (int i = 0; i < paths.length; i++) {
                    final String path = paths[i];
                    parquetObjects[i] = recursivelyGetObjectFromRecord(path, (GenericData.Record) record);
                }
                e.putProperty(column, getConverter(group).parquetObjectsToGafferObject(column, parquetObjects));
            }
            return e;
        }

        private Object recursivelyGetObjectFromRecord(final String path, final GenericData.Record record) {
            if (path.contains(".")) {
                final int dotIndex = path.indexOf(".");
                return recursivelyGetObjectFromRecord(path.substring(dotIndex + 1), (GenericData.Record) record.get(path.substring(0, dotIndex)));
            } else {
                if (record != null) {
                    Object result = record.get(path);
                    if (result instanceof ByteBuffer) {
                        result = ((ByteBuffer) result).array();
                    }
                    return result;
                } else {
                    return null;
                }
            }
        }

        // TODO -  seems no benefit in doing this over just getting from schemaUtils?
        private GafferGroupObjectConverter getConverter(final String group) throws SerialisationException {
            if (groupToObjectConverter.containsKey(group)) {
                return groupToObjectConverter.get(group);
            } else {
                GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
                groupToObjectConverter.put(group, converter);
                return converter;
            }
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
