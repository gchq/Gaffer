/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.directrdd;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A <code>RFileReaderIterator</code> is a {@link scala.collection.Iterator} formed by merging iterators over
 * a set of RFiles.
 */
public class RFileReaderIterator implements java.util.Iterator<Map.Entry<Key, Value>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RFileReaderIterator.class);
    private final Partition partition;
    private final TaskContext taskContext;
//    private final Set<String> requiredColumnFamilies;
    private final List<SortedKeyValueIterator<Key, Value>> iterators = new ArrayList<>();
    private SortedKeyValueIterator<Key, Value> mergedIterator = null;
    private SortedKeyValueIterator<Key, Value> iteratorAfterIterators = null;
    private Configuration configuration;

    public RFileReaderIterator(final Partition partition,
                               final TaskContext taskContext,
//                               final Set<String> requiredColumnFamilies,
                               final Configuration configuration) {
        this.partition = partition;
        this.taskContext = taskContext;
//        if (null == requiredColumnFamilies || requiredColumnFamilies.isEmpty()) {
//            throw new IllegalArgumentException("requiredColumnFamilies must be non-null and non-empty");
//        }
//        this.requiredColumnFamilies = requiredColumnFamilies;
        this.configuration = configuration;
        try {
            init();
        } catch (final IOException e) {
            throw new RuntimeException("IOException initialising RFileReaderIterator", e);
        }
    }

    @Override
    public boolean hasNext() {
        return iteratorAfterIterators.hasTop();
    }

    @Override
    public Map.Entry<Key, Value> next() {
        final Map.Entry<Key, Value> next = new AbstractMap.SimpleEntry<>(iteratorAfterIterators.getTopKey(),
                iteratorAfterIterators.getTopValue());
        try {
            iteratorAfterIterators.next();
        } catch (final IOException e) {
            // Swallow
        }
        return next;
    }

    private void init() throws IOException {
        LOGGER.info("Initialising RFileReaderIterator");
        final AccumuloTablet accumuloTablet = (AccumuloTablet) partition;
        final AccumuloConfiguration accumuloConfiguration = SiteConfiguration.getInstance(DefaultConfiguration.getInstance());

        // Required column families according to the configuration
        final Set<ByteSequence> requiredColumnFamilies = InputConfigurator
                .getFetchedColumns(AccumuloInputFormat.class, configuration)
                .stream()
                .map(Pair::getFirst)
                .map(c -> new ArrayByteSequence(c.toString()))
                .collect(Collectors.toSet());

        // Column families
        final Set<ByteSequence> columnFamilies = new HashSet<>();
        final List<SortedKeyValueIterator<Key, Value>> iterators = new ArrayList<>();
        for (final String filename : accumuloTablet.getFiles()) {
            final Path path = new Path(filename);
            final FileSystem fs = path.getFileSystem(configuration);

            final RFile.Reader rFileReader = new RFile.Reader(
                    new CachableBlockFile.Reader(fs, path, configuration, null, null, accumuloConfiguration));
            iterators.add(rFileReader);

            for (final ArrayList<ByteSequence> cfs : rFileReader.getLocalityGroupCF().values()) {
                for (final ByteSequence bs : cfs) {
                    if (requiredColumnFamilies.contains(cfs.toString())) {
                        columnFamilies.add(bs);
                    }
                }
            }
        }
        mergedIterator = new MultiIterator(iterators, true);

        // Apply iterator stack
        final List<IteratorSetting> iteratorSettings = getIteratorSettings();
        iteratorSettings.sort((is1, is2) -> is1.getPriority() - is2.getPriority());
        iteratorAfterIterators = mergedIterator;
        for (final IteratorSetting is : iteratorSettings) {
            iteratorAfterIterators = applyIterator(iteratorAfterIterators, is);
        }

        taskContext.addTaskCompletionListener(new TaskCompletionListener() {
            @Override
            public void onTaskCompletion(final TaskContext context) {
                close();
            }
        });

        final Range range = new Range(accumuloTablet.getStartRow(), true, accumuloTablet.getEndRow(), false);
        iteratorAfterIterators.seek(range, columnFamilies, false);
        LOGGER.info("Initialised iterator");
    }

    private SortedKeyValueIterator<Key, Value> applyIterator(final SortedKeyValueIterator<Key, Value> source,
                                                             final IteratorSetting is) {
        try {
            SortedKeyValueIterator<Key, Value> result = Class.forName(is.getIteratorClass())
                    .asSubclass(SortedKeyValueIterator.class).newInstance();
            result.init(source, is.getOptions(), null);
            return result;
        } catch (final IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException("Exception creating iterator of class " + is.getIteratorClass());
        }
    }

    // Taken from org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator
    private List<IteratorSetting> getIteratorSettings() {
        final String iterators = configuration.get(enumToConfKey(AccumuloInputFormat.class, InputConfigurator.ScanOpts.ITERATORS));

        // If no iterators are present, return an empty list
        if (iterators == null || iterators.isEmpty()) {
            LOGGER.info("Found no iterators on configuration");
            return new ArrayList<>();
        }
        LOGGER.info("Found {} iterators in configuration", iterators.length());

        // Compose the set of iterators encoded in the job configuration
        final StringTokenizer tokens = new StringTokenizer(iterators, StringUtils.COMMA_STR);
        final List<IteratorSetting> list = new ArrayList<>();
        try {
            while (tokens.hasMoreTokens()) {
                final String itstring = tokens.nextToken();
                final ByteArrayInputStream bais = new ByteArrayInputStream(
                        org.apache.accumulo.core.util.Base64.decodeBase64(itstring.getBytes(UTF_8)));
                list.add(new IteratorSetting(new DataInputStream(bais)));
                bais.close();
                LOGGER.info("Added iterator {}", list.get(list.size() - 1));
            }
        } catch (final IOException e) {
            throw new IllegalArgumentException("couldn't decode iterator settings");
        }
        return list;
    }

    protected static String enumToConfKey(final Class<?> implementingClass, final Enum<?> e) {
        return implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "." + StringUtils.camelize(e.name().toLowerCase());
    }

    private void close() {
        for (final SortedKeyValueIterator<Key, Value> iterator : iterators) {
            RFile.Reader reader = null;
            try {
                reader = (RFile.Reader) iterator;
                LOGGER.debug("Closing RFile.Reader {}", reader);
                reader.close();
            } catch (final IOException e) {
                LOGGER.error("IOException closing reader {}", reader);
            }
        }
    }
}
