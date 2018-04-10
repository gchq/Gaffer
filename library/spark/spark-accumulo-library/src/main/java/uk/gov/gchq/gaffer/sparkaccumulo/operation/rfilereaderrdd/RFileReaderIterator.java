/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
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
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@code RFileReaderIterator} is a {@link java.util.Iterator} formed by merging iterators over
 * a set of RFiles.
 */
public class RFileReaderIterator implements java.util.Iterator<Map.Entry<Key, Value>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RFileReaderIterator.class);
    private final Partition partition;
    private final TaskContext taskContext;
    private final List<SortedKeyValueIterator<Key, Value>> iterators = new ArrayList<>();
    private SortedKeyValueIterator<Key, Value> mergedIterator = null;
    private SortedKeyValueIterator<Key, Value> iteratorAfterIterators = null;
    private Configuration configuration;
    private Set<String> auths;

    public RFileReaderIterator(final Partition partition,
                               final TaskContext taskContext,
                               final Configuration configuration,
                               final Set<String> auths) {
        this.partition = partition;
        this.taskContext = taskContext;
        this.configuration = configuration;
        this.auths = auths;
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
        final Map.Entry<Key, Value> next = new AbstractMap.SimpleEntry<>(new Key(iteratorAfterIterators.getTopKey()),
                new Value(iteratorAfterIterators.getTopValue()));
        try {
            iteratorAfterIterators.next();
        } catch (final IOException e) {
            // Swallow
        }
        return next;
    }

    private void init() throws IOException {
        final AccumuloTablet accumuloTablet = (AccumuloTablet) partition;
        LOGGER.info("Initialising RFileReaderIterator for files {}", StringUtils.join(accumuloTablet.getFiles(), ','));
        final AccumuloConfiguration accumuloConfiguration = SiteConfiguration.getInstance(DefaultConfiguration.getInstance());

        // Required column families according to the configuration
        final Set<ByteSequence> requiredColumnFamilies = InputConfigurator
                .getFetchedColumns(AccumuloInputFormat.class, configuration)
                .stream()
                .map(Pair::getFirst)
                .map(c -> new ArrayByteSequence(c.toString()))
                .collect(Collectors.toSet());
        LOGGER.info("RFileReaderIterator will read column families of {}", StringUtils.join(requiredColumnFamilies, ','));

        // Column families
        final List<SortedKeyValueIterator<Key, Value>> iterators = new ArrayList<>();
        for (final String filename : accumuloTablet.getFiles()) {
            final Path path = new Path(filename);
            final FileSystem fs = path.getFileSystem(configuration);

            final RFile.Reader rFileReader = new RFile.Reader(
                    new CachableBlockFile.Reader(fs, path, configuration, null, null, accumuloConfiguration));
            iterators.add(rFileReader);
        }
        mergedIterator = new MultiIterator(iterators, true);

        // Apply visibility filtering iterator
        if (null != auths) {
            final Authorizations authorizations = new Authorizations(auths.toArray(new String[auths.size()]));
            final VisibilityFilter visibilityFilter = new VisibilityFilter(mergedIterator, authorizations, new byte[]{});
            final IteratorSetting visibilityIteratorSetting = new IteratorSetting(1, "auth", VisibilityFilter.class);
            visibilityFilter.init(mergedIterator, visibilityIteratorSetting.getOptions(), null);
            iteratorAfterIterators = visibilityFilter;
            LOGGER.info("Set authorizations to {}", authorizations);
        } else {
            iteratorAfterIterators = mergedIterator;
        }

        // Apply iterator stack
        final List<IteratorSetting> iteratorSettings = getIteratorSettings();
        iteratorSettings.sort(Comparator.comparingInt(IteratorSetting::getPriority));
        for (final IteratorSetting is : iteratorSettings) {
            iteratorAfterIterators = applyIterator(iteratorAfterIterators, is);
        }

        taskContext.addTaskCompletionListener(context -> close());

        final Range range = new Range(accumuloTablet.getStartRow(), true, accumuloTablet.getEndRow(), false);
        iteratorAfterIterators.seek(range, requiredColumnFamilies, true);
        LOGGER.info("Initialised iterator");
    }

    private SortedKeyValueIterator<Key, Value> applyIterator(final SortedKeyValueIterator<Key, Value> source,
                                                             final IteratorSetting is) {
        try {
            SortedKeyValueIterator<Key, Value> result = Class.forName(is.getIteratorClass())
                    .asSubclass(SortedKeyValueIterator.class).newInstance();
            result.init(source, is.getOptions(), new IteratorEnvironment() {
                @Override
                public SortedKeyValueIterator<Key, Value> reserveMapFileReader(final String mapFileName) throws IOException {
                    return null;
                }

                @Override
                public AccumuloConfiguration getConfig() {
                    return null;
                }

                @Override
                public IteratorUtil.IteratorScope getIteratorScope() {
                    return IteratorUtil.IteratorScope.majc;
                }

                @Override
                public boolean isFullMajorCompaction() {
                    return false;
                }

                @Override
                public void registerSideChannel(final SortedKeyValueIterator<Key, Value> iter) {

                }

                @Override
                public Authorizations getAuthorizations() {
                    return null;
                }

                @Override
                public IteratorEnvironment cloneWithSamplingEnabled() {
                    return null;
                }

                @Override
                public boolean isSamplingEnabled() {
                    return false;
                }

                @Override
                public SamplerConfiguration getSamplerConfiguration() {
                    return null;
                }
            });
            return result;
        } catch (final IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException("Exception creating iterator of class " + is.getIteratorClass());
        }
    }

    private List<IteratorSetting> getIteratorSettings() {
        return InputConfigurator.getIterators(AccumuloInputFormat.class, configuration);
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
