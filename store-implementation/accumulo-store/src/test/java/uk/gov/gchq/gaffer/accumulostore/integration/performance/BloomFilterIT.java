/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.integration.performance;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.RangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.CoreKeyBloomFunctor;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityRangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicRangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Tests the performance of the Bloom filter - checks that looking up random data is quicker
 * than looking up data that is present.
 * This class is based on Accumulo's BloomFilterLayerLookupTest (org.apache.accumulo.core.file.BloomFilterLayerLookupTest).
 */
public class BloomFilterIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterIT.class);
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private RangeFactory byteEntityRangeFactory;
    private AccumuloElementConverter byteEntityElementConverter;
    private RangeFactory gaffer1RangeFactory;
    private AccumuloElementConverter gafferV1ElementConverter;

    @Before
    public void setup() {
        Schema schema = new Schema.Builder()
                .type(TestTypes.PROP_INTEGER, Integer.class)
                .vertexSerialiser(new JavaSerialiser())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(AccumuloPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(AccumuloPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .build())
                .build();
        byteEntityRangeFactory = new ByteEntityRangeFactory(schema);
        byteEntityElementConverter = new ByteEntityAccumuloElementConverter(schema);
        gaffer1RangeFactory = new ClassicRangeFactory(schema);
        gafferV1ElementConverter = new ClassicAccumuloElementConverter(schema);
    }

    @Test
    public void test() throws RangeFactoryException, IOException {
        testFilter(byteEntityElementConverter, byteEntityRangeFactory);
        testFilter(gafferV1ElementConverter, gaffer1RangeFactory);
    }

    private void testFilter(final AccumuloElementConverter elementConverter, final RangeFactory rangeFactory) throws RangeFactoryException, IOException {
        // Create random data to insert, and sort it
        final Random random = new Random();
        final HashSet<Key> keysSet = new HashSet<>();
        final HashSet<Entity> dataSet = new HashSet<>();
        for (int i = 0; i < 100000; i++) {
            final Entity source = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("type" + random.nextInt(Integer.MAX_VALUE))
                    .build();
            final Entity destination = new Entity(TestGroups.ENTITY);
            destination.setVertex("type" + random.nextInt(Integer.MAX_VALUE));
            dataSet.add(source);
            dataSet.add(destination);
            final Entity sourceEntity = new Entity.Builder()
                    .group(source.getGroup())
                    .vertex(source.getVertex())
                    .build();
            final Entity destinationEntity = new Entity(destination.getGroup());
            destinationEntity.setVertex(destination.getVertex());
            final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                    .source(source.getVertex())
                    .dest(destination.getVertex())
                    .directed(true)
                    .build();
            keysSet.add(elementConverter.getKeyFromEntity(sourceEntity));
            keysSet.add(elementConverter.getKeyFromEntity(destinationEntity));
            final Pair<Key, Key> edgeKeys = elementConverter.getKeysFromEdge(edge);
            keysSet.add(edgeKeys.getFirst());
            keysSet.add(edgeKeys.getSecond());
        }
        final ArrayList<Key> keys = new ArrayList<>(keysSet);
        Collections.sort(keys);
        final Properties property = new Properties();
        property.put(AccumuloPropertyNames.COUNT, 10);
        final Value value = elementConverter.getValueFromProperties(TestGroups.ENTITY, property);
        final Value value2 = elementConverter.getValueFromProperties(TestGroups.EDGE, property);

        // Create Accumulo configuration
        final ConfigurationCopy accumuloConf = new ConfigurationCopy(AccumuloConfiguration
                .getDefaultConfiguration());
        accumuloConf.set(Property.TABLE_BLOOM_ENABLED, "true");
        accumuloConf.set(Property.TABLE_BLOOM_KEY_FUNCTOR, CoreKeyBloomFunctor.class
                .getName());
        accumuloConf.set(Property.TABLE_FILE_TYPE, RFile.EXTENSION);
        accumuloConf.set(Property.TABLE_BLOOM_LOAD_THRESHOLD, "1");
        accumuloConf.set(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT, "1");

        // Create Hadoop configuration
        final Configuration conf = CachedConfiguration.getInstance();
        final FileSystem fs = FileSystem.get(conf);

        // Open file
        final String suffix = FileOperations.getNewFileExtension(accumuloConf);
        final String filenameTemp = tempFolder.getRoot().getAbsolutePath();
        final String filename = filenameTemp + "." + suffix;
        final File file = new File(filename);
        if (file.exists()) {
            file.delete();
        }

        // Writer
        final FileSKVWriter writer = FileOperations.getInstance()
                .newWriterBuilder()
                .forFile(filename, fs, conf)
                .withTableConfiguration(accumuloConf)
                .build();

        try {
            // Write data to file
            writer.startDefaultLocalityGroup();
            for (final Key key : keys) {
                if (elementConverter.getElementFromKey(key, false)
                        .getGroup()
                        .equals(TestGroups.ENTITY)) {
                    writer.append(key, value);
                } else {
                    writer.append(key, value2);
                }
            }
        } finally {
            writer.close();
        }

        // Reader
        final FileSKVIterator reader = FileOperations.getInstance()
                .newReaderBuilder()
                .forFile(filename, fs, conf)
                .withTableConfiguration(accumuloConf)
                .seekToBeginning(false)
                .build();

        try {
            // Calculate random look up rate - run it 3 times and take best
            final int numTrials = 5;
            double maxRandomRate = -1.0;
            for (int i = 0; i < numTrials; i++) {
                final double rate = calculateRandomLookUpRate(reader, dataSet, random, rangeFactory);
                if (rate > maxRandomRate) {
                    maxRandomRate = rate;
                }
            }
            LOGGER.info("Max random rate = {}", maxRandomRate);

            // Calculate look up rate for items that were inserted
            double maxCausalRate = -1.0;
            for (int i = 0; i < numTrials; i++) {
                double rate = calculateCausalLookUpRate(reader, dataSet, random, rangeFactory);
                if (rate > maxCausalRate) {
                    maxCausalRate = rate;
                }
            }
            LOGGER.info("Max causal rate = {}", maxCausalRate);

            // Random look up rate should be much faster
            assertTrue(maxRandomRate > maxCausalRate);
        } finally {
            // Close reader
            reader.close();
        }
    }

    private double calculateRandomLookUpRate(final FileSKVIterator reader, final HashSet<Entity> dataSet, final Random random, final RangeFactory rangeFactory) throws IOException, RangeFactoryException {
        final EntityId[] randomData = new EntitySeed[5000];
        for (int i = 0; i < 5000; i++) {
            randomData[i] = new EntitySeed("type" + random.nextInt(Integer.MAX_VALUE));
        }
        final long start = System.currentTimeMillis();
        for (int i = 0; i < 5000; i++) {
            seek(reader, randomData[i], rangeFactory);
            if (dataSet.contains(randomData[i])) {
                assertTrue(reader.hasTop());
            }
        }
        final long end = System.currentTimeMillis();
        final double randomRate = 5000 / ((end - start) / 1000.0);
        LOGGER.info("Random look up rate = {}", randomRate);
        return randomRate;
    }

    private double calculateCausalLookUpRate(final FileSKVIterator reader, final HashSet<Entity> dataSet, final Random random, final RangeFactory rangeFactory) throws IOException, RangeFactoryException {
        int count = 0;
        final long start = System.currentTimeMillis();
        for (final Entity simpleEntity : dataSet) {
            seek(reader, ElementSeed.createSeed(simpleEntity), rangeFactory);
            assertTrue(reader.hasTop());
            count++;
            if (count >= 5000) {
                break;
            }
        }
        final long end = System.currentTimeMillis();
        final double causalRate = 5000 / ((end - start) / 1000.0);
        LOGGER.info("Causal look up rate = {}", causalRate);
        return causalRate;
    }

    private void seek(final FileSKVIterator reader, final EntityId seed, final RangeFactory rangeFactory) throws IOException, RangeFactoryException {
        final GetElements operation = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();
        final List<Range> range = rangeFactory.getRange(seed, operation);
        for (final Range ran : range) {
            reader.seek(ran, new ArrayList<>(), false);
        }
    }
}
