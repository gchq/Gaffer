/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore.integration;


import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.RangeFactory;
import gaffer.accumulostore.key.core.impl.CoreKeyBloomFunctor;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityRangeFactory;
import gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import gaffer.accumulostore.key.core.impl.classic.ClassicRangeFactory;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.key.exception.RangeFactoryException;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Entity;
import gaffer.data.element.Properties;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.data.elementdefinition.view.ViewEntityDefinition;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.serialisation.implementation.JavaSerialiser;
import gaffer.store.schema.StoreElementDefinition;
import gaffer.store.schema.StoreSchema;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
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
import org.codehaus.plexus.util.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the performance of the Bloom filter - checks that looking up random data is quicker
 * than looking up data that is present.
 * This class is based on Accumulo's BloomFilterLayerLookupTest (org.apache.accumulo.core.file.BloomFilterLayerLookupTest).
 */
public class BloomFilterIT {

    private final static RangeFactory byteEntityRangeFactory;
    private final static AccumuloElementConverter byteEntityElementConverter;
    private final static RangeFactory Gaffer1RangeFactory;
    private final static AccumuloElementConverter gafferV1ElementConverter;
    private final static StoreSchema schema;

    static {
        schema = new StoreSchema.Builder()
                .vertexSerialiser(new JavaSerialiser())
                .edge(TestGroups.EDGE, new StoreElementDefinition())
                .entity(TestGroups.ENTITY, new StoreElementDefinition())
                .build();
        byteEntityRangeFactory = new ByteEntityRangeFactory(schema);
        byteEntityElementConverter = new ByteEntityAccumuloElementConverter(schema);
        Gaffer1RangeFactory = new ClassicRangeFactory(schema);
        gafferV1ElementConverter = new ClassicAccumuloElementConverter(schema);
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void test() throws AccumuloElementConversionException, RangeFactoryException {
        testFilter(byteEntityElementConverter, byteEntityRangeFactory);
        testFilter(gafferV1ElementConverter, Gaffer1RangeFactory);
    }

    public void testFilter(final AccumuloElementConverter elementConverter, final RangeFactory rangeFactory) throws AccumuloElementConversionException, RangeFactoryException {
        try {
            // Create random data to insert, and sort it
            final Random random = new Random();
            final HashSet<Key> keysSet = new HashSet<>();
            final HashSet<Entity> dataSet = new HashSet<>();
            for (int i = 0; i < 100000; i++) {
                final Entity source = new Entity(TestGroups.ENTITY);
                source.setVertex("type" + random.nextInt(Integer.MAX_VALUE));
                final Entity destination = new Entity(TestGroups.ENTITY);
                destination.setVertex("type" + random.nextInt(Integer.MAX_VALUE));
                dataSet.add(source);
                dataSet.add(destination);
                final Entity sourceEntity = new Entity(source.getGroup());
                sourceEntity.setVertex(source.getVertex());
                final Entity destinationEntity = new Entity(destination.getGroup());
                destinationEntity.setVertex(destination.getVertex());
                final Edge edge = new Edge(TestGroups.EDGE, source.getVertex(), destination.getVertex(), true);
                keysSet.add(elementConverter.getKeyFromEntity(sourceEntity));
                keysSet.add(elementConverter.getKeyFromEntity(destinationEntity));
                final Pair<Key> edgeKeys = elementConverter.getKeysFromEdge(edge);
                keysSet.add(edgeKeys.getFirst());
                keysSet.add(edgeKeys.getSecond());
            }
            final ArrayList<Key> keys = new ArrayList<>(keysSet);
            Collections.sort(keys);
            final Properties property = new Properties();
            property.put(AccumuloPropertyNames.INT, 10);
            final Value value = elementConverter.getValueFromProperties(property, TestGroups.ENTITY);
            final Value value2 = elementConverter.getValueFromProperties(property, TestGroups.EDGE);

            // Create Accumulo configuration
            final ConfigurationCopy accumuloConf = new ConfigurationCopy(AccumuloConfiguration.getDefaultConfiguration());
            accumuloConf.set(Property.TABLE_BLOOM_ENABLED, "true");
            accumuloConf.set(Property.TABLE_BLOOM_KEY_FUNCTOR, CoreKeyBloomFunctor.class.getName());
            accumuloConf.set(Property.TABLE_FILE_TYPE, RFile.EXTENSION);
            accumuloConf.set(Property.TABLE_BLOOM_LOAD_THRESHOLD, "1");
            accumuloConf.set(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT, "1");

            // Create Hadoop configuration
            final Configuration conf = CachedConfiguration.getInstance();
            final FileSystem fs = FileSystem.get(conf);

            // Open file
            final String suffix = FileOperations.getNewFileExtension(accumuloConf);
            final String filenameTemp = tempFolder.newFile().getAbsolutePath();
            FileUtils.fileDelete(filenameTemp);
            final String filename = filenameTemp + "." + suffix;
            final File file = new File(filename);
            if (file.exists()) {
                file.delete();
            }
            final FileSKVWriter writer = FileOperations.getInstance().openWriter(filename, fs, conf, accumuloConf);

            // Write data to file
            writer.startDefaultLocalityGroup();
            for (Key key : keys) {
                if (elementConverter.getElementFromKey(key).getGroup().equals(TestGroups.ENTITY)) {
                    writer.append(key, value);
                } else {
                    writer.append(key, value2);
                }
            }
            writer.close();

            // Reader
            FileSKVIterator reader = FileOperations.getInstance().openReader(filename, false, fs, conf, accumuloConf);

            // Calculate random look up rate - run it 3 times and take best
            final int numTrials = 5;
            double maxRandomRate = -1.0;
            for (int i = 0; i < numTrials; i++) {
                double rate = calculateRandomLookUpRate(reader, dataSet, random, rangeFactory);
                if (rate > maxRandomRate) {
                    maxRandomRate = rate;
                }
            }
            System.out.println("Max random rate = " + maxRandomRate);

            // Calculate look up rate for items that were inserted
            double maxCausalRate = -1.0;
            for (int i = 0; i < numTrials; i++) {
                double rate = calculateCausalLookUpRate(reader, dataSet, random, rangeFactory);
                if (rate > maxCausalRate) {
                    maxCausalRate = rate;
                }
            }
            System.out.println("Max causal rate = " + maxCausalRate);

            // Random look up rate should be much faster
            assertTrue(maxRandomRate > maxCausalRate);

            // Close reader
            reader.close();
        } catch (IOException e) {
            fail("IOException " + e);
        }
    }

    private double calculateRandomLookUpRate(final FileSKVIterator reader, final HashSet<Entity> dataSet, final Random random, final RangeFactory rangeFactory) throws IOException, AccumuloElementConversionException, RangeFactoryException {
        EntitySeed[] randomData = new EntitySeed[5000];
        for (int i = 0; i < 5000; i++) {
            randomData[i] = new EntitySeed("type" + random.nextInt(Integer.MAX_VALUE));
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5000; i++) {
            seek(reader, randomData[i], rangeFactory);
            if (dataSet.contains(randomData[i])) {
                assertTrue(reader.hasTop());
            }
        }
        long end = System.currentTimeMillis();
        double randomRate = 5000 / ((end - start) / 1000.0);
        System.out.println("Random look up rate = " + randomRate);
        return randomRate;
    }

    private double calculateCausalLookUpRate(final FileSKVIterator reader, final HashSet<Entity> dataSet, final Random random, final RangeFactory rangeFactory) throws IOException, RangeFactoryException {
        int count = 0;
        long start = System.currentTimeMillis();
        for (Entity simpleEntity : dataSet) {
            seek(reader, ElementSeed.createSeed(simpleEntity), rangeFactory);
            assertTrue(reader.hasTop());
            count++;
            if (count >= 5000) {
                break;
            }
        }
        long end = System.currentTimeMillis();
        double causalRate = 5000 / ((end - start) / 1000.0);
        System.out.println("Causal look up rate = " + causalRate);
        return causalRate;
    }

    private void seek(final FileSKVIterator reader, final EntitySeed seed, final RangeFactory rangeFactory) throws IOException, RangeFactoryException {
        View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewEdgeDefinition())
                .entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();

        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view);
        List<Range> range = rangeFactory.getRange(seed, operation);
        for (Range ran : range) {
            reader.seek(ran, new ArrayList<ByteSequence>(), false);
        }
    }
}
