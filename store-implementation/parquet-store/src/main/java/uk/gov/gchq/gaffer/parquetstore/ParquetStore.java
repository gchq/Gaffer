/*
 * Copyright 2017-2019. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore;

import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.GetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.ImportJavaRDDOfElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.ImportRDDOfElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.CalculatePartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GroupPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.Partition;
import uk.gov.gchq.gaffer.parquetstore.partitioner.serialisation.GraphPartitionerSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ArrayListStringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.BooleanParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ByteParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.DateParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.DoubleParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.FloatParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.FreqMapParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.HashSetStringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.InLineHyperLogLogPlusParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.IntegerParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.LongParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ShortParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.StringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.TreeSetStringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.TypeSubTypeValueParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.TypeValueParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.spark.operation.javardd.ImportJavaRDDOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.SerialisationFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.koryphe.ValidationResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;

/**
 * An implementation of {@link Store} that uses Parquet files to store the {@link Element}s.
 * <p>
 * It is designed to make the most of the Parquet file types by serialising the {@link Element}s using
 * {@link uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialiser}s which also allows for Gaffer objects to be
 * stored as multiple or nested columns of primitive types.
 */
public class ParquetStore extends Store {
    public static final String GROUP = "group";
    public static final String GRAPH = "graph";
    public static final String VERTEX = IdentifierType.VERTEX.name();
    public static final String SOURCE = IdentifierType.SOURCE.name();
    public static final String DESTINATION = IdentifierType.DESTINATION.name();
    public static final String DIRECTED = IdentifierType.DIRECTED.name();
    public static final String SNAPSHOT = "snapshot";
    public static final String REVERSED_EDGES = "reversedEdges";
    public static final String PARTITION = "partition";
    public static final int LENGTH_OF_PARTITION_NUMBER_IN_FILENAME = 7;

    @SuppressFBWarnings("MS_MUTABLE_ARRAY")
    public static final Serialiser[] SERIALISERS = new Serialiser[]{
            new StringParquetSerialiser(),
            new ByteParquetSerialiser(),
            new IntegerParquetSerialiser(),
            new LongParquetSerialiser(),
            new BooleanParquetSerialiser(),
            new DateParquetSerialiser(),
            new DoubleParquetSerialiser(),
            new FloatParquetSerialiser(),
            new InLineHyperLogLogPlusParquetSerialiser(),
            new ShortParquetSerialiser(),
            new TypeValueParquetSerialiser(),
            new FreqMapParquetSerialiser(),
            new TreeSetStringParquetSerialiser(),
            new TypeSubTypeValueParquetSerialiser(),
            new ArrayListStringParquetSerialiser(),
            new HashSetStringParquetSerialiser(),
            new JavaSerialiser()
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStore.class);
    private static final Set<StoreTrait> TRAITS =
            Collections.unmodifiableSet(Sets.newHashSet(
                    ORDERED,
//                    VISIBILITY,
                    INGEST_AGGREGATION,
                    PRE_AGGREGATION_FILTERING
//                    STORE_VALIDATION
            ));

    private GraphPartitioner graphPartitioner;
    private long currentSnapshot;
    private SchemaUtils schemaUtils;
    private FileSystem fs;

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        if (!(properties instanceof ParquetStoreProperties)) {
            throw new StoreException("ParquetStore must be initialised with properties of class ParquetStoreProperties");
        }
        final ParquetStoreProperties parquetStoreProperties = (ParquetStoreProperties) properties;
        if (null == parquetStoreProperties.getDataDir()) {
            throw new StoreException("The ParquetStoreProperties must contain a non-null data directory ("
                    + ParquetStoreProperties.DATA_DIR + ")");
        }
        if (null == parquetStoreProperties.getTempFilesDir()) {
            throw new StoreException("The ParquetStoreProperties must contain a non-null temporary data directory ("
                    + ParquetStoreProperties.TEMP_FILES_DIR + ")");
        }
        LOGGER.info("Initialising ParquetStore for graph id {}", graphId);
        super.initialise(graphId, schema, parquetStoreProperties);
        try {
            fs = FileSystem.get(new Configuration());
            schemaUtils = new SchemaUtils(getSchema());
            initialise();
            loadGraphPartitioner();
        } catch (final IOException e) {
            throw new StoreException("Could not connect to the file system", e);
        }
    }

    public static String getSnapshotPath(final long snapshot) {
        return SNAPSHOT + "=" + snapshot;
    }

    private void initialise() throws IOException, StoreException {
        // If data directory is empty or does not exist then this is the first time the store has been created.
        final Path dataDirPath = new Path(getDataDir());
        if (!fs.exists(dataDirPath) || 0 == fs.listStatus(dataDirPath).length) {
            LOGGER.info("Data directory {} doesn't exist or is empty so initialising directory structure", dataDirPath);
            currentSnapshot = System.currentTimeMillis();
            LOGGER.info("Initialising snapshot id to {}", currentSnapshot);
            final Path snapshotPath = new Path(dataDirPath, getSnapshotPath(currentSnapshot));
            LOGGER.info("Creating snapshot directory {}", snapshotPath);
            fs.mkdirs(snapshotPath);
            LOGGER.info("Creating group directories under {}", snapshotPath);
            for (final String group : getSchema().getGroups()) {
                final Path groupDir = getGroupPath(group);
                fs.mkdirs(groupDir);
                LOGGER.info("Created directory {}", groupDir);
            }
            LOGGER.info("Creating group directories for reversed edges under {}", snapshotPath);
            for (final String group : getSchema().getEdgeGroups()) {
                final Path groupDir = getGroupPathForReversedEdges(group);
                fs.mkdirs(groupDir);
                LOGGER.info("Created directory {}", groupDir);
            }
            LOGGER.info("Creating GraphPartitioner with 0 split points for each group");
            graphPartitioner = new GraphPartitioner();
            for (final String group : getSchema().getGroups()) {
                graphPartitioner.addGroupPartitioner(group, new GroupPartitioner(group, new ArrayList<>()));
            }
            for (final String group : getSchema().getEdgeGroups()) {
                graphPartitioner.addGroupPartitionerForReversedEdges(group, new GroupPartitioner(group, new ArrayList<>()));
            }
            LOGGER.info("Writing GraphPartitioner to snapshot directory");
            final FSDataOutputStream dataOutputStream = fs.create(getGraphPartitionerPath());
            new GraphPartitionerSerialiser().write(graphPartitioner, dataOutputStream);
            dataOutputStream.close();
            LOGGER.info("Wrote GraphPartitioner to file {}", getGraphPartitionerPath().toString());
        } else {
            LOGGER.info("Data directory {} exists and is non-empty, validating a snapshot directory exists", dataDirPath);
            final FileStatus[] fileStatuses = fs.listStatus(dataDirPath, f -> f.getName().startsWith(SNAPSHOT + "="));
            final List<FileStatus> directories = Arrays.stream(fileStatuses).filter(f -> f.isDirectory()).collect(Collectors.toList());
            if (0 == directories.size()) {
                LOGGER.error("Data directory {} should contain a snapshot directory", dataDirPath);
                throw new StoreException("Data directory should contain a snapshot directory");
            }
            this.currentSnapshot = getLatestSnapshot();
            LOGGER.info("Latest snapshot directory in data directory {} is {}", dataDirPath, this.currentSnapshot);
            LOGGER.info("Verifying snapshot directory contains the correct directories");
            for (final String group : getSchema().getGroups()) {
                final Path groupDir = getGroupPath(group);
                if (!fs.exists(groupDir)) {
                    LOGGER.error("Directory {} should exist", groupDir);
                    throw new StoreException("Group directory " + groupDir + " should exist in snapshot directory " + getSnapshotPath(this.currentSnapshot));
                }
            }
            for (final String group : getSchema().getEdgeGroups()) {
                final Path groupDir = getGroupPathForReversedEdges(group);
                if (!fs.exists(groupDir)) {
                    LOGGER.error("Directory {} should exist", groupDir);
                    throw new StoreException("Group directory " + groupDir + " should exist in snapshot directory " + getSnapshotPath(this.currentSnapshot));
                }
            }
        }
    }

    public Path getGraphPartitionerPath() {
        return new Path(getProperties().getDataDir() + "/" + SNAPSHOT + "=" + currentSnapshot, "graphPartitioner");
    }

    private void loadGraphPartitioner() throws StoreException {
        final String dataDir = getDataDir();
        try {
            if (fs.exists(new Path(dataDir))) {
                this.currentSnapshot = getLatestSnapshot(dataDir);
                LOGGER.info("Setting currentSnapshot to {}", this.currentSnapshot);
                final Path path = getGraphPartitionerPath();
                if (!fs.exists(path)) {
                    LOGGER.info("Graph partitioner does not exist in {} so creating it", path);
                    final GraphPartitioner partitioner =
                            new CalculatePartitioner(new Path(dataDir + "/" + getSnapshotPath(this.currentSnapshot)), getSchema(), fs).call();
                    LOGGER.info("Writing graph partitioner to {}", path);
                    final FSDataOutputStream stream = fs.create(path);
                    new GraphPartitionerSerialiser().write(partitioner, stream);
                    stream.close();
                }
                LOGGER.info("Loading graph partitioner from path {}", path);
                loadGraphPartitioner(path);
            } else {
                throw new StoreException("Data directory " + dataDir + " does not exist - store is in an inconsistent state");
            }
        } catch (final IOException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    private void loadGraphPartitioner(final Path graphPartitionerPath) throws IOException {
        final FSDataInputStream stream = fs.open(graphPartitionerPath);
        this.graphPartitioner = new GraphPartitionerSerialiser().read(stream);
        stream.close();
    }

    public FileSystem getFS() {
        return fs;
    }

    public SchemaUtils getSchemaUtils() {
        return schemaUtils;
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    public String getDataDir() {
        return getProperties().getDataDir();
    }

    public String getTempFilesDir() {
        return getProperties().getTempFilesDir();
    }

    public String getFile(final String group, final Partition partition) {
        return getFile(group, partition.getPartitionId());
    }

    public String getFile(final String group, final Integer partitionId) {
        return getDataDir()
                + "/" + getSnapshotPath(currentSnapshot)
                + "/" + GRAPH
                + "/" + GROUP + "=" + group
                + "/" + getFile(partitionId);
    }

    public static String getFile(final Integer partitionId) {
        return PARTITION + "-" + zeroPad("" + partitionId) + ".parquet";
    }

    private static String zeroPad(final String input) {
        final StringBuilder temp = new StringBuilder(input);
        while (temp.length() < LENGTH_OF_PARTITION_NUMBER_IN_FILENAME) {
            temp.insert(0, "0");
        }
        return temp.toString();
    }

    public String getFileForReversedEdges(final String group, final Partition partition) {
        return getFileForReversedEdges(group, partition.getPartitionId());
    }

    public String getFileForReversedEdges(final String group, final Integer partitionId) {
        return getDataDir()
                + "/" + getSnapshotPath(currentSnapshot)
                + "/" + REVERSED_EDGES
                + "/" + GROUP + "=" + group
                + "/" + getFile(partitionId);
    }

    public List<Path> getFilesForGroup(final String group) throws IOException {
        final Path dir = new Path(getDataDir()
                + "/" + getSnapshotPath(currentSnapshot)
                + "/" + GRAPH
                + "/" + GROUP + "=" + group);
        final FileStatus[] files = fs.listStatus(dir, path -> path.getName().endsWith(".parquet"));
        return Arrays
                .stream(files)
                .map(FileStatus::getPath)
                .collect(Collectors.toList());
    }

    public Path getGroupPath(final String group) {
        return new Path(getDataDir()
                + "/" + getSnapshotPath(currentSnapshot)
                + "/" + GRAPH
                + "/" + GROUP + "=" + group);
    }

    public static String getGroupSubDir(final String group, final boolean reversed) {
        return (reversed ? REVERSED_EDGES : GRAPH)
                + "/" + GROUP + "=" + group;
    }

    public Path getGroupPathForReversedEdges(final String group) {
        if (!getSchema().getEdgeGroups().contains(group)) {
            throw new IllegalArgumentException("Invalid group: " + group + " is not an edge group");
        }
        return new Path(getDataDir()
                + "/" + getSnapshotPath(currentSnapshot)
                + "/" + REVERSED_EDGES
                + "/" + GROUP + "=" + group);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be ParquetStoreProperties")
    @Override
    public ParquetStoreProperties getProperties() {
        return (ParquetStoreProperties) super.getProperties();
    }

    @Override
    protected Class<ParquetStoreProperties> getPropertiesClass() {
        return ParquetStoreProperties.class;
    }

    @Override
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected GetAllElementsHandler getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected GetAdjacentIdsHandler getAdjacentIdsHandler() {
        return new GetAdjacentIdsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
//        addOperationHandler(GetDataFrameOfElements.class, new GetDataframeOfElementsHandler());
        addOperationHandler(ImportJavaRDDOfElements.class, new ImportJavaRDDOfElementsHandler());
        addOperationHandler(ImportRDDOfElements.class, new ImportRDDOfElementsHandler());
//        addOperationHandler(GetGraphFrameOfElements.class, new GetGraphFrameOfElementsHandler());
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected SchemaOptimiser createSchemaOptimiser() {
        return new SchemaOptimiser(new SerialisationFactory(SERIALISERS));
    }

    @Override
    public void validateSchemas() {
        super.validateSchemas();
        validateConsistentVertex();
    }

    @Override
    protected void validateSchemaElementDefinition(final Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry, final ValidationResult validationResult) {
        super.validateSchemaElementDefinition(schemaElementDefinitionEntry, validationResult);
        validateConsistentGroupByProperties(schemaElementDefinitionEntry, validationResult);
    }

    public long getLatestSnapshot() throws StoreException {
        return getLatestSnapshot(getDataDir());
    }

    public void setLatestSnapshot(final long snapshot) throws StoreException {
        final Path snapshotPath = new Path(getDataDir(), getSnapshotPath(snapshot));
        try {
            if (!fs.exists(snapshotPath)) {
                throw new StoreException(String.format("Failed setting currentSnapshot: '%s' does not exist", snapshotPath.toString()));
            }
        } catch (final IOException e) {
            throw new StoreException("IOException checking Path: ", e);
        }

        LOGGER.info("Setting currentSnapshot to {} and reloading graph partitioner", snapshot);
        this.currentSnapshot = snapshot;
        loadGraphPartitioner();
    }

    private long getLatestSnapshot(final String rootDir) throws StoreException {
        long latestSnapshot = 0L;
        try {
            for (final FileStatus status : fs.listStatus(new Path(rootDir))) {
                final long currentSnapshot = Long.parseLong(status.getPath().getName().replace("snapshot=", ""));
                if (latestSnapshot < currentSnapshot) {
                    latestSnapshot = currentSnapshot;
                }
            }
        } catch (final IOException e) {
            throw new StoreException(e.getMessage());
        }
        return latestSnapshot;
    }

    public GraphPartitioner getGraphPartitioner() {
        return graphPartitioner;
    }
}
