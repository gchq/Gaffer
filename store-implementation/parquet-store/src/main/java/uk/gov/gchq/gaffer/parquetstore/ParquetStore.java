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
package uk.gov.gchq.gaffer.parquetstore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.AddElementsFromRDDHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.DummyGetAdjacentEntitySeedsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetDataframeOfElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialisationFactory;
import uk.gov.gchq.gaffer.parquetstore.utils.Constants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class ParquetStore extends Store {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStore.class);
    private static final Set<StoreTrait> TRAITS = new HashSet<>();
    private HashMap<String, ArrayList<Tuple3<Object[], Object[], String>>> groupToIndex;
    private SchemaUtils schemaUtils;
    private FileSystem fs;
    private long currentSnapshot;

    static {
        TRAITS.add(StoreTrait.INGEST_AGGREGATION);
        TRAITS.add(StoreTrait.ORDERED);
        TRAITS.add(StoreTrait.PRE_AGGREGATION_FILTERING);
    }

    public ParquetStore(final Schema schema, final StoreProperties properties) throws StoreException {
        super();
        initialise(schema, properties);
    }

    public ParquetStore() {
        super();
    }

    @Override
    protected SchemaOptimiser createSchemaOptimiser() {
        return new SchemaOptimiser(new ParquetSerialisationFactory());
    }

    @Override
    public void initialise(final Schema schema, final StoreProperties properties) throws StoreException {
        super.initialise(schema, properties);
        this.schemaUtils = new SchemaUtils(this.getSchema());
        loadIndices();
    }

    public FileSystem getFS() throws StoreException {
        if (this.fs == null) {
            try {
                this.fs = FileSystem.get(new Configuration());
            } catch (IOException e) {
                throw new StoreException("Could not connect to the file system", e);
            }
        }
        return this.fs;
    }

    public SchemaUtils getSchemaUtils() {
        if (this.schemaUtils == null) {
            this.schemaUtils = new SchemaUtils(getSchema());
        }
        return this.schemaUtils;
    }

    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    public boolean isValidationRequired() {
        return false;
    }

    @Override
    protected Context createContext(final User user) {
        if (user instanceof SparkUser) {
            final SparkConf conf = ((SparkUser) user).getSparkSession().sparkContext().getConf();
            final String sparkSerialiser = conf.get("spark.serializer", null);
            if (sparkSerialiser == null || !sparkSerialiser.equals("org.apache.spark.serializer.KryoSerializer")) {
                LOGGER.warn("For the best performance you should set the spark config 'spark.serializer' = 'org.apache.spark.serializer.KryoSerializer'");
            }
            final String sparkKryoRegistrator = conf.get("spark.kryo.registrator", null);
            if (sparkKryoRegistrator == null || !sparkKryoRegistrator.equals("uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")) {
                LOGGER.warn("For the best performance you should set the spark config 'spark.kryo.registrator' = 'uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator'");
            }
            return super.createContext(user);
        } else {
            LOGGER.info("Setting up the Spark session using " + this.getProperties().getSparkMaster() + " as the master URL");
            final SparkSession spark = SparkSession.builder()
                    .appName("Gaffer Parquet Store")
                    .master(this.getProperties().getSparkMaster())
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
                    .getOrCreate();
            return super.createContext(new SparkUser(user, spark));
        }
    }

    protected void addAdditionalOperationHandlers() {
        addOperationHandler(GetDataFrameOfElements.class, new GetDataframeOfElementsHandler());
        addOperationHandler(ImportRDDOfElements.class, new AddElementsFromRDDHandler());
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
    protected DummyGetAdjacentEntitySeedsHandler getAdjacentIdsHandler() {
        return new DummyGetAdjacentEntitySeedsHandler();
    }

    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        throw new UnsupportedOperationException("Operation " + operation.getClass() + " is not supported by the ParquetStore.");
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be ParquetStoreProperties")
    @Override
    public ParquetStoreProperties getProperties() {
        return (ParquetStoreProperties) super.getProperties();
    }

    public void loadIndices() throws StoreException {
        try {
            if (this.groupToIndex == null) {
                this.groupToIndex = new HashMap<>();
            } else {
                this.groupToIndex.clear();
            }
            final FileSystem fs = getFS();
            final String rootDirString = this.getProperties().getDataDir();
            final Path rootDir = new Path(rootDirString);
            if (fs.exists(rootDir)) {
                final FileStatus[] snapshots = fs.listStatus(rootDir);
                long latestSnapshot = getCurrentSnapshot();
                for (final FileStatus fileStatus : snapshots) {
                    final String snapshot = fileStatus.getPath().getName();
                    long currentSnapshot = Long.parseLong(snapshot);
                    if (latestSnapshot < currentSnapshot) {
                        latestSnapshot = currentSnapshot;
                    }
                }
                if (latestSnapshot != 0L) {
                    setCurrentSnapshot(latestSnapshot);
                    for (final String group : this.schemaUtils.getEntityGroups()) {
                        loadIndex(group, Constants.VERTEX);
                    }
                    for (final String group : this.schemaUtils.getEdgeGroups()) {
                        loadIndex(group, Constants.SOURCE);
                        loadIndex(group, Constants.DESTINATION);
                    }
                }
            }
        } catch (IOException e) {
            throw new StoreException("Failed to connect to the file system", e);
        }
    }

    private void loadIndex(final String group, final String identifier) throws StoreException {
        try {
            final String indexDir;
            final Path path;
            if (identifier.equals(Constants.VERTEX)) {
                indexDir = getProperties().getDataDir() + "/" + getCurrentSnapshot() + "/graph/" + Constants.GROUP + "=" + group + "/";
                path = new Path(indexDir + "_index");
            } else if (identifier.equals(Constants.SOURCE)) {
                indexDir = getProperties().getDataDir() + "/" + getCurrentSnapshot() + "/graph/" + Constants.GROUP + "=" + group + "/";
                path = new Path(indexDir + "_index");
            } else {
                indexDir = getProperties().getDataDir() + "/" + getCurrentSnapshot() + "/reverseEdges/" + Constants.GROUP + "=" + group + "/";
                path = new Path(indexDir + "_index");
            }
            LOGGER.info("Loading the index from path " + path.toString());
            final ArrayList<Tuple3<Object[], Object[], String>> index = new ArrayList<>();
            if (getFS().exists(path)) {
                final FSDataInputStream reader = getFS().open(path);
                while (reader.available() > 0) {
                    final int numOfCols = reader.readInt();
                    final Object[] minObjects = new Object[numOfCols];
                    final Object[] maxObjects = new Object[numOfCols];
                    for (int i = 0; i < numOfCols; i++) {
                        final int colTypeLength = reader.readInt();
                        final byte[] colType = readBytes(colTypeLength, reader);
                        final int minLength = reader.readInt();
                        final byte[] min = readBytes(minLength, reader);
                        minObjects[i] = deserialiseColumn(colType, min);
                        final int maxLength = reader.readInt();
                        final byte[] max = readBytes(maxLength, reader);
                        maxObjects[i] = deserialiseColumn(colType, max);
                    }
                    final int filePathLength = reader.readInt();
                    final byte[] filePath = readBytes(filePathLength, reader);
                    final String fileString = new String(filePath, Charset.forName("UTF-8"));
                    LOGGER.debug("min: " + Arrays.toString(minObjects));
                    LOGGER.debug("max: " + Arrays.toString(maxObjects));
                    LOGGER.debug("filePath: " + indexDir + fileString);
                    index.add(new Tuple3<>(minObjects, maxObjects, indexDir + fileString));
                }
                reader.close();
                index.sort(Comparator.comparing(Tuple3::_3));
                if (identifier.equals(Constants.DESTINATION)) {
                    this.groupToIndex.put(group + "_reversed", index);
                } else {
                    this.groupToIndex.put(group, index);
                }
            }
        } catch (IOException e) {
            if (identifier.equals(Constants.DESTINATION)) {
                throw new StoreException("IO Exception while loading the index from " + getProperties().getDataDir() +
                        "/" + getCurrentSnapshot() + "/reverseEdges/" + Constants.GROUP + "=" + group + "/_index", e);
            } else {
                throw new StoreException("IO Exception while loading the index from " + getProperties().getDataDir() +
                        "/" + getCurrentSnapshot() + "/graph/" + Constants.GROUP + "=" + group + "/_index", e);
            }
        }
    }

    private byte[] readBytes(final int length, final FSDataInputStream reader) throws IOException {
        final byte[] bytes = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length && bytesRead > -1) {
            bytesRead += reader.read(bytes, bytesRead, length - bytesRead);
        }
        return bytes;
    }

    private Object deserialiseColumn(final byte[] colType, final byte[] value) {
        final String colTypeName = new String(colType, Charset.forName("UTF-8"));
        if (colTypeName.equals("long")) {
            return BytesUtils.bytesToLong(value);
        } else if (colTypeName.equals("int")) {
            return BytesUtils.bytesToInt(value);
        } else if (colTypeName.equals("boolean")) {
            return BytesUtils.bytesToBool(value);
        } else if (colTypeName.equals("float")) {
            return Float.intBitsToFloat(BytesUtils.bytesToInt(value));
        } else if (colTypeName.endsWith(" (UTF8)")) {
            return Binary.fromReusedByteArray(value).toStringUsingUTF8();
        } else {
            return value;
        }
    }

    public ArrayList<Tuple3<Object[], Object[], String>> getIndexForGroup(final String group) {
        return this.groupToIndex.get(group);
    }

    public HashMap<String, ArrayList<Tuple3<Object[], Object[], String>>> getIndex() {
        return this.groupToIndex;
    }

    public void setCurrentSnapshot(final long timestamp) {
        this.currentSnapshot = timestamp;
    }

    public long getCurrentSnapshot() {
        return this.currentSnapshot;
    }
}
