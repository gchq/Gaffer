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

package gaffer.accumulostore;

import java.io.UnsupportedEncodingException;
import java.nio.file.Path;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.TableUtils;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.graph.Graph;
import gaffer.store.StoreException;
import gaffer.store.schema.StoreSchema;

/**
 * Factory for creating new {@link gaffer.graph.Graph} instances of
 * {@link gaffer.accumulostore.AccumuloStore}.
 */
public final class AccumuloStoreBackedGraphFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloStoreBackedGraphFactory.class);

    private AccumuloStoreBackedGraphFactory() {
        // private constructor to prevent users instantiating this class.
        // All methods in this factory are static and should be called directly.
    }

    /**
     * Creates a new {@link gaffer.accumulostore.AccumuloStore} from a
     * properties file only, provided the table name specified in that
     * properties file already exists
     *
     * @param propertiesFileLocation the properties file location
     * @return A new Instance of the AccumuloStore
     * @throws StoreException if any issues occur creating a Graph backed by an Accumulo Store.
     */
    public static Graph getGraph(final Path propertiesFileLocation) throws StoreException {
        final AccumuloProperties props = new AccumuloProperties(propertiesFileLocation);
        final MapWritable map = TableUtils.getStoreConstructorInfo(props);
        final DataSchema dataSchema = DataSchema
                .fromJson(((BytesWritable) map.get(AccumuloStoreConstants.DATA_SCHEMA_KEY)).getBytes());
        final StoreSchema storeSchema = StoreSchema
                .fromJson(((BytesWritable) map.get(AccumuloStoreConstants.STORE_SCHEMA_KEY)).getBytes());
        final String keyPackageClass;
        try {
            keyPackageClass = new String(((BytesWritable) map.get(AccumuloStoreConstants.KEY_PACKAGE_KEY)).getBytes(),
                    AccumuloStoreConstants.UTF_8_CHARSET);
        } catch (final UnsupportedEncodingException e) {
            throw new StoreException(e.getMessage(), e);
        }

        if (!props.getKeyPackageClass().equals(keyPackageClass)) {
            LOGGER.warn("Key package class " + props.getKeyPackageClass() + " will be overridden by cached class "
                    + keyPackageClass);
            props.setKeyPackageClass(keyPackageClass);
        }

        return new Graph(dataSchema, storeSchema, props);
    }
}
