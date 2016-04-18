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

import gaffer.accumulostore.key.core.AbstractCoreKeyPackage;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.commonutil.StreamUtil;
import gaffer.store.StoreException;
import gaffer.store.schema.Schema;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

public class SingletonLocalMiniAccumuloStoreForTest extends LocalMiniAccumuloStore {
    private static MiniAccumuloCluster singletonMiniAccumuloCluster;

    public SingletonLocalMiniAccumuloStoreForTest() {
        this(ByteEntityKeyPackage.class);
    }

    public SingletonLocalMiniAccumuloStoreForTest(final Class<? extends AbstractCoreKeyPackage> keyPackageClass) {
        final Schema schema = Schema.fromJson(
                StreamUtil.dataSchema(getClass()),
                StreamUtil.dataTypes(getClass()),
                StreamUtil.storeSchema(getClass()),
                StreamUtil.storeTypes(getClass()));

        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        properties.setKeyPackageClass(keyPackageClass.getName());

        try {
            initialise(schema, properties);
        } catch (StoreException e) {
            throw new RuntimeException(e);
        }

        clearTables();
    }

    @Override
    protected MiniAccumuloCluster createMiniCluster(final AccumuloProperties accProps) {
        if (null == singletonMiniAccumuloCluster) {
            singletonMiniAccumuloCluster = super.createMiniCluster(accProps);
        }
        return singletonMiniAccumuloCluster;
    }

    private void clearTables() {
        try {
            final Connector connection = getConnection();
            for (String tableName : connection.tableOperations().list()) {
                try {
                    connection.tableOperations().delete(tableName);
                } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e1) {
                    // swallow exceptions!
                }
            }
        } catch (Exception e) {
            // swallow exceptions!
        }
    }
}
