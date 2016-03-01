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
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.commonutil.StreamUtil;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.store.StoreException;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public class MockAccumuloStoreForTest extends MockAccumuloStore {
    private static final MockInstance BYTE_ENTITY_MOCK_INSTANCE = new MockInstance();
    private static final MockInstance CLASSIC_MOCK_INSTANCE = new MockInstance();

    private Connector byteEntityMockConnector;
    private Connector classicMockConnector;

    public MockAccumuloStoreForTest() {
        this(ByteEntityKeyPackage.class);
    }

    public MockAccumuloStoreForTest(final Class<? extends AbstractCoreKeyPackage> keyPackageClass) {
        final DataSchema dataSchema = DataSchema.fromJson(StreamUtil.dataSchema(getClass()));
        final StoreSchema storeSchema = StoreSchema.fromJson(StreamUtil.storeSchema(getClass()));
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        properties.setKeyPackageClass(keyPackageClass.getName());

        try {
            initialise(dataSchema, storeSchema, properties);
        } catch (StoreException e) {
            throw new RuntimeException(e);
        }

        clearTables();
    }

    @Override
    public Connector getConnection() throws StoreException {
        try {
            if (ByteEntityKeyPackage.class.getName().equals(getProperties().getKeyPackageClass())) {
                if (null == byteEntityMockConnector) {
                    byteEntityMockConnector = BYTE_ENTITY_MOCK_INSTANCE.getConnector("user", new PasswordToken("password"));
                }
                return byteEntityMockConnector;
            }
            if (ClassicKeyPackage.class.getName().equals(getProperties().getKeyPackageClass())) {
                if (null == classicMockConnector) {
                    classicMockConnector = CLASSIC_MOCK_INSTANCE.getConnector("user", new PasswordToken("password"));
                }
                return classicMockConnector;
            }
            throw new StoreException("Invalid key package class");
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    @Override
    public Connector getMockConnector() {
        if (ByteEntityKeyPackage.class.getName().equals(getProperties().getKeyPackageClass())) {
            return byteEntityMockConnector;
        }
        if (ClassicKeyPackage.class.getName().equals(getProperties().getKeyPackageClass())) {
            return classicMockConnector;
        }

        throw new RuntimeException("Invalid key package class");
    }

    private void clearTables() {
        try {
            final Connector connection = getConnection();
            for (String tableName : connection.tableOperations().list()) {
                connection.tableOperations().delete(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
