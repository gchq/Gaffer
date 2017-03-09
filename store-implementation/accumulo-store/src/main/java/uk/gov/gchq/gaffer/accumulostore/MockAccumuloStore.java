/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * An {@link AccumuloStore} that uses an Accumulo {@link MockInstance} to
 * provide a {@link Connector}.
 */
public class MockAccumuloStore extends AccumuloStore {

    private static final PasswordToken PASSWORD_TOKEN = new PasswordToken(AccumuloProperties.PASSWORD);
    private MockInstance mockAccumulo = null;
    private Connector mockConnector;

    @Override
    public Connector getConnection() throws StoreException {
        try {
            mockConnector = mockAccumulo.getConnector(AccumuloProperties.USER, PASSWORD_TOKEN);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new StoreException(e.getMessage(), e);
        }
        return mockConnector;
    }

    public void initialise(final Schema schema, final StoreProperties properties)
            throws StoreException {
        if (!(properties instanceof AccumuloProperties)) {
            throw new StoreException("Store must be initialised with AccumuloProperties");
        }
        mockAccumulo = new MockInstance(((AccumuloProperties) properties).getInstance());
        super.initialise(schema, properties);
    }

    @Override
    protected void addUserToConfiguration(final Configuration conf) throws AccumuloSecurityException {
        InputConfigurator.setConnectorInfo(AccumuloInputFormat.class,
                conf,
                AccumuloProperties.USER,
                PASSWORD_TOKEN);
    }

    @Override
    protected void addZookeeperToConfiguration(final Configuration conf) {
        InputConfigurator.setMockInstance(AccumuloInputFormat.class,
                conf,
                getProperties().getInstance());
    }

    public MockInstance getMockAccumulo() {
        return mockAccumulo;
    }

    public Connector getMockConnector() {
        return mockConnector;
    }

    OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
        return super.getOperationHandler(opClass);
    }

}
