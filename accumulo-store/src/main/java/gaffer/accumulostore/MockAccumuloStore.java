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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import gaffer.store.StoreException;

/**
 * An {@link AccumuloStore} that uses an Accumulo {@link MockInstance} to
 * provide a {@link Connector}.
 */
public class MockAccumuloStore extends AccumuloStore {
    private final MockInstance mockAccumulo = new MockInstance();
    private Connector mockConnector;

    @Override
    public Connector getConnection() throws StoreException {
        try {
            mockConnector = mockAccumulo.getConnector("user", new PasswordToken("password"));
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new StoreException(e.getMessage(), e);
        }

        return mockConnector;
    }

    public MockInstance getMockAccumulo() {
        return mockAccumulo;
    }

    public Connector getMockConnector() {
        return mockConnector;
    }
}
