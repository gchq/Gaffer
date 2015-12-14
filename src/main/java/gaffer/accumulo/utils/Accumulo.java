/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.accumulo.utils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;

/**
 * Convenience class for connecting to an Accumulo instance and reduce the
 * required boilerplate in clients.
 */
public class Accumulo {

    /**
     * Connect to an Accumulo instance using configuration supplied in an
     * {@link AccumuloConfig} object.
     * 
     * @param conf  The Accumulo configuration to use
     * @return A Connector to the specified instance of Accumulo
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public static Connector connect(AccumuloConfig conf) throws AccumuloException, AccumuloSecurityException {
        String instance = conf.getInstanceName();
        String zookeepers = conf.getZookeepers();
        String user = conf.getUserName();
        String password = conf.getPassword();

        if (instance == null) {
            throw new RuntimeException("No instance name specified in configuration");
        }
        if (zookeepers == null) {
            throw new RuntimeException("No zookeepers specified in configuration");
        }
        if (user == null) {
            throw new RuntimeException("No username specified in configuration");
        }
        if (password == null) {
            throw new RuntimeException("No password specified in configuration");
        }

        if (!zookeepers.equals(AccumuloConfig.MOCK_ZOOKEEPERS)) {
            Instance inst = new ZooKeeperInstance(instance, zookeepers);
            return inst.getConnector(user, password);
        }
        Instance inst = new MockInstance(instance);
        return inst.getConnector(user, password);
    }

}
