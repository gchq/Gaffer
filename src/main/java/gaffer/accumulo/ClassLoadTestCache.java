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
package gaffer.accumulo;

import gaffer.Pair;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache of the results of testing class loading - used for testing whether functions that a user wants
 * to give to {@link AccumuloBackedGraph} to give to an iterator can be loaded.
 */
public class ClassLoadTestCache {

    // Map from instance id to (className, asTypeName) pair to boolean indicating whether it can
    // successfully be loaded using instanceOperations().testClassLoad().
    private static ConcurrentMap<String, Map<Pair<String>, Boolean>> instanceMap = new ConcurrentHashMap<String, Map<Pair<String>, Boolean>>();
    // Map from (instance id, table name) pair to (className, asTypeName) pair to boolean indicating
    // whether it can successfully be loaded using tableOperations().testClassLoad().
    private static ConcurrentMap<Pair<String>, Map<Pair<String>, Boolean>> tableMap = new ConcurrentHashMap<Pair<String>, Map<Pair<String>, Boolean>>();

    private ClassLoadTestCache() { }

    /**
     * Checks whether the class with the provided className can be loaded as the given type.
     *
     * @param connector  A connector to an Accumulo instance
     * @param tableName  The name of the table
     * @param className  The name of the class which we want to test whether it can be loaded
     * @param asTypeName  The type of class we want to load the class as
     * @return <code>true</code> if the class can be loaded
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public static synchronized boolean checkLoad(Connector connector, String tableName, String className, String asTypeName)
            throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        try {
            String instanceId = connector.getInstance().getInstanceID();
            Pair<String> classTypePair = new Pair<String>(className, asTypeName);
            boolean onInstanceClasspath = checkInstanceLoad(connector, instanceId, classTypePair);
            if (onInstanceClasspath) {
                return true;
            }
            return checkTableLoad(connector, instanceId, tableName, classTypePair);
        } catch (ClassCastException e) {
            return false;
        }
    }

    private static boolean checkInstanceLoad(Connector connector, String instanceId, Pair<String> classTypePair)
            throws AccumuloSecurityException, AccumuloException, ClassCastException {
        if (!instanceMap.containsKey(instanceId)) {
            Map<Pair<String>, Boolean> map = new HashMap<Pair<String>, Boolean>();
            instanceMap.put(instanceId, map);
        }
        if (!instanceMap.get(instanceId).containsKey(classTypePair)) {
            boolean onInstanceClasspath = connector.instanceOperations().testClassLoad(classTypePair.getFirst(), classTypePair.getSecond());
            instanceMap.get(instanceId).put(classTypePair, onInstanceClasspath);
        }
        return instanceMap.get(instanceId).get(classTypePair);
    }

    private static boolean checkTableLoad(Connector connector, String instanceId, String tableName, Pair<String> classTypePair)
            throws AccumuloSecurityException, AccumuloException, TableNotFoundException, ClassCastException {
        Pair<String> instanceTablePair = new Pair<String>(instanceId, tableName);
        if (!tableMap.containsKey(instanceTablePair)) {
            Map<Pair<String>, Boolean> map = new HashMap<Pair<String>, Boolean>();
            tableMap.put(instanceTablePair, map);
        }
        if (!tableMap.get(instanceTablePair).containsKey(classTypePair)) {
            boolean onTableClasspath = connector.tableOperations().testClassLoad(tableName, classTypePair.getFirst(), classTypePair.getSecond());
            tableMap.get(instanceTablePair).put(classTypePair, onTableClasspath);

        }
        return tableMap.get(instanceTablePair).get(classTypePair);
    }

}
