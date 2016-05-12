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

package gaffer.authoriser;

import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.user.User;
import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * A <code>OperationAuthoriser</code> contains the operation authorisation properties.
 */
public class OperationAuthoriser {
    private Map<Class<? extends Operation>, Set<String>> opAuthsMap = new HashMap<>();

    public OperationAuthoriser() {
    }

    public OperationAuthoriser(final Path propFileLocation) {
        this(readProperties(propFileLocation));
    }

    public OperationAuthoriser(final InputStream stream) {
        this(readProperties(stream));
    }

    public OperationAuthoriser(final Properties props) {
        loadOpAuthMap(props);
    }

    /**
     * Checks the {@link Operation}s in the provided {@link OperationChain}
     * are allowed to be executed by the user.
     * This is done by checking the user's auths against the operation auths.
     * If an operation cannot be executed then an {@link IllegalAccessError} is thrown.
     *
     * @param user    the user to authorise.
     * @param opChain the operation chain.
     */
    public void authorise(final OperationChain<?> opChain, final User user) {
        if (null != opChain) {
            for (Operation operation : opChain.getOperations()) {
                authorise(operation, user);
            }
        }
    }

    public void authorise(final Operation operation, final User user) {
        if (null != operation) {
            final Class<? extends Operation> opClass = operation.getClass();
            final Set<String> userOpAuths = user.getOpAuths();
            boolean authorised = true;
            for (Entry<Class<? extends Operation>, Set<String>> entry : opAuthsMap.entrySet()) {
                if (entry.getKey().isAssignableFrom(opClass)) {
                    if (!userOpAuths.containsAll(entry.getValue())) {
                        authorised = false;
                        break;
                    }
                }
            }

            if (!authorised) {
                throw new IllegalAccessError("User does not have permission to run operation: "
                        + operation.getClass().getName());
            }
        }
    }

    public void authoriseResult(final Object result, final User user) {
        // This method can be overridden to add additional authorisation checks on the results.
    }

    /**
     * Add operation authorisations
     *
     * @param opClass the operation class
     * @param auths   the authorisations
     */
    public void setOpAuths(final Class<? extends Operation> opClass, final Set<String> auths) {
        opAuthsMap.put(opClass, auths);
    }

    /**
     * Add operation authorisations
     *
     * @param opClass the operation class
     * @param auths   the authorisations
     */
    public void addOpAuths(final Class<? extends Operation> opClass, final String... auths) {
        Set<String> opAuths = opAuthsMap.get(opClass);
        if (null == opAuths) {
            opAuths = new HashSet<>();
            opAuthsMap.put(opClass, opAuths);
        }
        Collections.addAll(opAuths, auths);
    }

    private void loadOpAuthMap(final Properties props) {
        for (String opClassName : props.stringPropertyNames()) {
            try {
                final Class<? extends Operation> opClass = Class.forName(opClassName).asSubclass(Operation.class);
                final Set<String> auths = new HashSet<>();
                for (String auth : props.getProperty(opClassName).split(",")) {
                    if (null != auth && !"".equals(auth)) {
                        auths.add(auth);
                    }
                }
                opAuthsMap.put(opClass, auths);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Properties readProperties(final Path propFileLocation) {
        Properties props;
        if (null != propFileLocation) {
            try {
                props = readProperties(Files.newInputStream(propFileLocation, StandardOpenOption.READ));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            props = new Properties();
        }

        return props;
    }

    private static Properties readProperties(final InputStream stream) {
        final Properties props = new Properties();
        if (null != stream) {
            try {
                props.load(stream);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        }

        return props;
    }
}
