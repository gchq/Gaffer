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

package uk.gov.gchq.gaffer.graph.hook;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;
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
 * An <code>OperationAuthoriser</code> is a {@link GraphHook} that checks a
 * user is authorised to execute an operation chain. This class requires a map
 * of operation authorisations, these can be added using setOpAuths(Class, Set) or
 * addOpAuths(Class,String...). Alternatively a properties file can be provided
 * containing the operations and the required authorisations.
 */
public class OperationAuthoriser implements GraphHook {
    public static final String AUTH_SEPARATOR = ",";

    private final Map<Class<? extends Operation>, Set<String>> opAuthsMap = new HashMap<>();
    private final Set<String> allOpAuths = new HashSet<>();

    /**
     * Default constructor. Use setOpAuths(Class, Set) or
     * addOpAuths(Class,String...) to add operation authorisations.
     */
    public OperationAuthoriser() {
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the property file from the {@link Path} provided.
     *
     * @param propFileLocation path to authorisations property file
     */
    public OperationAuthoriser(final Path propFileLocation) {
        this(readProperties(propFileLocation));
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the property file from the {@link InputStream} provided.
     *
     * @param stream input stream of authorisations property file
     */
    public OperationAuthoriser(final InputStream stream) {
        this(readProperties(stream));
    }

    /**
     * Constructs an {@link OperationAuthoriser} with the authorisations
     * defined in the provided authorisations property file.
     *
     * @param props authorisations property file
     */
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
    @Override
    public void preExecute(final OperationChain<?> opChain, final User user) {
        if (null != opChain) {
            for (final Operation operation : opChain.getOperations()) {
                authorise(operation, user);
            }
        }
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final User user) {
        // This method can be overridden to add additional authorisation checks on the results.
        return result;
    }

    /**
     * Set operation authorisations for a given operation class.
     *
     * @param opClass the operation class
     * @param auths   the authorisations
     */
    public void setOpAuths(final Class<? extends Operation> opClass, final Set<String> auths) {
        opAuthsMap.put(opClass, auths);
        allOpAuths.addAll(auths);
    }

    /**
     * Add operation authorisations for a given operation class.
     * This can be called multiple times for the same operation class and the
     * authorisations will be appended.
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
        Collections.addAll(allOpAuths, auths);
    }

    public Set<String> getAllOpAuths() {
        return Collections.unmodifiableSet(allOpAuths);
    }

    protected void authorise(final Operation operation, final User user) {
        if (null != operation) {
            final Class<? extends Operation> opClass = operation.getClass();
            final Set<String> userOpAuths = user.getOpAuths();
            boolean authorised = true;
            for (final Entry<Class<? extends Operation>, Set<String>> entry : opAuthsMap.entrySet()) {
                if (entry.getKey().isAssignableFrom(opClass)) {
                    if (!userOpAuths.containsAll(entry.getValue())) {
                        authorised = false;
                        break;
                    }
                }
            }

            if (!authorised) {
                throw new UnauthorisedException("User does not have permission to run operation: "
                        + operation.getClass().getName());
            }
        }
    }


    private void loadOpAuthMap(final Properties props) {
        for (final String opClassName : props.stringPropertyNames()) {
            final Class<? extends Operation> opClass;
            try {
                opClass = Class.forName(opClassName).asSubclass(Operation.class);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
            final Set<String> auths = new HashSet<>();
            for (final String auth : props.getProperty(opClassName).split(AUTH_SEPARATOR)) {
                if (!StringUtils.isEmpty(auth)) {
                    auths.add(auth);
                }
            }
            setOpAuths(opClass, auths);
        }
    }

    private static Properties readProperties(final Path propFileLocation) {
        Properties props;
        if (null != propFileLocation) {
            try {
                props = readProperties(Files.newInputStream(propFileLocation, StandardOpenOption.READ));
            } catch (final IOException e) {
                throw new IllegalArgumentException(e);
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
            } catch (final IOException e) {
                throw new IllegalArgumentException("Failed to load store properties file : " + e.getMessage(), e);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        }

        return props;
    }
}
