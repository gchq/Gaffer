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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * An {@code OperationAuthoriser} is a {@link GraphHook} that checks a
 * user is authorised to execute an operation chain. This class requires a map
 * of operation authorisations.
 */
public class OperationAuthoriser implements GraphHook {
    private final Set<String> allAuths = new HashSet<>();
    private final Map<Class<?>, Set<String>> auths = new HashMap<>();

    /**
     * Checks the {@link Operation}s in the provided {@link OperationChain}
     * are allowed to be executed by the user.
     * This is done by checking the user's auths against the operation auths.
     * If an operation cannot be executed then an {@link IllegalAccessError} is thrown.
     *
     * @param context  the user to authorise.
     * @param opChain the operation chain.
     */
    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (null != opChain) {
            for (final Operation operation : opChain.getOperations()) {
                authorise(operation, context.getUser());
            }
            authorise(opChain, context.getUser());
        }
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        // This method can be overridden to add additional authorisation checks on the results.
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    /**
     * Add operation authorisations for a given operation class.
     * This can be called multiple times for the same operation class and the
     * authorisations will be appended.
     *
     * @param opClass the operation class
     * @param auths   the authorisations
     */
    public void addAuths(final Class<? extends Operation> opClass, final String... auths) {
        final Set<String> opAuths = this.auths.computeIfAbsent(opClass, k -> new HashSet<>());
        Collections.addAll(opAuths, auths);
        Collections.addAll(allAuths, auths);
    }

    public Map<Class<?>, Set<String>> getAuths() {
        return Collections.unmodifiableMap(auths);
    }

    public void setAuths(final Map<Class<?>, Set<String>> auths) {
        this.auths.clear();
        this.allAuths.clear();
        if (null != auths) {
            this.auths.putAll(auths);
            for (final Set<String> authsSet : auths.values()) {
                allAuths.addAll(authsSet);
            }
        }
    }

    @JsonGetter("auths")
    public Map<String, Set<String>> getAuthsAsStrings() {
        final Map<String, Set<String>> authsAsStrings = CollectionUtil.toMapWithStringKeys(auths);
        return Collections.unmodifiableMap(authsAsStrings);
    }

    @JsonSetter("auths")
    public void setAuthsFromStrings(final Map<String, Set<String>> auths) throws ClassNotFoundException {
        setAuths(CollectionUtil.toMapWithClassKeys(auths));
    }

    @JsonIgnore
    public Set<String> getAllAuths() {
        return Collections.unmodifiableSet(allAuths);
    }

    protected void authorise(final Operation operation, final User user) {
        if (null != operation) {
            if (operation instanceof OperationChain) {
                final List<Operation> operations = ((OperationChain) operation).getOperations();
                operations.forEach(op -> authorise(op, user));
            }

            final Class<? extends Operation> opClass = operation.getClass();
            final Set<String> userOpAuths = user.getOpAuths();
            boolean authorised = true;
            for (final Entry<Class<?>, Set<String>> entry : auths.entrySet()) {
                if ((entry.getKey().isAssignableFrom(opClass))
                        && (!userOpAuths.containsAll(entry.getValue()))) {
                    authorised = false;
                    break;
                }
            }

            if (!authorised) {
                throw new UnauthorisedException("User does not have permission to run operation: "
                        + operation.getClass().getName());
            }
        }
    }
}
