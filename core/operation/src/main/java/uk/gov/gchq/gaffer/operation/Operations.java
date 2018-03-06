/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An <code>Operations</code> will hold a list of {@link Operation}s, which are often dealt with recursively.
 *
 * @param <T> the type of the {@link Operation}s
 */
public interface Operations<T extends Operation> {
    /**
     * Should return a {@link Collection} of all operations contained within the {@link Operations} implementation.
     * The collection of operations may be modified by Gaffer.
     *
     * @return A {@link Collection} of {@link Operation}s.
     */
    Collection<T> getOperations();

    /**
     * Updates the operations using the provided collection.
     * By default this will just clear the current operations collection
     * and add all the new operations.
     *
     * @param operations the new operations.
     */
    default void updateOperations(final Collection<T> operations) {
        try {
            getOperations().clear();
            getOperations().addAll(operations);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to update operations from: " + getOperations() + " to " + operations, e);
        }
    }

    /**
     * The class of the operations. By default this will return the
     * {@link Operation} class.
     *
     * @return the class of the operations
     */
    @JsonIgnore
    default Class<T> getOperationsClass() {
        return (Class) Operation.class;
    }

    /**
     * Recursively flattens nested operations.
     *
     * @return a list of flattened operations.
     */
    default List<Operation> flatten() {
        final List<Operation> tmp = new ArrayList<>(1);

        for (final Operation operation : getOperations()) {
            if (operation instanceof Operations) {
                tmp.addAll(((Operations) operation).flatten());
            } else {
                tmp.add(operation);
            }
        }

        return tmp;
    }
}
