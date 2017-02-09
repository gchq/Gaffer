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

package uk.gov.gchq.gaffer.named.operation.cache;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.named.operation.ExtendedNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.user.User;

/**
 * A generic wrapper for a cache object that adds Named operations to it. This should be implemented and injected into
 * the Handlers:
 * {@link uk.gov.gchq.gaffer.named.operation.handler.GetAllNamedOperationsHandler}
 * {@link uk.gov.gchq.gaffer.named.operation.handler.AddNamedOperationHandler},
 * {@link uk.gov.gchq.gaffer.named.operation.handler.DeleteNamedOperationHandler},
 * {@link uk.gov.gchq.gaffer.named.operation.handler.NamedOperationHandler}
 * The cache is then assigned through the OperationsDeclarations.json file. There is an example in the resources folder.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface INamedOperationCache {
    /**
     * Adds or updates, depending on the overwrite flag, a {@link ExtendedNamedOperation} to a cache. This should be implemented
     * With appropriate methods, depending on the type of cache used and access restrictions applied.
     *
     * @param operation The ExtendedNamedOperation to store.
     * @param overWrite Flag relating to whether to add or overwrite a ExtendedNamedOperation.
     * @param user      The user calling the operation. If overwrite is set, it should be used for checking permissions. Otherwise, it can be set to null.
     * @throws CacheOperationFailedException Exception thrown when the ExtendedNamedOperation is not added.
     */
    void addNamedOperation(final ExtendedNamedOperation operation, final boolean overWrite, final User user) throws CacheOperationFailedException;

    /**
     * Deletes a ExtendedNamedOperation from the cache.
     *
     * @param name The name of the ExtendedNamedOperation a user would like to delete
     * @param user A {@link User} object that can optionally be used for checking permissions
     * @throws CacheOperationFailedException exception thrown when the ExtendedNamedOperation could not be deleted
     */
    void deleteNamedOperation(final String name, final User user) throws CacheOperationFailedException;

    /**
     * Gets a ExtendedNamedOperation from the cache.
     *
     * @param name The name of the ExtendedNamedOperation held in the cache.
     * @param user The {@link User} object that can optionally be used for checking permissions.
     * @return A ExtendedNamedOperation corresponding to the name given if one exists in the Cache.
     * @throws CacheOperationFailedException exception thrown when the ExtendedNamedOperation couldn't be retrieved.
     */
    ExtendedNamedOperation getNamedOperation(final String name, final User user) throws CacheOperationFailedException;

    /**
     * Gets all the NamedOperations that a user has the permission to execute.
     *
     * @param user   The user making the request
     * @param simple flag relating to whether to return full NamedOperation details or just name and description.
     *               Ordinarily this should be set to true
     * @return a closeable iterable of NamedOperations that a given user can execute
     */
    CloseableIterable<NamedOperation> getAllNamedOperations(final User user, final boolean simple);

    /**
     * Clears all keys and values from the Cache
     *
     * @throws CacheOperationFailedException Exception thrown if the cache fails to clear
     */
    void clear() throws CacheOperationFailedException;
}
