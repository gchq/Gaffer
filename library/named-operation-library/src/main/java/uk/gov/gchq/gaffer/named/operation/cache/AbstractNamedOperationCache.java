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


import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.user.User;

public abstract class AbstractNamedOperationCache implements INamedOperationCache {

    /**
     * If the user is just adding to the cache, ie the overwrite flag is set to false, then no security is added.
     * However if the user is overwriting the named operation stored in the cache, then their opAuths must be checked
     * against the write roles associated with the {@link NamedOperationDetail}. If it turns out the user is overwriting a
     * non-existent NamedOperationDetail, then the users NamedOperationDetail will be added normally.
     *
     * @param namedOperation The NamedOperationDetail that the user wants to store
     * @param overwrite      Flag relating to whether the user is adding (false) or updating/overwriting (true)
     * @throws CacheOperationFailedException thrown if the user doesn't have write access to the NamedOperationDetail requested,
     *                                       or if the add operation fails for some reason.
     */
    @Override
    public void addNamedOperation(final NamedOperationDetail namedOperation, final boolean overwrite, final User user) throws CacheOperationFailedException {
        String name;
        try {
            name = namedOperation.getOperationName();
        } catch (final NullPointerException e) {
            throw new CacheOperationFailedException("NamedOperation cannot be null", e);
        }
        if (name == null) {
            throw new CacheOperationFailedException("NamedOperation name cannot be null");
        }
        if (!overwrite) {
            addToCache(name, namedOperation, false);
            return;
        }

        NamedOperationDetail existing = null;

        try {
            existing = getFromCache(name);
        } catch (final CacheOperationFailedException e) { // if there is no existing named Operation add one
            addToCache(name, namedOperation, false);
            return;
        }
        if (existing.hasWriteAccess(user)) {
            addToCache(name, namedOperation, true);
        } else {
            throw new CacheOperationFailedException("User " + namedOperation.getCreatorId() + " does not have permission to overwrite");
        }
    }

    /**
     * Checks whether a {@link User} has write access to the cache. If they do then the NamedOperationDetail and name is
     * removed from the cache. If they don't or the NamedOperationDetail doesn't exist then an Exception is thrown.
     *
     * @param name The name of the NamedOperationDetail a user would like to delete
     * @param user A {@link User} object that can optionally be used for checking permissions
     * @throws CacheOperationFailedException Thrown when the NamedOperationDetail doesn't exist or the User doesn't have
     *                                       write permission on the NamedOperationDetail.
     */
    @Override
    public void deleteNamedOperation(final String name, final User user) throws CacheOperationFailedException {
        if (name == null) {
            throw new CacheOperationFailedException("NamedOperation name cannot be null");
        }
        NamedOperationDetail existing = getFromCache(name);
        if (existing.hasWriteAccess(user)) {
            deleteFromCache(name);
        } else {
            throw new CacheOperationFailedException("User " + user +
                    " does not have authority to delete named operation: " + name);
        }
    }

    /**
     * First gets the NamedOperationDetail in question and checks whether the user has read access before returning the value.
     * If the NamedOperationDetail doesn't exist or the User doesn't have permission to read this NamedOperationDetail, then an
     * exception is thrown.
     *
     * @param name The name of the NamedOperationDetail held in the cache.
     * @param user The {@link User} object that is used for checking read permissions.
     * @return NamedOperationDetail
     * @throws CacheOperationFailedException thrown if the NamedOperationDetail doesn't exist or the User doesn't have permission
     *                                       to read it.
     */
    @Override
    public NamedOperationDetail getNamedOperation(final String name, final User user) throws CacheOperationFailedException {
        NamedOperationDetail op = getFromCache(name);
        if (op.hasReadAccess(user)) {
            return op;
        } else {
            throw new CacheOperationFailedException("User: " + user + " does not have read access to " + name);
        }
    }


    @Override
    public abstract CloseableIterable<NamedOperationDetail> getAllNamedOperations(User user);

    @Override
    public abstract void clear() throws CacheOperationFailedException;

    public abstract void deleteFromCache(String name) throws CacheOperationFailedException;

    public abstract void addToCache(String name, NamedOperationDetail operation, boolean overwrite) throws CacheOperationFailedException;

    public abstract NamedOperationDetail getFromCache(String name) throws CacheOperationFailedException;
}
