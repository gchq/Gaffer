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

package gaffer.export;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.user.User;

/**
 * An <code>Exporter</code> can store data of any kind and retrieve it with
 * pagination.
 */
public abstract class Exporter {
    public static final String KEY_SEPARATOR = "_";
    private User user;
    private Long timestamp = System.currentTimeMillis();

    /**
     * Initialises the export. This base method just stores the current user.
     * Override this method (and call super) to add further initialisation.
     *
     * @param config configuration for the export (This will be an instance of a gaffer Store)
     * @param user   the user who initiated the export
     */
    public void initialise(final Object config, final User user) {
        this.user = user;
    }

    public final void add(final String key, final Iterable<?> values, final User user) {
        validateSameUser(user);
        _add(key, values, user);
    }

    public final CloseableIterable<?> get(final String key, final User user, final int start, final int end) {
        validateSameUser(user);
        return _get(key, user, start, end);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public String getUserTimestampedExportName() {
        if (null == user) {
            throw new IllegalArgumentException("Exporter must be initialised with a user");
        }

        return user.getUserId() + KEY_SEPARATOR + timestamp;
    }

    protected abstract void _add(final String key, final Iterable<?> values, final User user);

    protected abstract CloseableIterable<?> _get(final String key, final User user, final int start, final int end);


    protected String getUserExportKey(final String key) {
        return getUserTimestampedExportName() + KEY_SEPARATOR + key;
    }

    private void validateSameUser(final User user) {
        if (null == this.user) {
            throw new IllegalArgumentException("This exporter cannot be used until it has been initialised by a user.");
        } else if (null == user) {
            throw new IllegalArgumentException("A user is required to use this Exporter.");
        } else if (!this.user.equals(user)) {
            throw new IllegalArgumentException("This Exporter has been initialised by user " + this.user.getUserId() + ". It cannot be used a different user, " + user.getUserId() + ".");
        }
    }
}
