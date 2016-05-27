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

public abstract class Exporter {
    private User user;
    private Long timestamp = System.currentTimeMillis();

    public boolean initialise(final Object config, final User user) {
        final boolean isNew = null == this.user || !this.user.equals(user);
        if (isNew) {
            this.user = user;
        }

        return isNew;
    }

    public final void add(final String key, final Iterable<Object> values, final User user) {
        validateSameUser(user);

        _add(key, values, user);
    }

    public final CloseableIterable<Object> get(final String key, final User user, final int start, final int end) {
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
            throw new IllegalArgumentException("A user is required");
        }

        return user.getUserId() + "_" + timestamp;
    }

    protected abstract void _add(final String key, final Iterable<Object> values, final User user);

    protected abstract CloseableIterable<Object> _get(final String key, final User user, final int start, final int end);


    protected String getUserExportKey(final String key) {
        return getUserTimestampedExportName() + "_" + key;
    }

    private void validateSameUser(final User user) {
        if (null == this.user || !this.user.equals(user)) {
            throw new IllegalArgumentException("User's cannot be changed between operations in a chain");
        }
    }
}
