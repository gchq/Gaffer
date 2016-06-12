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
package gaffer.store;

import gaffer.user.User;
import java.util.HashMap;
import java.util.Map;

/**
 * A <code>Context</code> contains operation chain execution information, such
 * as the user who executed the operation chain and an operation chain cache.
 */
public class Context {
    private final User user;
    private final Map<String, Iterable<?>> cache = new HashMap<>();

    public Context() {
        this(new User());
    }

    public Context(final User user) {
        this.user = user;
    }

    public User getUser() {
        return user;
    }

    public Map<String, Iterable<?>> getCache() {
        return cache;
    }
}
