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
package gaffer.function.simple.types;

import java.util.HashMap;
import java.util.Map;

/**
 * <code>FreqMap</code> simply extends {@link java.util.HashMap} with String keys and Integer values.
 */
public class FreqMap extends HashMap<String, Integer> {
    private static final long serialVersionUID = -6178586775831730274L;

    public FreqMap(final Map<? extends String, ? extends Integer> m) {
        super(m);
    }

    public FreqMap() {
    }

    public FreqMap(final int initialCapacity) {
        super(initialCapacity);
    }

    public FreqMap(final int initialCapacity, final float loadFactor) {
        super(initialCapacity, loadFactor);
    }
}
