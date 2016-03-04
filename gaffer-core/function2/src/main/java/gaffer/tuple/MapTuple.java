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

package gaffer.tuple;

import java.util.HashMap;
import java.util.Iterator;

public class MapTuple extends HashMap<String, Object> implements Tuple<String> {
    private static final long serialVersionUID = -5799451579204438891L;

    public void putValue(final String reference, final Object value) {
        put(reference, value);
    }

    public Object getValue(final String reference) {
        return get(reference);
    }

    public Iterator<Object> iterator() {
        return values().iterator();
    }
}
