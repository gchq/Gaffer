/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.accumulo.core.iterators.OptionDescriber;

public class IteratorUtils {
    public static OptionDescriber.IteratorOptions describeOptions(final String name, final String description, final OptionDescriber.IteratorOptions superOptions) {
        final Map<String, String> namedOptions = new HashMap<>();
        for (Map.Entry<String, String> entry : superOptions.getNamedOptions().entrySet()) {
            namedOptions.put(entry.getKey(), entry.getValue());
        }
        return new OptionDescriber.IteratorOptions(name, description, namedOptions, null);
    }
}