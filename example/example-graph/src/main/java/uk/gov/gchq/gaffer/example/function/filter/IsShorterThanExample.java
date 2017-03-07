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
package uk.gov.gchq.gaffer.example.function.filter;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.function.filter.IsShorterThan;
import java.util.HashMap;
import java.util.Map;

public class IsShorterThanExample extends FilterFunctionExample {
    private final Map<String, String> map = new HashMap<>();
    final Map<String, String> bigMap = new HashMap<>(map);

    public static void main(final String[] args) {
        new IsShorterThanExample().run();
    }

    public IsShorterThanExample() {
        super(IsShorterThan.class);
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        bigMap.put("4", "d");
    }

    public void runExamples() {
        isShorterThan4();
    }

    public void isShorterThan4() {
        // ---------------------------------------------------------
        final IsShorterThan function = new IsShorterThan(4);
        // ---------------------------------------------------------

        runExample(function,
                "123", "1234",
                new Integer[]{1, 2, 3}, new Integer[]{1, 2, 3, 4},
                Lists.newArrayList(1, 2, 3), Lists.newArrayList(1, 2, 3, 4),
                map, bigMap,
                10000, 10000L);
    }
}
