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
package gaffer.example.function.filter;

import com.google.common.collect.Lists;
import gaffer.function.simple.filter.IsShorterThan;
import java.util.HashMap;
import java.util.Map;

public class IsShorterThanExample extends FilterFunctionExample {
    public static void main(final String[] args) {
        new IsShorterThanExample().run();
    }

    public IsShorterThanExample() {
        super(IsShorterThan.class);
    }

    public void runExamples() {
        isShorterThan4();
    }

    public void isShorterThan4() {
        final Map<String, String> map = new HashMap<>();
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        final Map<String, String> bigMap = new HashMap<>(map);
        bigMap.put("4", "d");

        runExample(new IsShorterThan(4),
                "new IsShorterThan(4)",
                "123", "1234",
                new Integer[]{1, 2, 3}, new Integer[]{1, 2, 3, 4},
                Lists.newArrayList(1, 2, 3), Lists.newArrayList(1, 2, 3, 4),
                map, bigMap,
                10000, 10000L);
    }
}
