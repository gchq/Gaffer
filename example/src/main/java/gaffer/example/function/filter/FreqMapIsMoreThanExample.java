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

import gaffer.function.simple.filter.FreqMapIsMoreThan;
import gaffer.types.simple.FreqMap;

public class FreqMapIsMoreThanExample extends FilterFunctionExample {
    public static void main(final String[] args) {
        new FreqMapIsMoreThanExample().run();
    }

    public FreqMapIsMoreThanExample() {
        super(FreqMapIsMoreThan.class);
    }

    public void runExamples() {
        freqMapIsMoreThan2();
        freqMapIsMoreThanOrEqualTo2();
    }

    public void freqMapIsMoreThan2() {
        final FreqMap map1 = new FreqMap();
        map1.put("key1", 1);

        final FreqMap map2 = new FreqMap();
        map2.put("key1", 2);

        final FreqMap map3 = new FreqMap();
        map3.put("key1", 3);

        final FreqMap map4 = new FreqMap();
        map4.put("key1", 3);
        map4.put("key2", 0);

        final FreqMap map5 = new FreqMap();
        map5.put("key2", 3);

        runExample(new FreqMapIsMoreThan("key1", 2),
                "new FreqMapIsMoreThan(\"key1\", 2)",
                map1, map2, map3, map4, map5);
    }

    public void freqMapIsMoreThanOrEqualTo2() {
        final FreqMap map1 = new FreqMap();
        map1.put("key1", 1);

        final FreqMap map2 = new FreqMap();
        map2.put("key1", 2);

        final FreqMap map3 = new FreqMap();
        map3.put("key1", 3);

        final FreqMap map4 = new FreqMap();
        map4.put("key1", 3);
        map4.put("key2", 0);

        final FreqMap map5 = new FreqMap();
        map5.put("key2", 3);

        runExample(new FreqMapIsMoreThan("key1", 2, true),
                "new FreqMapIsMoreThan(\"key1\", 2)",
                map1, map2, map3, map4, map5);
    }
}
