/*
 * Copyright 2022-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.util;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class MergeElementFunctionWithGivenStore extends MergeElementFunction {


    public static final String TEAR_DOWN_TEMP_GRAPH = "tearDownTempGraph";

    @Override
    @JsonIgnore
    public Set<String> getRequiredContextValues() {
        final HashSet<String> set = new HashSet<>(super.getRequiredContextValues());
        set.add(TEMP_RESULTS_GRAPH);
        set.add(TEAR_DOWN_TEMP_GRAPH);
        return Collections.unmodifiableSet(set);
    }

}
