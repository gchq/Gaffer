/*
 * Copyright 2018-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.join.match;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.koryphe.serialisation.json.JsonSimpleClassName;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
@JsonSimpleClassName(includeSubtypes = true)
public interface Match {

    /**
     * Initialises the match and stores the match candidates.
     * @param matchCandidates candidate pool to check for matches
     */
    void init(final Iterable matchCandidates);

    /**
     * Compares a list of Objects against a test Object and returns matched Objects.
     *
     * @param testObject Object to test against.
     * @return List containing matched Objects.
     */
    List matching(final Object testObject);
}
