/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import uk.gov.gchq.gaffer.operation.Operation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdditionalOperations {
    private List<Operation> start;
    private List<Operation> end;
    private Map<String, List<Operation>> before;
    private Map<String, List<Operation>> after;

    public AdditionalOperations() {
        start = new ArrayList<>();
        end = new ArrayList<>();
        before = new HashMap<>();
        after = new HashMap<>();
    }

    public List<Operation> getStart() {
        return start;
    }

    public void setStart(final List<Operation> start) {
        if (start == null) {
            this.start = new ArrayList<>();
            return;
        }
        this.start = start;
    }

    public List<Operation> getEnd() {
        return end;
    }

    public void setEnd(final List<Operation> end) {
        if (end == null) {
            this.end = new ArrayList<>();
            return;
        }
        this.end = end;
    }

    public Map<String, List<Operation>> getBefore() {
        return before;
    }

    public void setBefore(final Map<String, List<Operation>> before) {
        if (before == null) {
            this.before = new HashMap<>();
            return;
        }
        this.before = before;
    }

    public Map<String, List<Operation>> getAfter() {
        return after;
    }

    public void setAfter(final Map<String, List<Operation>> after) {
        if (after == null) {
            this.after = new HashMap<>();
            return;
        }
        this.after = after;
    }
}
