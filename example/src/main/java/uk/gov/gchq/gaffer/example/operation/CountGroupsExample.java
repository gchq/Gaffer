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
package uk.gov.gchq.gaffer.example.operation;

import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

public class CountGroupsExample extends OperationExample {
    public static void main(final String[] args) throws OperationException {
        new CountGroupsExample().run();
    }

    public CountGroupsExample() {
        super(CountGroups.class);
    }

    @Override
    public void runExamples() {
        countAllElementGroups();
        countAllElementGroupsWithLimit();
    }

    public GroupCounts countAllElementGroups() {
        // ---------------------------------------------------------
        final OperationChain<GroupCounts> opChain = new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new CountGroups())
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }

    public GroupCounts countAllElementGroupsWithLimit() {
        // ---------------------------------------------------------
        final OperationChain<GroupCounts> opChain = new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new CountGroups(5))
                .build();
        // ---------------------------------------------------------

        return runExample(opChain);
    }
}
