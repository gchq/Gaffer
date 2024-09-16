/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

/**
 * A {@code CountGroupsHandler} handles {@link CountGroups} operations.
 */
public class CountGroupsHandler implements OutputOperationHandler<CountGroups, GroupCounts> {
    @Override
    @SuppressWarnings("PMD.UseTryWithResources") //Not worthwhile
    public GroupCounts doOperation(final CountGroups operation,
                                   final Context context, final Store store)
            throws OperationException {
        final GroupCounts groupCounts = new GroupCounts();
        try {
            int count = 0;
            if (null != operation.getInput()) {
                for (final Element element : operation.getInput()) {
                    if (null != operation.getLimit()) {
                        count++;
                        if (count > operation.getLimit()) {
                            groupCounts.setLimitHit(true);
                            break;
                        }
                    }

                    if (element instanceof Entity) {
                        groupCounts.addEntityGroup(element.getGroup());
                    } else {
                        groupCounts.addEdgeGroup(element.getGroup());
                    }
                }
            }
        } finally {
            CloseableUtil.close(operation);
        }

        return groupCounts;
    }
}
