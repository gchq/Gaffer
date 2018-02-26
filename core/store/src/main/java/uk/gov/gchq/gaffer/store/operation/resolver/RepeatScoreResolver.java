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
package uk.gov.gchq.gaffer.store.operation.resolver;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.impl.Repeat;

public class RepeatScoreResolver implements ScoreResolver<Repeat> {
    @Override
    public Integer getScore(final Repeat repeat, final ScoreResolver defaultScoreResolver) {
        int score = 0;
        if (null != repeat) {
            final Operation delegate = repeat.getOperation();

            if (delegate instanceof Operations) {

                for (final Operation op : ((Operations<Operation>) delegate).getOperations()) {
                    score += defaultScoreResolver.getScore(op);
                }
            } else {
                score = defaultScoreResolver.getScore(delegate);
            }

            score *= repeat.getTimes();
        }
        return score;
    }

    @Override
    public Integer getScore(final Repeat operation) {
        throw new UnsupportedOperationException("Default score resolver has not been provided.");
    }
}
