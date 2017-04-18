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
package uk.gov.gchq.gaffer.commonutil.pair;

public class MutablePair<F, S> extends AbstractPair<F, S> {
    private static final long serialVersionUID = 6533436107353534068L;

    public MutablePair() {
    }

    public MutablePair(final F first) {
        super(first);
    }

    public MutablePair(final F first, final S second) {
        super(first, second);
    }

    public void setFirst(final F first) {
        this.first = first;
    }

    public void setSecond(final S second) {
        this.second = second;
    }
}
