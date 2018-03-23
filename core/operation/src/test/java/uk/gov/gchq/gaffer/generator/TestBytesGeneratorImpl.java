/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.generator;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;

public class TestBytesGeneratorImpl implements OneToManyElementGenerator<byte[]> {
    private TestGeneratorImpl delegate = new TestGeneratorImpl();

    @Override
    public Iterable<Element> _apply(final byte[] domainObject) {
        return delegate._apply(null != domainObject ? StringUtil.toString(domainObject) : null);
    }
}
