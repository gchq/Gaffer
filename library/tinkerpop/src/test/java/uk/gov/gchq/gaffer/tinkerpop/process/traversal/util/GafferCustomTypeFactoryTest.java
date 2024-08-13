/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.util;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.COMPLEX_TSTV_ID;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.COMPLEX_TSTV_ID_STRING;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_ID;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_ID_STRING;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_PROPERTY_SET;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_PROPERTY_SET_STRING;

class GafferCustomTypeFactoryTest {

    @Test
    void shouldConvertTypeSubTypeValues() {
        // When
        Object stringParsed = GafferCustomTypeFactory.parseAsCustomTypeIfValid(TSTV_ID_STRING);
        Object tstvParsed = GafferCustomTypeFactory.parseForGraphSONv3(TSTV_ID);

        // Then
        assertThat(stringParsed)
                .isInstanceOf(TypeSubTypeValue.class)
                .extracting(r -> (TypeSubTypeValue) r)
                .isEqualTo(TSTV_ID);

        assertThat(tstvParsed)
                .isInstanceOf(String.class)
                .extracting(r -> (String) r)
                .isEqualTo(TSTV_ID_STRING);
    }

    @Test
    void shouldConvertComplexTypeSubTypeValues() {
        // When
        Object stringParsed = GafferCustomTypeFactory.parseAsCustomTypeIfValid(COMPLEX_TSTV_ID_STRING);
        Object tstvParsed = GafferCustomTypeFactory.parseForGraphSONv3(COMPLEX_TSTV_ID);

        // Then
        assertThat(stringParsed)
                .isInstanceOf(TypeSubTypeValue.class)
                .extracting(r -> (TypeSubTypeValue) r)
                .isEqualTo(COMPLEX_TSTV_ID);

        assertThat(tstvParsed)
                .isInstanceOf(String.class)
                .extracting(r -> (String) r)
                .isEqualTo(COMPLEX_TSTV_ID_STRING);
    }

    @Test
    void shouldParseSetOfTypeSubTypeValues() {
        Object stringParsed = GafferCustomTypeFactory.parseAsCustomTypeIfValid(TSTV_PROPERTY_SET_STRING);
        Object tstvParsed = GafferCustomTypeFactory.parseForGraphSONv3(TSTV_PROPERTY_SET);

        // Then
        assertThat(stringParsed)
                .isInstanceOf(Collection.class)
                .asInstanceOf(InstanceOfAssertFactories.COLLECTION)
                .containsExactlyInAnyOrderElementsOf(TSTV_PROPERTY_SET);

        assertThat(tstvParsed)
                .isInstanceOf(Collection.class)
                .asInstanceOf(InstanceOfAssertFactories.COLLECTION)
                .containsExactlyInAnyOrderElementsOf(TSTV_PROPERTY_SET_STRING);
    }

    @Test
    void shouldNotParseStringAsTstv() {
        String notATstv = "not|a|tstv";
        Object result = GafferCustomTypeFactory.parseAsCustomTypeIfValid(notATstv);
        assertThat(result)
                .isInstanceOf(String.class)
                .extracting(r -> (String) r)
                .isEqualTo("not|a|tstv");
    }

    @Test
    void shouldNotParseObjectAsTstv() {
        Object notATstv = 1;
        Object result = GafferCustomTypeFactory.parseAsCustomTypeIfValid(notATstv);
        assertThat(result)
                .isInstanceOf(Integer.class)
                .extracting(r -> (Integer) r)
                .isEqualTo(1);
    }
}
