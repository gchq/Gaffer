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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.HashMap;
import java.util.Map;

public class NamedViewDetailTest {
    @Test
    public void shouldJsonSerialise() throws SerialisationException {
        // Given
        final Map<String, ViewParameterDetail> params = new HashMap<>();
        params.put("entityGroup", new ViewParameterDetail.Builder()
                .description("some description")
                .defaultValue("red")
                .valueClass(String.class)
                .build());
        final NamedViewDetail namedViewDetail = new NamedViewDetail.Builder()
                .name("view1")
                .creatorId("creator")
                .description("description")
                .parameters(params)
                .view("{\"entities\": {" +
                        "\"${entityGroup}\":{}" +
                        "}" +
                        "}")
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(namedViewDetail, true);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"name\" : \"view1\",%n" +
                "  \"description\" : \"description\",%n" +
                "  \"creatorId\" : \"creator\",%n" +
                "  \"writeAccessRoles\" : [ ],%n" +
                "  \"parameters\" : {%n" +
                "    \"entityGroup\" : {%n" +
                "      \"description\" : \"some description\",%n" +
                "      \"defaultValue\" : \"red\",%n" +
                "      \"valueClass\" : \"java.lang.String\",%n" +
                "      \"required\" : false%n" +
                "    }%n" +
                "  },%n" +
                "  \"view\" : \"{\\\"entities\\\": {\\\"${entityGroup}\\\":{}}}\"%n" +
                "}"), new String(json));
    }
}
