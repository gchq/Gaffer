/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

import static org.junit.Assert.assertEquals;

public class ViewIT {

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSameJson() {
        final View view1 = loadView();
        final byte[] json1 = view1.toCompactJson();
        final View view2 = new View.Builder().json(json1).build();

        final byte[] json2 = view2.toCompactJson();

        assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSamePrettyJson() {
        final View view1 = loadView();
        final byte[] json1 = view1.toJson(true);
        final View view2 = new View.Builder().json(json1).build();

        final byte[] json2 = view2.toJson(true);

        assertEquals(new String(json1), new String(json2));
    }

    private View loadView() {
        return new View.Builder().json(StreamUtil.view(getClass())).build();
    }
}
