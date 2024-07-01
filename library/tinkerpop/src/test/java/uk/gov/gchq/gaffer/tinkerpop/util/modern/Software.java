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

package uk.gov.gchq.gaffer.tinkerpop.util.modern;

import uk.gov.gchq.gaffer.data.element.Entity;

import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.LANG;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.SOFTWARE;

/**
 * Helper class to make it easy to reference 'software' vertices in tests
 */
public class Software {
    private final String id;
    private final String name;
    private final String lang;

    public Software(String id, String name, String lang) {
        this.id = id;
        this.name = name;
        this.lang = lang;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getLang() {
        return lang;
    }

    public Entity toEntity() {
        return new Entity.Builder()
                .group(SOFTWARE)
                .vertex(id)
                .property(NAME, name)
                .property(LANG, lang)
                .build();
    }
}
