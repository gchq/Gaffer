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

package uk.gov.gchq.gaffer.arrayliststore.data;

public class SimpleEntityDataObject {

    private int id;
    private int visibility;
    private String properties;

    public SimpleEntityDataObject() {
    }

    public SimpleEntityDataObject(final int id, final int visibility, final String properties) {
        this.id = id;
        this.visibility = visibility;
        this.properties = properties;
    }

    public int getId() {
        return id;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public int getVisibility() {
        return visibility;
    }

    public void setVisibility(final int visibility) {
        this.visibility = visibility;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(final String properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "SimpleEntityDataObject{" +
                "id=" + id +
                ", visibility=" + visibility +
                ", properties='" + properties + '\'' +
                '}';
    }
}