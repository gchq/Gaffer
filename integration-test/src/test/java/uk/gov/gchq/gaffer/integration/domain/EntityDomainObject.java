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
package uk.gov.gchq.gaffer.integration.domain;

/**
 * Please note that this object has been created in order to test the ElementGenerator code in the Gaffer framework.
 * It is not intended to be a representative example of how to map a domain object to a Gaffer graph element.  For an
 * example of how this mapping may be achieved, please see the 'example' project.
 */
public class EntityDomainObject extends DomainObject {

    private String name;
    private String stringproperty;
    private Integer intProperty;

    public EntityDomainObject(final String name, final String stringproperty, final Integer intProperty) {
        this.name = name;
        this.stringproperty = stringproperty;
        this.intProperty = intProperty;
    }

    public EntityDomainObject() {
    }

    public String getStringproperty() {
        return stringproperty;
    }

    public void setStringproperty(final String stringproperty) {
        this.stringproperty = stringproperty;
    }

    public Integer getIntProperty() {
        return intProperty;
    }

    public void setIntProperty(final Integer intProperty) {
        this.intProperty = intProperty;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "EntityDomainObject{" +
                "name='" + name + '\'' +
                ", stringproperty='" + stringproperty + '\'' +
                ", intProperty=" + intProperty +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final EntityDomainObject that = (EntityDomainObject) o;

        if (!name.equals(that.name)) return false;
        if (stringproperty != null ? !stringproperty.equals(that.stringproperty) : that.stringproperty != null)
            return false;
        return !(intProperty != null ? !intProperty.equals(that.intProperty) : that.intProperty != null);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (stringproperty != null ? stringproperty.hashCode() : 0);
        result = 31 * result + (intProperty != null ? intProperty.hashCode() : 0);
        return result;
    }
}
