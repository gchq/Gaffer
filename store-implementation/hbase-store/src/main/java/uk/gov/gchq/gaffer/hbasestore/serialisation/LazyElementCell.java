/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.hbasestore.serialisation;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;

public class LazyElementCell {
    private final ElementSerialisation serialisation;
    private Cell cell;
    private Element element;
    private String group;

    public LazyElementCell(final Cell cell,
                           final ElementSerialisation serialisation) {
        this.cell = cell;
        this.serialisation = serialisation;
    }

    public LazyElementCell(final Cell cell,
                           final ElementSerialisation serialisation,
                           final Element element) {
        this.cell = cell;
        this.serialisation = serialisation;
        setElement(element);
    }

    public Cell getCell() {
        return cell;
    }

    public void setCell(final Cell cell) {
        this.cell = cell;
        group = null;
    }

    public boolean isElementLoaded() {
        return null != element;
    }

    public Element getElement() {
        if (null == element) {
            if (isDeleted()) {
                throw new IllegalStateException("Element has been marked for deletion it should not be used");
            }
            try {
                setElement(serialisation.getElement(cell));
            } catch (SerialisationException e) {
                throw new RuntimeException(e);
            }
        }

        return element;
    }

    public void setElement(final Element element) {
        this.element = element;
        group = null != element ? element.getGroup() : null;
    }

    public ElementSerialisation getSerialisation() {
        return serialisation;
    }

    public boolean isDeleted() {
        return CellUtil.isDelete(cell);
    }

    public String getGroup() {
        if (null == group) {
            if (!isDeleted()) {
                try {
                    group = serialisation.getGroup(cell);
                } catch (SerialisationException e) {
                    throw new RuntimeException("Unable to deserialise group", e);
                }
            }
        }

        return group;
    }
}
