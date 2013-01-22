package org.olap4j.metadata;

import java.util.List;

public interface MeasureGroup extends MetadataElement {
    NamedList<Dimension> getDimensions();

    NamedList<Hierarchy> getHierarchies();

    List<Measure> getMeasures();
}
