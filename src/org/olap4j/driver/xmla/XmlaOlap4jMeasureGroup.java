package org.olap4j.driver.xmla;

import java.util.ArrayList;
import java.util.List;

import org.olap4j.OlapException;
import org.olap4j.impl.Named;
import org.olap4j.impl.NamedListImpl;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.MeasureGroup;
import org.olap4j.metadata.NamedList;

/**
 * Implementation of {@link org.olap4j.metadata.Measure}
 * for XML/A providers.
 */
class XmlaOlap4jMeasureGroup
    implements MeasureGroup, Named
{
    final XmlaOlap4jCube olap4jCube;
    private final String name;
    private final String caption;
    private final String description;

    final NamedList<XmlaOlap4jDimension> dimensions;
    private NamedList<XmlaOlap4jHierarchy> hierarchies = null;
    final List<XmlaOlap4jMeasure> measures =
        new ArrayList<XmlaOlap4jMeasure>();

    XmlaOlap4jMeasureGroup(
        XmlaOlap4jCube olap4jCube,
        String name,
        String caption,
        String description) throws OlapException
    {
        assert olap4jCube != null;
        this.olap4jCube = olap4jCube;
        this.name = name;
        this.caption = caption;
        this.description = description;

        final XmlaOlap4jConnection olap4jConnection =
            olap4jCube.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;

        final XmlaOlap4jConnection.Context context =
            new XmlaOlap4jConnection.Context(olap4jCube, null, null, null);

        String[] restrictions = {
            "CATALOG_NAME",
            olap4jCube.olap4jSchema.olap4jCatalog.getName(),
            "SCHEMA_NAME",
            olap4jCube.olap4jSchema.getName(),
            "CUBE_NAME",
            olap4jCube.getName(),
            "MEASUREGROUP_NAME",
            getName()
        };

        this.dimensions = new DeferredNamedListImpl<XmlaOlap4jDimension>(
            XmlaOlap4jConnection.MetadataRequest.MDSCHEMA_MEASUREGROUP_DIMENSIONS,
            context,
            new XmlaOlap4jConnection.MeasureGroupDimensionHandler(olap4jCube),
            restrictions);

        // populate measures up front; a measure is needed in every query
        olap4jConnection.populateList(
            measures,
            context,
            XmlaOlap4jConnection.MetadataRequest.MDSCHEMA_MEASURES,
            new XmlaOlap4jConnection.MeasureHandler(),
            restrictions);
    }

    public String getName() {
        return name;
    }

    public String getUniqueName() {
        return "[" + name + "]";
    }

    public String getCaption() {
        return caption;
    }

    public String getDescription() {
        return description;
    }

    public boolean isVisible() {
        return true;
    }

    public NamedList<Dimension> getDimensions() {
        return Olap4jUtil.cast(dimensions);
    }

    public NamedList<Hierarchy> getHierarchies() {
        // This is a costly operation. It forces the init
        // of all dimensions and all hierarchies.
        // We defer it to this point.
        if (this.hierarchies == null) {
            this.hierarchies = new NamedListImpl<XmlaOlap4jHierarchy>();
            for (XmlaOlap4jDimension dim : this.dimensions) {
                this.hierarchies.addAll(dim.hierarchies);
            }
        }
        return Olap4jUtil.cast(hierarchies);
    }

    public List<Measure> getMeasures() {
        return Olap4jUtil.cast(measures);
    }
}
