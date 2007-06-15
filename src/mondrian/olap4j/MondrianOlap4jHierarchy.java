/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.metadata.*;

import java.util.Locale;

/**
 * Implementation of {@link org.olap4j.metadata.Hierarchy}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 25, 2007
 */
class MondrianOlap4jHierarchy implements Hierarchy {
    private final MondrianOlap4jSchema olap4jSchema;
    private final mondrian.olap.Hierarchy hierarchy;

    MondrianOlap4jHierarchy(
        MondrianOlap4jSchema olap4jSchema,
        mondrian.olap.Hierarchy hierarchy)
    {
        this.olap4jSchema = olap4jSchema;
        this.hierarchy = hierarchy;
    }

    public boolean equals(Object obj) {
        return obj instanceof MondrianOlap4jHierarchy &&
            hierarchy.equals(((MondrianOlap4jHierarchy) obj).hierarchy);
    }

    public int hashCode() {
        return hierarchy.hashCode();
    }

    public Dimension getDimension() {
        return new MondrianOlap4jDimension(
            olap4jSchema, hierarchy.getDimension());
    }

    public NamedList<Level> getLevels() {
        if (false) {
            return null;
        }
        throw new UnsupportedOperationException();
    }

    public boolean hasAll() {
        if (false) {
            return false;
        }
        throw new UnsupportedOperationException();
    }

    public Member getDefaultMember() {
        if (false) {
            return null;
        }
        throw new UnsupportedOperationException();
    }

    public String getName() {
        if (false) {
            return null;
        }
        throw new UnsupportedOperationException();
    }

    public String getUniqueName() {
        return hierarchy.getUniqueName();
    }

    public String getCaption(Locale locale) {
        if (false) {
            return null;
        }
        throw new UnsupportedOperationException();
    }

    public String getDescription(Locale locale) {
        if (false) {
            return null;
        }
        throw new UnsupportedOperationException();
    }
}

// End MondrianOlap4jHierarchy.java