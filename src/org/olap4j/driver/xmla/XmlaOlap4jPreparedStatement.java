/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.olap4j.driver.xmla;

import org.olap4j.*;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.metadata.*;
import org.olap4j.type.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of {@link org.olap4j.PreparedOlapStatement}
 * for XML/A providers.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newPreparedStatement}.</p>
 *
 * @author jhyde
 * @since Jun 12, 2007
 */
abstract class XmlaOlap4jPreparedStatement
    extends XmlaOlap4jStatement
    implements PreparedOlapStatement, OlapParameterMetaData
{
    final XmlaOlap4jCellSetMetaData cellSetMetaData;
    private final String mdx;

    /**
     * Creates an XmlaOlap4jPreparedStatement.
     *
     * @param olap4jConnection Connection
     * @param mdx MDX query string
     * @throws OlapException on error
     */
    XmlaOlap4jPreparedStatement(
        XmlaOlap4jConnection olap4jConnection,
        String mdx) throws OlapException
    {
        super(olap4jConnection);

        // Execute a statement and steal its metadata.
        final OlapStatement statement = olap4jConnection.createStatement();
        try {
            final CellSet cellSet = statement.executeOlapQuery(mdx);
            final XmlaOlap4jCellSetMetaData cellSetMetaData1 =
                (XmlaOlap4jCellSetMetaData) cellSet.getMetaData();
            this.cellSetMetaData = cellSetMetaData1.cloneFor(this);
            cellSet.close();
            statement.close();
        } catch (SQLException e) {
            throw getHelper().createException(
                "Error while preparing statement '" + mdx + "'",
                e);
        }

        this.mdx = mdx;
    }

    /**
     * Returns the error-handler.
     *
     * @return Error handler
     */
    private final XmlaHelper getHelper() {
        return olap4jConnection.helper;
    }

    // override OlapStatement

    public CellSet executeOlapQuery(String mdx) throws OlapException {
        return super.executeOlapQuery(mdx);
    }

    // implement PreparedOlapStatement

    public CellSet executeQuery() throws OlapException {
        return executeOlapQuery(mdx);
    }

    public OlapParameterMetaData getParameterMetaData() throws OlapException {
        return this;
    }

    public Cube getCube() {
        return cellSetMetaData.cube;
    }

    // implement PreparedStatement

    public int executeUpdate() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        getParameter(parameterIndex).setValue(null);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setBigDecimal(
        int parameterIndex, BigDecimal x) throws SQLException
    {
        getParameter(parameterIndex).setValue(x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setBytes(int parameterIndex, byte x[]) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        getParameter(parameterIndex).setValue(x);
    }

    public void setTimestamp(
        int parameterIndex, Timestamp x) throws SQLException
    {
        getParameter(parameterIndex).setValue(x);
    }

    public void setAsciiStream(
        int parameterIndex, InputStream x, int length) throws SQLException
    {
        getParameter(parameterIndex).setValue(x);
    }

    public void setUnicodeStream(
        int parameterIndex, InputStream x, int length) throws SQLException
    {
        getParameter(parameterIndex).setValue(x);
    }

    public void setBinaryStream(
        int parameterIndex, InputStream x, int length) throws SQLException
    {
        getParameter(parameterIndex).setValue(x);
    }

    public void unset(int parameterIndex) throws SQLException {
        getParameter(parameterIndex).unset();
    }

    public boolean isSet(int parameterIndex) throws SQLException {
        return getParameter(parameterIndex).isSet();
    }

    public void clearParameters() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setObject(
        int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        getParameter(parameterIndex).setValue(x);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        final Parameter parameter = getParameter(parameterIndex);
        parameter.setValue(x);
    }

    public boolean execute() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void addBatch() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setCharacterStream(
        int parameterIndex, Reader reader, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public CellSetMetaData getMetaData() {
        return cellSetMetaData;
    }

    public void setDate(
        int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setTime(
        int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setTimestamp(
        int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setNull(
        int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setObject(
        int parameterIndex,
        Object x,
        int targetSqlType,
        int scaleOrLength) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement OlapParameterMetaData

    public String getParameterName(int param) throws OlapException {
        Parameter paramDef = getParameter(param);
        return paramDef.getName();
    }

    private Parameter getParameter(int param) throws OlapException {
        final List<Parameter> parameters = getParameters();
        if (param < 1 || param > parameters.size()) {
            throw getHelper().createException(
                "parameter ordinal " + param + " out of range");
        }
        return parameters.get(param - 1);
    }

    private List<Parameter> getParameters() {
        // XMLA statements do not have parameters yet
        return Collections.emptyList();
    }

    public Type getParameterOlapType(int param) throws OlapException {
        throw Olap4jUtil.needToImplement(this);
    }

    public int getParameterCount() {
        return getParameters().size();
    }

    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    public boolean isSigned(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        return type instanceof NumericType;
    }

    public int getPrecision(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        if (type instanceof NumericType) {
            return 0; // precision not applicable
        }
        if (type instanceof StringType) {
            return Integer.MAX_VALUE;
        }
        return 0;
    }

    public int getScale(int param) throws SQLException {
        return 0; // scale not applicable
    }

    public int getParameterType(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        if (type instanceof NumericType) {
            return Types.NUMERIC;
        } else if (type instanceof StringType) {
            return Types.VARCHAR;
        } else if (type instanceof NullType) {
            return Types.NULL;
        } else {
            return Types.OTHER;
        }
    }

    public String getParameterTypeName(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        return type.toString();
    }

    public String getParameterClassName(int param) throws SQLException {
        final Type type = getParameterOlapType(param);
        return foo(
            new XmlaOlap4jPreparedStatement.TypeHelper<Class<?>>() {
                public Class<?> booleanType(BooleanType type) {
                    return Boolean.class;
                }

                public Class<Cube> cubeType(CubeType cubeType) {
                    return Cube.class;
                }

                public Class<Number> decimalType(DecimalType decimalType) {
                    return Number.class;
                }

                public Class<Dimension> dimensionType(
                    DimensionType dimensionType)
                {
                    return Dimension.class;
                }

                public Class<Hierarchy> hierarchyType(
                    HierarchyType hierarchyType)
                {
                    return Hierarchy.class;
                }

                public Class<Level> levelType(LevelType levelType) {
                    return Level.class;
                }

                public Class<Member> memberType(MemberType memberType) {
                    return Member.class;
                }

                public Class<Void> nullType(NullType nullType) {
                    return Void.class;
                }

                public Class<Number> numericType(NumericType numericType) {
                    return Number.class;
                }

                public Class<Iterable> setType(SetType setType) {
                    return Iterable.class;
                }

                public Class<String> stringType(StringType stringType) {
                    return String.class;
                }

                public Class<Member[]> tupleType(TupleType tupleType) {
                    return Member[].class;
                }

                public Class<?> symbolType(SymbolType symbolType) {
                    // parameters cannot be of this type
                    throw new UnsupportedOperationException();
                }
            },
            type).getName();
    }

    public int getParameterMode(int param) throws SQLException {
        throw Olap4jUtil.needToImplement(this);
    }

    // Helper classes

    private interface TypeHelper<T> {
        T booleanType(BooleanType type);
        T cubeType(CubeType cubeType);
        T decimalType(DecimalType decimalType);
        T dimensionType(DimensionType dimensionType);
        T hierarchyType(HierarchyType hierarchyType);
        T levelType(LevelType levelType);
        T memberType(MemberType memberType);
        T nullType(NullType nullType);
        T numericType(NumericType numericType);
        T setType(SetType setType);
        T stringType(StringType stringType);
        T tupleType(TupleType tupleType);
        T symbolType(SymbolType symbolType);
    }

    <T> T foo(XmlaOlap4jPreparedStatement.TypeHelper<T> helper, Type type) {
        if (type instanceof BooleanType) {
            return helper.booleanType((BooleanType) type);
        } else if (type instanceof CubeType) {
            return helper.cubeType((CubeType) type);
        } else if (type instanceof DecimalType) {
            return helper.decimalType((DecimalType) type);
        } else if (type instanceof DimensionType) {
            return helper.dimensionType((DimensionType) type);
        } else if (type instanceof HierarchyType) {
            return helper.hierarchyType((HierarchyType) type);
        } else if (type instanceof LevelType) {
            return helper.levelType((LevelType) type);
        } else if (type instanceof MemberType) {
            return helper.memberType((MemberType) type);
        } else if (type instanceof NullType) {
            return helper.nullType((NullType) type);
        } else if (type instanceof NumericType) {
            return helper.numericType((NumericType) type);
        } else if (type instanceof SetType) {
            return helper.setType((SetType) type);
        } else if (type instanceof StringType) {
            return helper.stringType((StringType) type);
        } else if (type instanceof TupleType) {
            return helper.tupleType((TupleType) type);
        } else if (type instanceof SymbolType) {
            return helper.symbolType((SymbolType) type);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private interface Parameter {
        String getName();
        void setValue(Object o);
        void unset();
        boolean isSet();
    }

    @Override
    public void setRowId(int i, RowId rowId) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNString(int i, String s) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNClob(int i, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClob(int i, Reader reader, long l) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNClob(int i, Reader reader, long l) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int i, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNCharacterStream(int i, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClob(int i, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBlob(int i, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNClob(int i, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

}

// End XmlaOlap4jPreparedStatement.java
