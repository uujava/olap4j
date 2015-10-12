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

import org.olap4j.OlapException;
import org.olap4j.driver.xmla.proxy.XmlaOlap4jProxy;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * Implementation of {@link Factory} for JDBC 3.0.
 *
 * @author jhyde
 * @since Jun 14, 2007
 */
class FactoryJdbc3Impl implements Factory {
    /**
     * Creates a FactoryJdbc3Impl.
     */
    public FactoryJdbc3Impl() {
    }

    public Connection newConnection(
        XmlaOlap4jDriver driver,
        XmlaOlap4jProxy proxy,
        String url,
        Properties info)
        throws SQLException
    {
        return new FactoryJdbc3Impl.XmlaOlap4jConnectionJdbc3(
            driver, proxy, url, info);
    }

    public EmptyResultSet newEmptyResultSet(
        XmlaOlap4jConnection olap4jConnection)
    {
        List<String> headerList = Collections.emptyList();
        List<List<Object>> rowList = Collections.emptyList();
        return new FactoryJdbc3Impl.EmptyResultSetJdbc3(
            olap4jConnection, headerList, rowList);
    }

    public ResultSet newFixedResultSet(
        XmlaOlap4jConnection olap4jConnection,
        List<String> headerList,
        List<List<Object>> rowList)
    {
        return new EmptyResultSetJdbc3(olap4jConnection, headerList, rowList);
    }

    public XmlaOlap4jCellSet newCellSet(
        XmlaOlap4jStatement olap4jStatement) throws OlapException
    {
        return new XmlaOlap4jCellSetJdbc3(olap4jStatement);
    }

    public XmlaOlap4jStatement newStatement(
        XmlaOlap4jConnection olap4jConnection)
    {
        return new XmlaOlap4jStatementJdbc3(olap4jConnection);
    }

    public XmlaOlap4jPreparedStatement newPreparedStatement(
        String mdx,
        XmlaOlap4jConnection olap4jConnection) throws OlapException
    {
        return new XmlaOlap4jPreparedStatementJdbc3(
            olap4jConnection, mdx);
    }

    public XmlaOlap4jDatabaseMetaData newDatabaseMetaData(
        XmlaOlap4jConnection olap4jConnection)
    {
        return new XmlaOlap4jDatabaseMetaDataJdbc3(
            olap4jConnection);
    }

    // Inner classes

    private static class XmlaOlap4jStatementJdbc3
        extends XmlaOlap4jStatement
    {
        public XmlaOlap4jStatementJdbc3(
            XmlaOlap4jConnection olap4jConnection)
        {
            super(olap4jConnection);
        }
    }

    private static class XmlaOlap4jPreparedStatementJdbc3
        extends XmlaOlap4jPreparedStatement
    {
        public XmlaOlap4jPreparedStatementJdbc3(
            XmlaOlap4jConnection olap4jConnection,
            String mdx) throws OlapException
        {
            super(olap4jConnection, mdx);
        }
    }

    private static class XmlaOlap4jCellSetJdbc3
        extends XmlaOlap4jCellSet
    {
        public XmlaOlap4jCellSetJdbc3(
            XmlaOlap4jStatement olap4jStatement) throws OlapException
        {
            super(olap4jStatement);
        }
    }

    private static class EmptyResultSetJdbc3 extends EmptyResultSet {
        public EmptyResultSetJdbc3(
            XmlaOlap4jConnection olap4jConnection,
            List<String> headerList,
            List<List<Object>> rowList)
        {
            super(olap4jConnection, headerList, rowList);
        }
    }

    private class XmlaOlap4jConnectionJdbc3 extends XmlaOlap4jConnection {
        public XmlaOlap4jConnectionJdbc3(
            XmlaOlap4jDriver driver,
            XmlaOlap4jProxy proxy,
            String url,
            Properties info)
            throws SQLException
        {
            super(FactoryJdbc3Impl.this, driver, proxy, url, info);
        }

        @Override
        public Clob createClob() throws SQLException {
            return null;
        }

        @Override
        public Blob createBlob() throws SQLException {
            return null;
        }

        @Override
        public NClob createNClob() throws SQLException {
            return null;
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return null;
        }

        @Override
        public boolean isValid(int i) throws SQLException {
            return false;
        }

        @Override
        public void setClientInfo(String s, String s2) throws SQLClientInfoException {

        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {

        }

        @Override
        public String getClientInfo(String s) throws SQLException {
            return null;
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return null;
        }

        @Override
        public Array createArrayOf(String s, Object[] objects) throws SQLException {
            return null;
        }

        @Override
        public Struct createStruct(String s, Object[] objects) throws SQLException {
            return null;
        }

        @Override
        public void abort(Executor executor) throws SQLException {

        }

        @Override
        public void setNetworkTimeout(Executor executor, int i) throws SQLException {

        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return 0;
        }
    }

    private static class XmlaOlap4jDatabaseMetaDataJdbc3
        extends XmlaOlap4jDatabaseMetaData
    {
        public XmlaOlap4jDatabaseMetaDataJdbc3(
            XmlaOlap4jConnection olap4jConnection)
        {
            super(olap4jConnection);
        }

        @Override
        public RowIdLifetime getRowIdLifetime() throws SQLException {
            return null;
        }

        @Override
        public ResultSet getSchemas(String s, String s2) throws SQLException {
            return null;
        }

        @Override
        public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
            return false;
        }

        @Override
        public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
            return false;
        }

        @Override
        public ResultSet getClientInfoProperties() throws SQLException {
            return null;
        }

        @Override
        public ResultSet getFunctions(String s, String s2, String s3) throws SQLException {
            return null;
        }

        @Override
        public ResultSet getFunctionColumns(String s, String s2, String s3, String s4) throws SQLException {
            return null;
        }

        @Override
        public ResultSet getPseudoColumns(String s, String s2, String s3, String s4) throws SQLException {
            return null;
        }

        @Override
        public boolean generatedKeyAlwaysReturned() throws SQLException {
            return false;
        }
    }
}

// End FactoryJdbc3Impl.java
