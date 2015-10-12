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
package org.olap4j.driver.xmla.proxy;

import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import static org.olap4j.driver.xmla.XmlaOlap4jUtil.LOGGER;

/**
 * <p>CookieManager is a simple utility for handling cookies when working
 * with <code>java.net.URL</code> and <code>java.net.URLConnection</code>
 * objects.
 *
 * <p>This code was taken from http://www.hccp.org/java-net-cookie-how-to.html
 * and modified by Luc Boudreau to support concurrent access and be more
 * thread safe.
 *
 * <p>Here's a few use cases examples...
 *
 * <hr>
 * <code>
 *     <p>Cookiemanager cm = new CookieManager();
 *     <p>URL url = new URL("http://www.hccp.org/test/cookieTest.jsp");
 *
 *     <p> . . .
 *
 *     <p>// getting cookies:
 *     <p>URLConnection conn = url.openConnection();
 *     <p>conn.connect();
 *
 *     <p>// setting cookies
 *     <p>cm.storeCookies(conn);
 *     <p>cm.setCookies(url.openConnection());
 * </code>
 * <hr>
 *
 * @author <a href="mailto:spam@hccp.org">Ian Brown</a>
 */

@SuppressWarnings("unchecked")
class XmlaOlap4jCookieManager {

    private Map store;

    private static final String SET_COOKIE = "Set-Cookie";
    private static final String COOKIE_VALUE_DELIMITER = ";";
    private static final String PATH = "path";
    private static final String EXPIRES = "expires";
    private static final String DATE_FORMAT = "EEE, dd-MMM-yyyy hh:mm:ss z";
    private static final String SET_COOKIE_SEPARATOR = "; ";
    private static final String COOKIE = "Cookie";

    private static final char NAME_VALUE_SEPARATOR = '=';
    private static final char DOT = '.';

    private DateFormat dateFormat;

    public XmlaOlap4jCookieManager() {
        store = new ConcurrentHashMap();
        dateFormat = new SimpleDateFormat(DATE_FORMAT);
    }

    /**
     * Retrieves and stores cookies returned by the host on the other side
     * of the the open java.net.URLConnection.
     *
     * The connection MUST have been opened using the <i>connect()</i>
     * method or a IOException will be thrown.
     *
     * @param conn a java.net.URLConnection - must be open,
     * or IOException will be thrown
     * @throws java.io.IOException Thrown if <i>conn</i> is not open.
     */
    public void storeCookies(URLConnection conn) {
        // Determines the domain from where these cookies are being sent
        String domain = getDomainFromHost(conn.getURL().getHost());

        Map domainStore; // Where we will store cookies for this domain

        // Checks the store to see if we have an entry for this domain
        if (store.containsKey(domain)) {
            // we do, so lets retrieve it from the store
            domainStore = (Map) store.get(domain);
        } else {
            // we don't, so let's create it and put it in the store
            domainStore = new ConcurrentHashMap();
            store.put(domain, domainStore);
        }

        // OK, now we are ready to get the cookies out of the URLConnection

        String headerName = null;
        for (int i = 1; (headerName = conn.getHeaderFieldKey(i)) != null; i++) {
            if (headerName.equalsIgnoreCase(SET_COOKIE)) {
                Map cookie = new ConcurrentHashMap();
                StringTokenizer st = new StringTokenizer(
                    conn.getHeaderField(i), COOKIE_VALUE_DELIMITER);

                // the specification dictates that the first name/value pair
                // in the string is the cookie name and value, so let's handle
                // them as a special case:

                if (st.hasMoreTokens()) {
                    String token = st.nextToken();
                    String name = token.substring(
                        0,
                        token.indexOf(NAME_VALUE_SEPARATOR));
                    String value =
                        token.substring(
                            token .indexOf(NAME_VALUE_SEPARATOR) + 1,
                            token.length());
                    domainStore.put(name, cookie);
                    cookie.put(name, value);

                    if (LOGGER.isLoggable(java.util.logging.Level.FINE)) {
                        LOGGER.fine(
                                "Saving cookie : " + name + "=" + value);
                    }
                }

                while (st.hasMoreTokens()) {
                    String token = st.nextToken();

                    // Check if the separator does exist
                    // The other attributes are not stored (Ex: HttpOnly)
                    int separatorIndex = token.indexOf(NAME_VALUE_SEPARATOR);

                    if (separatorIndex > 0) {
                        String tokenName =
                            token.substring(
                                0,
                                separatorIndex)
                            .toLowerCase();
                        String tokenValue =
                            token.substring(
                                separatorIndex + 1,
                                token.length());

                        cookie.put(tokenName, tokenValue);

                        if (LOGGER.isLoggable(java.util.logging.Level.FINE)) {
                            LOGGER.fine(
                                "Saving cookie : "
                                + tokenName
                                + "=" + tokenValue);
                        }
                    }
                }
            }
        }
    }

    /**
     * Prior to opening a URLConnection, calling this method will set all
     * unexpired cookies that match the path or subpaths for thi underlying URL.
     *
     * <p>The connection MUST NOT have been opened method or an IOException will
     * be thrown.
     *
     * @param  conn a java.net.URLConnection  - must  NOT be  open, or
     * IOException will be thrown
     *
     * @throws java.io.IOException Thrown if <i>conn</i> has already been
     * opened.
     */
    public void setCookies(URLConnection conn) {
        // Determines the domain and path to retrieve the appropriate cookies
        URL url = conn.getURL();
        String domain = getDomainFromHost(url.getHost());
        String path = url.getPath();

        Map domainStore = (Map) store.get(domain);
        if (domainStore == null) {
            return;
        }
        StringBuffer cookieStringBuffer = new StringBuffer();

        Iterator cookieNames = domainStore.keySet().iterator();
        while (cookieNames.hasNext()) {
            String cookieName = (String) cookieNames.next();
            Map cookie = (Map) domainStore.get(cookieName);
            // check cookie to ensure path matches  and cookie is not expired
            // if all is cool, add cookie to header string
            if (comparePaths((String) cookie.get(PATH), path)
                && isNotExpired((String) cookie.get(EXPIRES)))
            {
                cookieStringBuffer.append(cookieName);
                cookieStringBuffer.append("=");
                cookieStringBuffer.append((String) cookie.get(cookieName));
                if (cookieNames.hasNext()) {
                    cookieStringBuffer.append(SET_COOKIE_SEPARATOR);
                }
            }
        }
        try {
            if (LOGGER.isLoggable(java.util.logging.Level.FINE)
                && !(cookieStringBuffer.toString().equals("")))
            {
                LOGGER.fine("Using cookie : " + cookieStringBuffer.toString());
            }
            conn.setRequestProperty(COOKIE, cookieStringBuffer.toString());
        } catch (java.lang.IllegalStateException ise) {
            throw new RuntimeException(
                "Illegal State! Cookies cannot be set on a URLConnection that is already connected. Only call setCookies(java.net.URLConnection) AFTER calling java.net.URLConnection.connect().");
        }
    }

    private String getDomainFromHost(String host) {
        if (host.indexOf(DOT) != host.lastIndexOf(DOT)) {
            return host.substring(host.indexOf(DOT) + 1);
        } else {
            return host;
        }
    }

    private boolean isNotExpired(String cookieExpires) {
        if (cookieExpires == null) {
            return true;
        }
        Date now = new Date();
        try {
            return (now.compareTo(dateFormat.parse(cookieExpires))) <= 0;
        } catch (java.text.ParseException pe) {
            pe.printStackTrace();
            return false;
        }
    }

    private boolean comparePaths(String cookiePath, String targetPath) {
        if (cookiePath == null) {
            return true;
        } else if (cookiePath.equals("/")) {
            return true;
        } else if (targetPath.regionMatches(
                0, cookiePath, 0, cookiePath.length()))
        {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns a string representation of stored cookies organized by domain.
     */

    public String toString() {
        return store.toString();
    }
}

// End XmlaOlap4jCookieManager.java



