/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.couchdb;

import org.elasticsearch.common.io.Closeables;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * A mini couchdb client
 */
public class CouchdbClient {

    private final String couchProtocol;
    private final String couchHost;
    private final int couchPort;
    private final String basicAuth;
    private final boolean noVerify;

    public CouchdbClient(String couchProtocol, String couchHost, int couchPort, String basicAuth, boolean noVerify) {
    	this.couchProtocol = couchProtocol;
    	this.couchHost = couchHost;
    	this.couchPort = couchPort;
    	this.basicAuth = basicAuth;
    	this.noVerify = noVerify;
    }

    public Map<String, Object> getDocument(final String path) throws CouchdbException {

		HttpURLConnection connection = null;
        InputStream is = null;
        
        Map<String, Object> doc;
        
        try {
            URL url = new URL(couchProtocol, couchHost, couchPort, path);
            connection = (HttpURLConnection) url.openConnection();
            if (basicAuth != null) {
                connection.addRequestProperty("Authorization", basicAuth);
            }
            connection.setDoInput(true);
            connection.setUseCaches(false);

            if (noVerify) {
                ((HttpsURLConnection) connection).setHostnameVerifier(
                        new HostnameVerifier() {
                            public boolean verify(String string, SSLSession ssls) {
                                return true;
                            }
                        }
                );
            }

            is = connection.getInputStream();

            final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            
            try {
                doc = XContentFactory.xContent(XContentType.JSON).createParser(reader).mapAndClose();
            } catch (IOException e) {
                throw new CouchdbException();
            }
            
        } catch (FileNotFoundException e) {
        	throw new CouchdbExceptionNotFound();
        } catch (Exception e) {
            throw new CouchdbException();
        } finally {
            Closeables.closeQuietly(is);
            
            // We do not call disconnect() on the connection because we want java to use keep-alive on the underlying
            // socket connection.
        }
        
        return doc;
    }
    
    public Map<String, Object> createDocument(final String path, final String body) throws CouchdbException {

		HttpURLConnection connection = null;
		OutputStream os = null;
        InputStream is = null;
        
        Map<String, Object> doc;
        
        try {
            URL url = new URL(couchProtocol, couchHost, couchPort, path);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");
            connection.addRequestProperty("Content-Type", "application/json");
            
            if (basicAuth != null) {
                connection.addRequestProperty("Authorization", basicAuth);
            }
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setUseCaches(false);

            if (noVerify) {
                ((HttpsURLConnection) connection).setHostnameVerifier(
                        new HostnameVerifier() {
                            public boolean verify(String string, SSLSession ssls) {
                                return true;
                            }
                        }
                );
            }

            os = connection.getOutputStream();
            
            final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        	writer.write(body);
        	writer.flush();
        	
        	is = connection.getInputStream();
        	
        	final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        	doc = XContentFactory.xContent(XContentType.JSON).createParser(reader).mapAndClose();

        } catch (Exception e) {
            throw new CouchdbException();
        } finally {
            Closeables.closeQuietly(is);
            Closeables.closeQuietly(os);
            
            // We do not call disconnect() on the connection because we want java to use keep-alive on the underlying
            // socket connection.
        }
        
        return doc;
    }
    
    public Collection<String> getAllDbs() throws CouchdbException {
    	final String path = "/_all_dbs";
    	
    	// Irritatingly we cannot use getDocument() for this request because the response is a JSON list
    	HttpURLConnection connection = null;
        InputStream is = null;
        
        Collection<String> names = new ArrayList<String>();
        
        try {
            URL url = new URL(couchProtocol, couchHost, couchPort, path);
            connection = (HttpURLConnection) url.openConnection();
            if (basicAuth != null) {
                connection.addRequestProperty("Authorization", basicAuth);
            }
            connection.setDoInput(true);
            connection.setUseCaches(false);

            if (noVerify) {
                ((HttpsURLConnection) connection).setHostnameVerifier(
                        new HostnameVerifier() {
                            public boolean verify(String string, SSLSession ssls) {
                                return true;
                            }
                        }
                );
            }

            is = connection.getInputStream();

            final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String line = reader.readLine();
            
            // FIXME: THIS IS A HACK
            line = line.substring(1, line.length() - 1);
            for (String name : line.split(",")) {
            	if (name.length() < 2) {
            		continue;
            	}
            	names.add(name.substring(1, name.length() - 1));
            }
            
            // Read the rest of the input stream. This is so the HttpURLConnection can use keep-alive on the connection
            while (reader.readLine() != null) {
            	// Do nothing
            }
            
        } catch (FileNotFoundException e) {
        	throw new CouchdbExceptionNotFound();
        } catch (Exception e) {
            throw new CouchdbException();
        } finally {
            Closeables.closeQuietly(is);
            
            // We do not call disconnect() on the connection because we want java to use keep-alive on the underlying
            // socket connection.
        }
    	
    	return names;
    }
}
