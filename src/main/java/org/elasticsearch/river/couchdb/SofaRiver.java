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

import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.*;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.client.Requests.deleteRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class SofaRiver extends AbstractRiverComponent implements River {

	private final String DEFAULT_INDEX_NAME = "sofa_db";
	private final String DEFAULT_TYPE_NAME = "sofa";
	
	private final int DEFAULT_BACKOFF_MIN = 1000;
	private final int DEFAULT_BACKOFF_MAX = 60000;
	
    private final Client client;

    private final String riverIndexName;

    private final String couchProtocol;
    private final String couchHost;
    private final int couchPort;
    private final Pattern couchDbFilter;
    private final String basicAuth;
    private final boolean noVerify;
    private final boolean couchIgnoreAttachments;

    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    
    private final int backoffMin;
    private final int backoffMax;
    
    private int backoff;

    private final ExecutableScript script;

    private volatile Thread slurperThread;
    private volatile boolean closed;

    @SuppressWarnings({"unchecked"})
    @Inject
    public SofaRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;

        if (settings.settings().containsKey("couchdb")) {
            Map<String, Object> couchSettings = (Map<String, Object>) settings.settings().get("couchdb");
            couchProtocol = XContentMapValues.nodeStringValue(couchSettings.get("protocol"), "http");
            noVerify = XContentMapValues.nodeBooleanValue(couchSettings.get("no_verify"), false);
            couchHost = XContentMapValues.nodeStringValue(couchSettings.get("host"), "localhost");
            couchPort = XContentMapValues.nodeIntegerValue(couchSettings.get("port"), 5984);
            
            couchIgnoreAttachments = XContentMapValues.nodeBooleanValue(couchSettings.get("ignore_attachments"), false);
            if (couchSettings.containsKey("user") && couchSettings.containsKey("password")) {
                String user = couchSettings.get("user").toString();
                String password = couchSettings.get("password").toString();
                basicAuth = "Basic " + Base64.encodeBytes((user + ":" + password).getBytes());
            } else {
                basicAuth = null;
            }

            if (couchSettings.containsKey("script")) {
                script = scriptService.executable("js", couchSettings.get("script").toString(), Maps.newHashMap());
            } else {
                script = null;
            }
            
            String couchDbFilterValue = XContentMapValues.nodeStringValue(couchSettings.get("db_filter"), null);
            if (couchDbFilterValue != null) {
            	couchDbFilter = Pattern.compile(couchDbFilterValue);
            } else {
            	couchDbFilter = null;
            }
        } else {
            couchProtocol = "http";
            couchHost = "localhost";
            couchPort = 5984;
            couchDbFilter = null;
            couchIgnoreAttachments = false;
            noVerify = false;
            basicAuth = null;
            script = null;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), DEFAULT_INDEX_NAME);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), DEFAULT_TYPE_NAME);
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            
            backoffMin = XContentMapValues.nodeIntegerValue(indexSettings.get("backoff_min"), DEFAULT_BACKOFF_MIN);
            backoffMax = XContentMapValues.nodeIntegerValue(indexSettings.get("backoff_max"), DEFAULT_BACKOFF_MAX);
        } else {
            indexName = DEFAULT_INDEX_NAME;
            typeName = DEFAULT_TYPE_NAME;
            bulkSize = 100;
            
            backoffMin = DEFAULT_BACKOFF_MIN;
            backoffMax = DEFAULT_BACKOFF_MAX;
        }
        
        backoff = backoffMin;
    }

    @Override
    public void start() {
        logger.info("starting sofa river [{}]: host [{}], port [{}], db filter [{}], indexing to [{}]/[{}]", SofaRiver.class.getPackage().getImplementationVersion(), couchHost, couchPort, couchDbFilter, indexName, typeName);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        slurperThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "sofa_river_slurper").newThread(new Slurper());
        slurperThread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing sofa stream river");
        closed = true;
        slurperThread.interrupt();
    }

    private class Slurper implements Runnable {
    	
    	private CouchdbClient couchClient;
    	
    	/**
    	 * Get a unique id for a database
    	 * 
    	 * We need to do this in-case a database is deleted and re-created with the same name. We
    	 * use a _local couch document so that replication will not duplicate the id which could confuse
    	 * the indexer.
    	 * 
    	 * @return The id or null if an error occured
    	 */
    	private String getDatabaseId(String dbname) throws CouchdbException {
    		
    		final String path = "/" + dbname + "/_local/es-sofa-river";
    		final String key = "index";
    		
    		String dbId;
    		
    		try {
    			Map<String, Object> doc = couchClient.getDocument(path);
    			dbId = (String) doc.get(key);
    		} catch (CouchdbExceptionNotFound e) {
    			// A 404 indicates we need to set the id on this database
    			dbId = dbname + "_" + UUID.randomUUID().toString();
    			
    			logger.info("No id document found for " + dbname + ". Creating " + dbId);
    			String doc = "{\"" + key + "\": \"" + dbId + "\"}";
    			
    			couchClient.createDocument(path, doc);
    		}
    		
    		return dbId;
    	}
    	
    	private class IndexException extends Exception {
			private static final long serialVersionUID = 1L;
    	}
    	
    	/**
    	 * Get the indexed seq of this database according to es
    	 * 
    	 * @return A seq string or null
    	 */
		private String getIndexSeq(final String dbId) throws IndexException {
    		
    		String lastSeq = null;
    		GetResponse lastSeqGetResponse = null;
    		
			// Otherwise get the seq from the index
    		try {
				client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
				lastSeqGetResponse = client.prepareGet(riverIndexName, riverName().name(), "_seq_" + dbId).execute().actionGet();
    		} catch (IndexMissingException e) {
    			throw new IndexException();
    		} catch (NoShardAvailableActionException e) {
    			throw new IndexException();
    		} catch (ElasticSearchInterruptedException e) {
    			throw new IndexException();
    		}
    		
			if (lastSeqGetResponse.exists()) {
				Map<String, Object> couchdbState = (Map<String, Object>) lastSeqGetResponse.sourceAsMap();
                
                if (couchdbState != null) {
                    lastSeq = couchdbState.get("last_seq").toString(); // we know its always a string
                }
            }
    		
    		return lastSeq;
    	}
    	
		/**
		 * Should we index this database name
		 */
    	private boolean isDatabaseIndexed(final String name) {
    		if (couchDbFilter == null) {
    			return true;
    		}
    		
    		Matcher matcher = couchDbFilter.matcher(name);
    		return matcher.matches();
    	}
    	
    	/**
    	 * Attempt to index a chunk of changes from the given db
    	 * 
    	 * @return {@link Boolean} If any changes were indexed
    	 */
    	@SuppressWarnings("unchecked")
		private boolean indexDatabase(final String name) {
    		
    		if (!isDatabaseIndexed(name)) {
    			return false;
    		}
    		
    		final String dbId;
        	try {
        		dbId = getDatabaseId(name);
        	} catch (CouchdbException e) {
        		logger.warn("Failed to get id for database ", name);
        		return false;
        	}
        	
        	final String lastSeq;
        	try {
        		lastSeq = getIndexSeq(dbId);
        	} catch (IndexException e) {
        		logger.error("Failed to get seq for db " + name);
        		return false;
        	}
        	logger.trace("Last seq for db " + name + ": " + lastSeq);
        	String path = "/" + name + "/_changes?include_docs=true&limit=" + bulkSize;

            if (lastSeq != null) {
                try {
                    path = path + "&since=" + URLEncoder.encode(lastSeq, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    // should not happen, but in any case...
                	path = path + "&since=" + lastSeq;
                }
            }
            
            Map<String, Object> changes;
            try {
            	changes = couchClient.getDocument(path);
            } catch (CouchdbException e) {
            	logger.warn("Failed to read changes for database ", name);
            	return false;
            }
            
            List<Map<String, Object>> results = (List<Map<String, Object>>) changes.get("results");
            if (results.size() == 0) {
            	return false;
            }
            
            // Prepare to update the index
            BulkRequestBuilder bulk = client.prepareBulk();
            for (Map<String, Object> line : results) {
            	try {
            		processLine(line, bulk);
            	} catch (Exception e) {
            		logger.error("Failed to index line. SKIPPING. ", e);
            	}
            }
            
            // Write the new last_seq to the database seq doc
            final String newLastSeq = changes.get("last_seq").toString();
            try {
                bulk.add(indexRequest(riverIndexName).type(riverName.name()).id("_seq_" + dbId)
                        .source(jsonBuilder().startObject().field("last_seq", newLastSeq).endObject()));
            } catch (IOException e) {
                logger.warn("failed to add last_seq entry to bulk indexing");
                return false;
            }

            try {
                BulkResponse response = bulk.execute().actionGet();
                if (response.hasFailures()) {
                    // TODO write to exception queue?
                	// Failures here still count as stuff getting indexed
                    logger.warn("failed to execute" + response.buildFailureMessage());
                    
                    return true;
                }
            } catch (Exception e) {
                logger.warn("failed to execute bulk", e);
                return false;
            }
            
            return true;
    	}
    	
        @Override
        public void run() {

        	couchClient = new CouchdbClient(couchProtocol, couchHost, couchPort, basicAuth, noVerify);
        	
            while (true) {
                if (closed) {
                    return;
                }

                Collection<String> dbs;
                
                // Get all database names
                try {
                	dbs = couchClient.getAllDbs();
                } catch (CouchdbException e) {
                	
                	logger.warn("failed to read from _all_dbs, throttling....", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        if (closed) {
                            return;
                        }
                    }
                    continue;
                }
                
                // Iterate through the dbs, checking the seq
                boolean hasChanges = false;
                
                for (final String db : dbs) {
                	if (closed) {
                        return;
                    }

                	hasChanges |= indexDatabase(db);
                }
                
                // If no indexes had changes then sleep for backoff
                if (!hasChanges) {
                	logger.info("No changes. Sleep for " + backoff);
                	try {
                		Thread.sleep(backoff);
                	} catch (InterruptedException e1) {
                		// Loop and return
                	}
                	
                	// Exponentially backoff up to the maxuimum
                	backoff = Math.min(backoffMax, backoff * 2);
                } else {
                	// If changes then reset the backoff
                	backoff = backoffMin;
                }
            }
        }
        
        @SuppressWarnings({"unchecked"})
        private Object processLine(Map<String, Object> ctx, BulkRequestBuilder bulk) {
            
            Object seq = ctx.get("seq");
            String id = ctx.get("id").toString();

            // Ignore design documents
            if (id.startsWith("_design/")) {
                if (logger.isTraceEnabled()) {
                    logger.trace("ignoring design document {}", id);
                }
                return seq;
            }

            if (script != null) {
                script.setNextVar("ctx", ctx);
                try {
                    script.run();
                    // we need to unwrap the ctx...
                    ctx = (Map<String, Object>) script.unwrap(ctx);
                } catch (Exception e) {
                    logger.warn("failed to script process {}, ignoring", e, ctx);
                    return seq;
                }
            }

            if (ctx.containsKey("ignore") && ctx.get("ignore").equals(Boolean.TRUE)) {
                // ignore dock
            } else if (ctx.containsKey("deleted") && ctx.get("deleted").equals(Boolean.TRUE)) {
                String index = extractIndex(ctx);
                String type = extractType(ctx);
                if (logger.isTraceEnabled()) {
                    logger.trace("processing [delete]: [{}]/[{}]/[{}]", index, type, id);
                }
                bulk.add(deleteRequest(index).type(type).id(id).routing(extractRouting(ctx)).parent(extractParent(ctx)));
            } else if (ctx.containsKey("doc")) {
                String index = extractIndex(ctx);
                String type = extractType(ctx);
                Map<String, Object> doc = (Map<String, Object>) ctx.get("doc");

                // Remove _attachment from doc if needed
                if (couchIgnoreAttachments) {
                    // no need to log that we removed it, the doc indexed will be shown without it
                    doc.remove("_attachments");
                } else {
                    // TODO by now, couchDB river does not really store attachments but only attachments meta infomration
                    // So we perhaps need to fully support attachments
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("processing [index ]: [{}]/[{}]/[{}], source {}", index, type, id, doc);
                }

                bulk.add(indexRequest(index).type(type).id(id).source(doc).routing(extractRouting(ctx)).parent(extractParent(ctx)));
            } else {
                logger.warn("ignoring unknown change");
            }
            return seq;
        }

        private String extractParent(Map<String, Object> ctx) {
            return (String) ctx.get("_parent");
        }

        private String extractRouting(Map<String, Object> ctx) {
            return (String) ctx.get("_routing");
        }

        private String extractType(Map<String, Object> ctx) {
            String type = (String) ctx.get("_type");
            if (type == null) {
                type = typeName;
            }
            return type;
        }

        private String extractIndex(Map<String, Object> ctx) {
            String index = (String) ctx.get("_index");
            if (index == null) {
                index = indexName;
            }
            return index;
        }
    }
}
