package com.wirktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wirktop.esutils.index.Indexer;
import com.wirktop.esutils.scroll.ScrollIterator;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Cosmin Marginean
 */
public class Search {

    private static final Logger log = LoggerFactory.getLogger(Search.class);

    public static final String ID = "_id";

    private Client client;
    private String index;
    private String type;
    private ObjectMapper objectMapper = new ObjectMapper();
    private Indexer indexer;

    public static SearchClientBuilder clientBuilder() {
        return new SearchClientBuilder();
    }

    public static SearchTransportBuilder transportBuilder() {
        return new SearchTransportBuilder();
    }

    /**
     * @param nodes       A collection of hostname:port elements
     * @param clusterName
     * @param index
     * @param type
     */
    public Search(Collection<String> nodes, String clusterName, String index, String type) {
        if (clusterName == null) {
            throw new IllegalArgumentException("clusterName argument cannot be null");
        }

        try {
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            TransportClient client = new PreBuiltTransportClient(settings);
            for (String nodeAddr : nodes) {
                String[] addressElements = nodeAddr.split(":");
                if (addressElements.length != 2) {
                    throw new IllegalArgumentException(String.format("Address %s has incorrect format (hostname:port)", nodeAddr));
                }
                String hostname = addressElements[0].trim();
                int port = Integer.parseInt(addressElements[1].trim());
                InetAddress byName = InetAddress.getByName(hostname);
                client.addTransportAddress(new InetSocketTransportAddress(byName, port));
            }
            initialize(client, index, type);
        } catch (UnknownHostException e) {
            log.error("Error creating Search component: " + e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    protected Search(Client client, String index, String type) {
        initialize(client, index, type);
    }

    private void initialize(Client client, String index, String type) {
        if (client == null) {
            throw new IllegalArgumentException("client argument cannot be null");
        }
        if (index == null) {
            throw new IllegalArgumentException("index argument cannot be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("type argument cannot be null");
        }
        this.client = client;
        this.index = index;
        this.type = type;
        this.indexer = new Indexer(this);
    }

    public JSONObject getJson(String id) {
        GetResponse response = get(id);
        if (response.isExists()) {
            return new JSONObject(response.getSourceAsString());
        }
        return null;
    }

    public Map<String, Object> getMap(String id) {
        GetResponse response = get(id);
        if (response.isExists()) {
            return response.getSourceAsMap();
        }
        return null;
    }

    public String getStr(String id) {
        GetResponse response = get(id);
        if (response.isExists()) {
            return response.getSourceAsString();
        }
        return null;
    }

    public <T> T get(String id, Class<T> docClass) {
        try {
            GetResponse response = get(id);
            if (response.isExists()) {
                return objectMapper.readValue(response.getSourceAsString(), docClass);
            }
            return null;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    private GetResponse get(String id) {
        GetRequestBuilder getRequest = client.prepareGet(index(), type(), id);
        return getRequest.execute().actionGet();
    }


    public Indexer indexer() {
        return indexer;
    }

    public Client client() {
        return client;
    }

    public SearchRequestBuilder searchRequest() {
        return client.prepareSearch(index())
                .setTypes(type());
    }

    public Stream<SearchHit> scroll(QueryBuilder query) {
        ScrollIterator iterator = new ScrollIterator(this, query, true, ScrollIterator.DEFAULT_PAGE_SIZE, ScrollIterator.DEFAULT_KEEPALIVE);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
}
