package com.wirktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wirktop.esutils.index.Indexer;
import com.wirktop.esutils.search.Scroll;
import com.wirktop.esutils.search.Search;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Cosmin Marginean
 */
public class ElasticClient {

    private static final Logger log = LoggerFactory.getLogger(ElasticClient.class);

    private Client client;
    private Admin admin;
    private Json json = new Json(new ObjectMapper());

    public ElasticClient(String clusterName, String commaSeparatedNodes) {
        this(transportClient(clusterName, splitNodes(commaSeparatedNodes)));
    }

    /**
     * @param clusterName Name of ElasticSearch cluster
     * @param nodes       A collection of hostname:port elements
     */
    public ElasticClient(String clusterName, Collection<String> nodes) {
        this(transportClient(clusterName, nodes));
    }

    private static List<String> splitNodes(String commaSeparatedNodes) {
        String[] nodesArray = commaSeparatedNodes.split(",");
        List<String> nodes = new ArrayList<>();
        for (String node : nodesArray) {
            nodes.add(node.trim());
        }
        return nodes;
    }

    public ElasticClient(Client client) {
        if (client == null) {
            throw new IllegalArgumentException("client argument cannot be null");
        }

        this.client = client;
        admin = new Admin(this);
    }

    private static Client transportClient(String clusterName, Collection<String> nodes) {
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
                client.addTransportAddress(new TransportAddress(byName, port));
            }
            return client;
        } catch (UnknownHostException e) {
            log.error("Error creating Search component: " + e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    public Search search(DataBucket bucket) {
        return new Search(this, bucket);
    }

    public Scroll scroll(DataBucket bucket) {
        return new Scroll(this, bucket);
    }

    public Indexer indexer(DataBucket bucket) {
        return new Indexer(this, bucket);
    }

    public Admin admin() {
        return admin;
    }

    public Client getClient() {
        return client;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        json = new Json(objectMapper);
    }

    public Json json() {
        return json;
    }
}
