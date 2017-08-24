package com.wirktop.esutils;

import com.wirktop.esutils.admin.Admin;
import com.wirktop.esutils.search.Search;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

/**
 * @author Cosmin Marginean
 */
public class ElasticSearchClient {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchClient.class);

    private Client client;
    private Admin admin;

    /**
     * @param nodes       A collection of hostname:port elements
     * @param clusterName Name of ES cluster
     */
    public ElasticSearchClient(Collection<String> nodes, String clusterName) {
        this(transportClient(nodes, clusterName));
    }

    public ElasticSearchClient(Client client) {
        if (client == null) {
            throw new IllegalArgumentException("client argument cannot be null");
        }

        this.client = client;
        admin = new Admin(client);
    }

    public Search search(String index, String type) {
        return new Search(client, index, type);
    }

    public Admin admin() {
        return admin;
    }

    private static Client transportClient(Collection<String> nodes, String clusterName) {
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
            return client;
        } catch (UnknownHostException e) {
            log.error("Error creating Search component: " + e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }
}