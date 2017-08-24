package com.wirktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;

public class SearchTransportBuilder {

    private Collection<String> nodes = new ArrayList<>();
    private String clusterName;
    private String index;
    private String type;
    private ObjectMapper objectMapper;

    public SearchTransportBuilder nodes(Collection<String> nodes) {
        this.nodes = nodes;
        return this;
    }

    public SearchTransportBuilder node(String node) {
        this.nodes.add(node);
        return this;
    }

    public SearchTransportBuilder clusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public SearchTransportBuilder index(String index) {
        this.index = index;
        return this;
    }

    public SearchTransportBuilder type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Optional
     *
     * @param objectMapper
     * @return
     */
    public SearchTransportBuilder setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }


    public Search build() {
        Search search = new Search(nodes, clusterName, index, type);
        if (objectMapper != null) {
            search.setObjectMapper(objectMapper);
        }
        return search;
    }
}