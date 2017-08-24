package com.wirktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Client;

public class SearchClientBuilder {

    private Client client;
    private String index;
    private String type;
    private ObjectMapper objectMapper;

    public SearchClientBuilder client(Client client) {
        this.client = client;
        return this;
    }

    public SearchClientBuilder index(String index) {
        this.index = index;
        return this;
    }

    public SearchClientBuilder type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Optional
     * @param objectMapper
     * @return
     */
    public SearchClientBuilder setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }

    public Search build() {
        Search search = new Search(client, index, type);
        if (objectMapper != null) {
            search.setObjectMapper(objectMapper);
        }
        return search;
    }
}