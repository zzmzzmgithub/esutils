package com.wirktop.esutils.admin;

import com.wirktop.esutils.SearchException;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

/**
 * @author Cosmin Marginean
 */
public class Admin {

    private static final Logger log = LoggerFactory.getLogger(Admin.class);

    private Client client;

    public Admin(Client client) {
        this.client = client;
    }

    public static void checkResponse(AcknowledgedResponse response) {
        if (!response.isAcknowledged()) {
            throw new SearchException("Error executing Elasticsearch request");
        }
    }

    public void createTemplate(String templateName, String jsonContent) {
        try {
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(jsonContent, XContentType.JSON);
            PutIndexTemplateResponse response = client.admin().indices().execute(PutIndexTemplateAction.INSTANCE, request).get();
            checkResponse(response);
            log.info("Created template {}", templateName);
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    public void createTemplate(String templateName, InputStream jsonInput) {
        try {
            String templateContent = IOUtils.toString(jsonInput, StandardCharsets.UTF_8.name());
            createTemplate(templateName, templateContent);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    public void createIndex(String index) {
        createIndex(index, -1);
    }

    public void createIndex(String index, int shards) {
        if (!indexExists(index)) {
            CreateIndexRequest request = new CreateIndexRequest(index);

            if (shards > 0) {
                Settings.Builder settings = Settings.builder()
                        .put("index.number_of_shards", shards);
                request = request.settings(settings);
            }

            CreateIndexResponse response = client.admin().indices().create(request).actionGet();
            checkResponse(response);
            final RefreshResponse refresh = client.admin().indices().prepareRefresh(index).execute().actionGet();
            if (refresh.getSuccessfulShards() < 1) {
                throw new SearchException(String.format("Index fail in %s shards.", refresh.getFailedShards()));
            }
            log.info("Created index: {}", index);
        } else {
            log.info("Index {} already exists. Skipping", index);
        }
    }

    public boolean indexExists(String index) {
        IndicesExistsResponse response = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        return response.isExists();
    }
}
