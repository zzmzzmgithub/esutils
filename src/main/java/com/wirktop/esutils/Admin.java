package com.wirktop.esutils;

import com.wirktop.esutils.index.IndexBatch;
import com.wirktop.esutils.search.Search;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Cosmin Marginean
 */
public class Admin {

    private static final Logger log = LoggerFactory.getLogger(Admin.class);
    private static final String ANYTYPE = "ANYTYPE";

    private ElasticSearchClient esClient;

    public Admin(ElasticSearchClient esClient) {
        this.esClient = esClient;
    }

    public static void checkResponse(AcknowledgedResponse response) {
        if (!response.isAcknowledged()) {
            throw new SearchException("Error executing Elasticsearch request");
        }
    }

    public void createTemplate(String templateName, String jsonContent) {
        try {
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(jsonContent, XContentType.JSON);
            PutIndexTemplateResponse response = esClient.getClient().admin()
                    .indices()
                    .execute(PutIndexTemplateAction.INSTANCE, request)
                    .get();
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

    public void createIndex(DataBucket dataBucket) {
        dataBucket.createIndex(this);
    }

    public void createIndex(DataBucket dataBucket, int shards) {
        dataBucket.createIndex(this, shards);
    }

    public void wipe(AliasWrappedBucket dataBucket) {
        wipe(dataBucket, 0);
    }

    public void wipe(AliasWrappedBucket dataBucket, int shards) {
        dataBucket.wipe(this, shards);
    }

    public void createIndex(String index) {
        createIndex(index, 0);
    }

    public void createIndex(String index, int shards) {
        if (!indexExists(index)) {
            CreateIndexRequest request = new CreateIndexRequest(index);

            if (shards > 0) {
                Settings.Builder settings = Settings.builder()
                        .put("index.number_of_shards", shards);
                request = request.settings(settings);
            }

            CreateIndexResponse response = esClient.getClient()
                    .admin()
                    .indices()
                    .create(request)
                    .actionGet();
            checkResponse(response);
            final RefreshResponse refresh = esClient
                    .getClient()
                    .admin()
                    .indices()
                    .prepareRefresh(index)
                    .execute()
                    .actionGet();
            if (refresh.getSuccessfulShards() < 1) {
                throw new SearchException(String.format("Index fail in %s shards.", refresh.getFailedShards()));
            }
            log.info("Created index: {}", index);
        } else {
            log.info("Index {} already exists. Skipping", index);
        }
    }

    public boolean aliasExists(String aliasName) {
        AliasesExistResponse response = esClient.getClient()
                .admin()
                .indices()
                .prepareAliasesExist(aliasName)
                .execute()
                .actionGet();
        return response.exists();
    }

    public void createAlias(String aliasName, String... indices) {
        createAlias(aliasName, null, indices);
    }

    public void createAlias(String aliasName, QueryBuilder filter, String... indices) {
        IndicesAliasesRequestBuilder req = esClient.getClient()
                .admin()
                .indices()
                .prepareAliases();
        if (filter != null) {
            req = req.addAlias(indices, aliasName, filter);
        } else {
            req = req.addAlias(indices, aliasName);
        }
        IndicesAliasesResponse response = req.execute().actionGet();
        checkResponse(response);
    }

    public void removeAlias(String alias) {
        GetAliasesRequest request = new GetAliasesRequest(alias);
        GetAliasesResponse response = esClient.getClient()
                .admin()
                .indices()
                .getAliases(request)
                .actionGet();
        ImmutableOpenMap<String, List<AliasMetaData>> aliases = response.getAliases();
        if (!aliases.isEmpty()) {
            String[] indices = aliases.keys().toArray(String.class);
            IndicesAliasesResponse removeResponse = esClient.getClient()
                    .admin()
                    .indices()
                    .prepareAliases()
                    .removeAlias(indices, alias)
                    .execute().actionGet();
            checkResponse(removeResponse);
        }
    }

    public Collection<String> indexesForAlias(String alias) {
        GetAliasesRequest r = new GetAliasesRequest(alias);
        GetAliasesResponse response = esClient.getClient()
                .admin()
                .indices()
                .getAliases(r)
                .actionGet();
        return Arrays.asList(response.getAliases().keys().toArray(String.class));
    }

    public boolean indexExists(String index) {
        IndicesExistsResponse response = esClient.getClient()
                .admin()
                .indices()
                .prepareExists(index)
                .execute()
                .actionGet();
        return response.isExists() && !aliasExists(index);
    }

    public void removeIndex(String index) {
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest(index);
        DeleteIndexResponse response = esClient.getClient()
                .admin()
                .indices()
                .delete(deleteRequest)
                .actionGet();
        checkResponse(response);
    }

    public void copyData(String srcIndex, String targetIndex) {
        Search targetSearch = esClient.search(new DataBucket(targetIndex, ANYTYPE));
        try (IndexBatch batch = targetSearch.indexer().batch(100)) {
            esClient.search(new DataBucket(srcIndex, ANYTYPE))
                    .scrollFullIndex()
                    .forEach((hit) -> batch.add(hit.getId(), hit.getSourceAsString()));
        }
    }
}
