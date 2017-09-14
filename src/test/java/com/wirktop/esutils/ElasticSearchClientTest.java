package com.wirktop.esutils;

import com.wirktop.esutils.search.Search;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * @author Cosmin Marginean
 */
public class ElasticSearchClientTest extends TestBase {


    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNodes() throws Exception {
        new ElasticSearchClient(Arrays.asList("aaaa2229300"), "x");
    }

    @Test(expected = SearchException.class)
    public void testInvalidHost() throws Exception {
        new ElasticSearchClient(Arrays.asList("blowup:9300"), "x");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoCluster() throws Exception {
        new ElasticSearchClient(Arrays.asList("blowup:9300"), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoClient() throws Exception {
        new ElasticSearchClient(null);
    }
}
