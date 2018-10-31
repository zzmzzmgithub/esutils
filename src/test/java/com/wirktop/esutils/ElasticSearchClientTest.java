package com.wirktop.esutils;

import org.junit.Test;

import java.util.Arrays;

/**
 * @author Cosmin Marginean
 */
public class ElasticSearchClientTest extends TestBase {


    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNodes() throws Exception {
        new ElasticClient("x", Arrays.asList("aaaa2229300"));
    }

    @Test(expected = SearchException.class)
    public void testInvalidHost() throws Exception {
        new ElasticClient("x", Arrays.asList("blowup:9300"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoCluster() throws Exception {
        new ElasticClient(null, Arrays.asList("blowup:9300"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoClient() throws Exception {
        new ElasticClient(null);
    }
}
