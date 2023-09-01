package com.shamengxin;

import co.elastic.clients.elasticsearch._types.aggregations.AggregateBuilders;
import com.fasterxml.jackson.databind.util.ArrayBuilders;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

public class RsetHighLevelClientForAggsTests extends SpringBootEsApplicationTests{

    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RsetHighLevelClientForAggsTests(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * min max sum 桶中只有一个返回值
     */
    @Test
    public void testAggsFunction() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fruit");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.sum("sum_price").field("price"))
                .size(0);
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = search.getAggregations();
        ParsedSum sum_price = aggregations.get("sum_price");
        System.out.println(sum_price.getValue());
    }


    /**
     * 基于term 类型进行聚合 基于字段进行分组聚合
     */
    @Test
    public void testAgges() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fruit");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.terms("price_group").field("price"))
                .size(0);
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = search.getAggregations();

        ParsedDoubleTerms price_group = aggregations.get("price_group");
        List<? extends Terms.Bucket> buckets = price_group.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey()+" "+bucket.getDocCount());
        }

    }
}
