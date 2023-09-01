package com.shamengxin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shamengxin.entity.Products;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RsetHighLevelClientForObjectTests extends SpringBootEsApplicationTests{

    private RestHighLevelClient restHighLevelClient;

    @Autowired
    public RsetHighLevelClientForObjectTests(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @Test
    public void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field("description").preTags("<span style='color:red'>").postTags("</span>");
        sourceBuilder.query(QueryBuilders.termQuery("description","浣熊"))
                .from(0)
                .size(10)
                .highlighter(highlightBuilder);
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(search.getHits().getTotalHits());
        System.out.println(search.getHits().getMaxScore());

        List<Products> productsList = new ArrayList<>();
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
            Products products = new ObjectMapper().readValue(hit.getSourceAsString(), Products.class);

            //处理高亮
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (highlightFields.containsKey("description")){
                Text description = highlightFields.get("description").fragments()[0];
                products.setDescription(description.toString());
            }

            productsList.add(products);
        }

        for (Products products : productsList) {
            System.out.println(products);
        }
    }

    /**
     * 将对象放入es中
     */
    @Test
    public void testIndex ()throws Exception{
        Products products = new Products();
        products.setId(1);
        products.setTitle("小浣熊干脆面");
        products.setPrice(2.5);
        products.setDescription("小浣熊真好吃");

        //录入es中
        IndexRequest indexRequest = new IndexRequest("products");
        indexRequest.id(products.getId().toString())
                .source(new ObjectMapper().writeValueAsString(products),XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index.status());
    }
}



