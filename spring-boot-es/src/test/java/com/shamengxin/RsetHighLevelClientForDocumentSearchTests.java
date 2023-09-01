package com.shamengxin;

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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.management.Descriptor;
import java.io.IOException;
import java.util.Map;

public class RsetHighLevelClientForDocumentSearchTests extends SpringBootEsApplicationTests{

    private RestHighLevelClient restHighLevelClient;

    @Autowired
    public RsetHighLevelClientForDocumentSearchTests(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * query :精确查询，根据文档得分进行返回
     * filter query：过滤查询 用来在大量数据中筛选出芬迪查询相关数据 不会计算文档的分
     */
    @Test
    public void testFilterQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .postFilter(QueryBuilders.rangeQuery("price").gt(0).lt(10));
        searchRequest.source(sourceBuilder);

        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("符合条件总数："+search.getHits().getTotalHits().value);
        System.out.println("获取文档最大分数："+search.getHits().getMaxScore());
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("id:"+ hit.getId());
            System.out.println("source: "+hit.getSourceAsString());
        }
    }

    /**
     * 分页查询
     */
    @Test
    public void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false).field("description").preTags("<span style='color:red'>").postTags("</span>");
        sourceBuilder.query(QueryBuilders.termQuery("description","好吃"))
                        .from(0) //起始位置
                        .size(10) //每页显示总条数
                        .sort("price", SortOrder.DESC)//1.排序字段 2.排序方式
                        .fetchSource(new String[]{},new String[]{"price"})
                        .highlighter(highlightBuilder);//1.包含字段数组 2.排除字段数组
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("符合条件总数："+search.getHits().getTotalHits().value);
        System.out.println("获取文档最大分数："+search.getHits().getMaxScore());
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("id:"+ hit.getId());
            System.out.println("source: "+hit.getSourceAsString());
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (highlightFields.containsKey("description")){
                System.out.println("description高亮结果："+highlightFields.get("description").fragments()[0]);
            }
        }
    }

    /**
     * 不同条件查询 term （关键词查询）
     */
    @Test
    public void testquery() throws IOException {

        //1.term查询
        // query(QueryBuilders.termQuery("description","浣熊"));
        //2.range查询
        // query(QueryBuilders.rangeQuery("price").gt(0).lt(10));
        //3.prefix前缀查询
        // query(QueryBuilders.prefixQuery("title","小浣"));
        //4.wildcard 通配符查询
        // query(QueryBuilders.wildcardQuery("title","小浣熊*"));
        //5.ids 多个指定id查询
        // query(QueryBuilders.idsQuery().addIds("1").addIds("2"));
        //6.multi_match 多字段查询
        query(QueryBuilders.multiMatchQuery("非常好吃","title","description"));
    }

    public void query(QueryBuilder queryBuilder) throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("符合条件总数："+search.getHits().getTotalHits().value);
        System.out.println("获取文档最大分数："+search.getHits().getMaxScore());
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("id:"+ hit.getId());
            System.out.println("source: "+hit.getSourceAsString());
        }
    }

    /**
     * 查询所有
     */
    @Test
    public void testMatchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest("products");//指定搜索的索引内容
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());//查询所有
        searchRequest.source(sourceBuilder);//指定查询条件
        SearchResponse search = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println("总条数："+ search.getHits().getTotalHits().value);
        System.out.println("最大得分："+search.getHits().getMaxScore());
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            System.out.println("id: "+id+"source:"+hit.getSourceAsString());
        }
    }
}



