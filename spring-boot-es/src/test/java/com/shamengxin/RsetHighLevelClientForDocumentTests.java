package com.shamengxin;

import com.shamengxin.entity.Products;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class RsetHighLevelClientForDocumentTests extends SpringBootEsApplicationTests{

    private RestHighLevelClient restHighLevelClient;

    @Autowired
    public RsetHighLevelClientForDocumentTests(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 基于id查询文档
     */
    @Test
    public void testQueryById() throws IOException {
        GetRequest getRequest = new GetRequest("products","1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("id: "+getResponse.getId());
        System.out.println("source: "+getResponse.getSourceAsString());

    }

    /**
     * 删除文档
     */
    @Test
    public void testDelete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("products","2");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    /**
     * 更新文档
     */
    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("products","1");
        updateRequest.doc("{\"title\":\"小浣熊干脆面\"}",XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }

    /**
     * 索引一条文档
     */
    @Test
    public void testCreate() throws IOException {
        //参数1：索引请求对象 参数2：请求配置对象
        IndexRequest indexRequest = new IndexRequest("products");
        indexRequest
                .id("2")
                .source("{\"title\":\"日本豆\",\"price\":\"2.5\",\"created_at\":\"2012-12-11\",\"description\":\"日本豆非常好吃！\"}", XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index.status());
    }
}



