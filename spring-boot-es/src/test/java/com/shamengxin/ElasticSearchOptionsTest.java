package com.shamengxin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shamengxin.entity.Products;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Query;


public class ElasticSearchOptionsTest extends SpringBootEsApplicationTests{

    private final ElasticsearchOperations elasticsearchOperations;

    @Autowired
    public ElasticSearchOptionsTest(ElasticsearchOperations elasticsearchOperations) {
        this.elasticsearchOperations = elasticsearchOperations;
    }

    /**
     * save 索引一条文档 更新一条文档
     *  save 方法文档id不存在时添加文档，当文档id存在时，更新文档
     */
    @Test
    public void testIndex(){
        Products products = new Products();
        products.setId(2);
        products.setTitle("小浣熊干脆面");
        products.setPrice(5.5);
        products.setDescription("小浣熊干脆面真好吃，曾经非常爱吃");
        elasticsearchOperations.save(products);
    }

    /**
     * 查询一条文档
     */
    @Test
    public void testSearch(){
        Products products = elasticsearchOperations.get("1", Products.class);
        System.out.println(products);
    }

    /**
     * 删除一条文档
     */
    @Test
    public void testDelete(){
        Products products = new Products();
        products.setId(1);
        elasticsearchOperations.delete(products);
    }

    /**
     * 删除所有
     */
    @Test
    public void testDeleteAll(){
        elasticsearchOperations.delete(Query.findAll(), Products.class);
    }

    /**
     *查询所有
     */
    @Test
    public void testQueryAll() throws JsonProcessingException {
        SearchHits<Products> search = elasticsearchOperations.search(Query.findAll(), Products.class);
        System.out.println("总分数："+search.getMaxScore());
        System.out.println("符合条件的总条数："+search.getTotalHits());
        for (SearchHit<Products> productsSearchHit : search) {
            System.out.println(new ObjectMapper().writeValueAsString(productsSearchHit.getContent()));
        }
    }
}
