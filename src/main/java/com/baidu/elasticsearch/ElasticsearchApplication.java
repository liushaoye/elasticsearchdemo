package com.baidu.elasticsearch;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Administrator
 */
@SpringBootApplication(scanBasePackages = {"com.baidu.conf"})
@RestController
public class ElasticsearchApplication {

    private final TransportClient client;

    @Autowired
    public ElasticsearchApplication(TransportClient client) {
        this.client = client;
    }


    @GetMapping("/")
    public String index() {
        return "index";
    }

    /**
     * 查询一条数据
     * 访问:http://localhost:8080/get/book/novel?id=1
     *
     * @param id
     * @return
     */
    @GetMapping("/get/book/novel")
    public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {

        if (id.isEmpty()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        GetResponse getResponse = this.client.prepareGet("book", "novel", id).get();

        if (!getResponse.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        return new ResponseEntity(getResponse.getSource(), HttpStatus.OK);
    }

    /**
     * 添加接口
     * 访问 :http://localhost:8080/add/book/novel
     *
     * @param title
     * @param author
     * @param wordCount
     * @param publicDate
     * @return
     */
    @PostMapping("/add/book/novel")
    public ResponseEntity add(@RequestParam(name = "title") String title,
                              @RequestParam(name = "author") String author,
                              @RequestParam(name = "word_count") String wordCount,
                              @RequestParam(name = "public_date")
                              @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                                      String publicDate

    ) {
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                    .field("title", title)
                    .field("author", author)
                    .field("word_count", wordCount)
                    .field("public_date", publicDate).endObject();

            IndexResponse getResponse = this.client.prepareIndex("book", "novel").setSource(xContentBuilder).get();

            return new ResponseEntity(getResponse.getId(), HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 删除
     * 访问:http://localhost:8080/delete/book/novel?id=1
     *
     * @param id
     * @return
     */
    @DeleteMapping("/delete/book/novel")
    public ResponseEntity deleted(@RequestParam(name = "id") String id) {

        DeleteResponse deleteResponse = this.client.prepareDelete("book", "novel", id).get();

        return new ResponseEntity(deleteResponse.getResult().toString(), HttpStatus.OK);
    }

    /**
     * 修改
     * 访问:http://localhost:8080/update/book/novel
     *
     * @param id
     * @param title
     * @param author
     * @param wordCount
     * @param publicDate
     * @return
     */
    @PutMapping("/update/book/novel")
    public ResponseEntity update(
            @RequestParam(name = "id") String id,
            @RequestParam(name = "title", required = false) String title,
            @RequestParam(name = "author", required = false) String author,
            @RequestParam(name = "word_count", required = false) String wordCount,
            @RequestParam(name = "public_date", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                    String publicDate

    ) {
        UpdateRequest updateRequest = new UpdateRequest("book", "novel", id);
        try {

            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();

            if (title != null) {
                xContentBuilder.field("title", title);
            }

            if (author != null) {
                xContentBuilder.field("author", author);
            }
            if (wordCount != null) {
                xContentBuilder.field("word_count", wordCount);
            }

            if (publicDate != null) {
                xContentBuilder.field("public_date", publicDate);
            }

            xContentBuilder.endObject();

            updateRequest.doc(xContentBuilder);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        try {
            UpdateResponse updateResponse = this.client.update(updateRequest).get();

            return new ResponseEntity(updateResponse.getResult().toString(), HttpStatus.OK);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    /**
     * 复合查询
     * 访问:
     *
     * @param title
     * @param author
     * @param gtWordCount
     * @param ltWordCount
     * @return
     */
    @PostMapping("/query/book/novel")
    public ResponseEntity query(@RequestParam(name = "title", required = false) String title,
                                @RequestParam(name = "author", required = false) String author,
                                @RequestParam(name = "gt_word_count", defaultValue = "0") int gtWordCount,
                                @RequestParam(name = "lt_word_count", required = false) Integer ltWordCount

    ) {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (author != null) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("author", author));
        }

        if (title != null) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("title", title));
        }

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("word_count").from(gtWordCount);

        if (ltWordCount != null && ltWordCount > 0) {

            rangeQueryBuilder.to(ltWordCount);
        }

        boolQueryBuilder.filter(rangeQueryBuilder);


        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("book")
                .setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder)
                .setFrom(0)
                .setSize(10);

        System.out.println(searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();
        List<Map<String, Object>> mapList = new ArrayList<>();

        for (SearchHit searchHit : searchResponse.getHits()) {
            mapList.add(searchHit.getSource());
        }

        return new ResponseEntity(mapList, HttpStatus.OK);
    }


    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApplication.class, args);
    }
}
