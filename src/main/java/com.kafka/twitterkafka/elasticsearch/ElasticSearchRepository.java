package com.kafka.twitterkafka.elasticsearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Repository
public class ElasticSearchRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchRepository.class);

    @Autowired
    private RestHighLevelClient CLIENT;

    private String INDEX = "twitter";

    public void save(String inJson) {
        IndexRequest indexRequest = new IndexRequest()
                .index(INDEX)
                .source(inJson, XContentType.JSON)
                .timeout(TimeValue.timeValueSeconds(1))
                .opType(DocWriteRequest.OpType.INDEX);


        CLIENT.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOGGER.info("The record has been saved to database::" +
                        "\nID:-> " + indexResponse.getId() +
                        "\nINDEX:-> " + indexResponse.getIndex() +
                        "\nRESULT:-> " + indexResponse.getResult() +
                        "\nVERSION:-> " + indexResponse.getVersion() +
                        "\nSHARD:-> " + indexResponse.getShardInfo()
                );
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.error("Could not save this record into database.... " +
                        "\nERROR :-> " + e);
            }
        });
    }
}
