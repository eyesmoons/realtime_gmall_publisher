package com.eyesmoons.gmall2020publisher.service.impl;

import com.eyesmoons.gmall2020publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    private JestClient jest;

    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();

        System.out.println(query);

        String indexName = "gmall_dau_info" + date + "-query";
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();
        Long total = 0L;
        try {
            SearchResult searchResult = jest.execute(search);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        Map<String, Long> map = new HashMap<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("dau_hour_agg").field("_2.hr.keyword").size(24);
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        String query = searchSourceBuilder.toString();

        System.out.println(query);

        String indexName = "gmall_dau_info" + date + "-query";
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();

        try {
            SearchResult searchResult = jest.execute(search);
            MetricAggregation agg = searchResult.getAggregations();
            TermsAggregation dau_hour_agg = agg.getTermsAggregation("dau_hour_agg");
            if (dau_hour_agg != null) {
                List<TermsAggregation.Entry> buckets = dau_hour_agg.getBuckets();
                if (buckets != null && buckets.size() > 0) {
                    for (TermsAggregation.Entry bucket : buckets) {
                        String key = bucket.getKey();
                        Long value = bucket.getCount();
                        map.put(key,value);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }
}
