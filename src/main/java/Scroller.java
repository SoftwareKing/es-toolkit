import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.function.Consumer;

/**
 * Created by art on 15.03.15.
 * <p>
 * ElasticSearch index scrollers: scroll over all search results by a given query
 */
public class Scroller {

    private final Client client;
    private final String index;
    private final String indexType;


    public Scroller(Client client, final String index, final String indexType) {
        this.client = client;
        this.index = index;
        this.indexType = indexType;
    }

    /**
     * scrolls over the index
     * @param consumer callback that will process the scroll response
     * @param scrollSize number of hits per scroll
     * @param timeValue how long the scoll is valid
     * @param query the search query
     * @param fields fields (leave empty if you want to get the whole doc)
     */
    public void scroll(final Consumer<SearchResponse> consumer,
                       final int scrollSize,
                       final TimeValue timeValue,
                       final QueryBuilder query,
                       final String... fields) {
        SearchResponse scrollResp = client.prepareSearch(index)
                .setSearchType(SearchType.SCAN)
                .setScroll(timeValue)
                .setQuery(query)
                .addFields()
                .setSize(scrollSize)
                .execute().actionGet();
        // process results
        consumer.accept(scrollResp);
        while (true) {
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(timeValue)
                    .execute()
                    .actionGet();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            } else {
                consumer.accept(scrollResp);
            }
        }
    }
    /**
     *
     * @param consumer
     * @param scrollSize
     * @param fields
     * scrolls over whole index using matchAll query
     */
    public void scroll(final Consumer<SearchResponse> consumer,
                       final int scrollSize,
                       final TimeValue timeValue,
                       final String... fields) {
        scroll(consumer, scrollSize, timeValue, QueryBuilders.matchAllQuery(), fields);
    }

    /**
     *
     * @param consumer
     * @param scrollSize
     * @param fields
     * scrolls over index using
     *  + matchAll query
     *  + scrollsize: 1000
     */
    public void scroll(final Consumer<SearchResponse> consumer,
                       final int scrollSize,
                       final String... fields) {
        scroll(consumer, scrollSize, new TimeValue(60_000L), QueryBuilders.matchAllQuery(), fields);
    }

    /**
     * scrolls over index using
     *  + matchAll query
     *  + scrollsize: 1000
     *  + timeValue: 60 sec
     * @param consumer
     * @param fields
     */
    public void scroll(final Consumer<SearchResponse> consumer,
                       final String... fields) {
        scroll(consumer, 1_000, new TimeValue(60_000L), QueryBuilders.matchAllQuery(), fields);
    }

}
