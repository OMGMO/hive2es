package com.omg.scala.dao;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import io.searchbox.core.*;
import io.searchbox.indices.*;
import io.searchbox.params.Parameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import com.omg.scala.utils.InitElasticSearchConfig;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.searchbox.client.JestResult;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.cluster.NodesStats;
import io.searchbox.core.SearchResult.Hit;
import io.searchbox.core.SuggestResult.Suggestion;

/**
 * es操作实现类
 */

public class ElasticSearchDaoImpl{

    static protected final Log log = LogFactory.getLog(ElasticSearchDaoImpl.class.getName());

    private InitElasticSearchConfig esConfig ;
    public ElasticSearchDaoImpl(String esUrl){
        this.esConfig = new InitElasticSearchConfig(esUrl);
    }
    
    public JestResult deleteIndex(String index) {
        DeleteIndex deleteIndex = new DeleteIndex.Builder(index).build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(deleteIndex);
            log.info("deleteIndex == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult clearCache() {
        ClearCache closeIndex = new ClearCache.Builder().build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(closeIndex);
            log.info("clearCache == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult closeIndex(String type) {
        CloseIndex closeIndex = new CloseIndex.Builder(type).build(); 
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(closeIndex);
            log.info("closeIndex == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult optimizeIndex() {
        Optimize optimize = new Optimize.Builder().build(); 
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(optimize);
            log.info("optimizeIndex == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult flushIndex() {
        Flush flush = new Flush.Builder().build(); 
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(flush);
            log.info("flushIndex == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult indicesExists(String index) {
        IndicesExists indicesExists = new IndicesExists.Builder(index).build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(indicesExists);
            log.info("indicesExists == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult nodesInfo() {
        NodesInfo nodesInfo = new NodesInfo.Builder().build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(nodesInfo);
            log.info("nodesInfo == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult health() {
        Health health = new Health.Builder().build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(health);
            log.info("health == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult nodesStats() {
        NodesStats nodesStats = new NodesStats.Builder().build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(nodesStats);
            log.info("nodesStats == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult updateDocument(String script , String index, String type, String id) {
        /*String script = "{" +
                "    \"doc\" : {" +
                "        \"title\" : \""+article.getTitle()+"\"," +
                "        \"content\" : \""+article.getContent()+"\"," +
                "        \"author\" : \""+article.getAuthor()+"\"," +
                "        \"source\" : \""+article.getSource()+"\"," +
                "        \"url\" : \""+article.getUrl()+"\"," +
                "        \"pubdate\" : \""+new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(article.getPubdate())+"\"" +
                "    }" +
                "}";*/
        Update update = new Update.Builder(script).index(index).type(type).id(id).build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(update);
            log.info("updateDocument == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result ;
    }

    
    public JestResult deleteDocument(String index, String type, String id) {
        Delete delete = new Delete.Builder(id).index(index).type(type).build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(delete);
            log.info("deleteDocument == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * assume that you have installed deletebyQuery plugin
     * @param index
     * @param type
     * @param params
     * @return
     */
    public JestResult deleteDocumentByQuery(String index, String type, String params) {

        DeleteByQuery db = new DeleteByQuery.Builder(params)
                .addIndex(index)
                .addType(type)
                .build();

        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(db);
            log.info("deleteDocument == " + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    
    public <T> JestResult getDocument(T object , String index, String type, String id) {
        Get get = new Get.Builder(index, id).type(type).build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(get);     
            T o = (T) result.getSourceAsObject(object.getClass());
            for (Method method : o.getClass().getMethods()) {
                log.info("getDocument == " + method.getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;

    }

    
    public List<Suggestion> suggest() {
        String suggestionName = "my-suggestion";
        Suggest suggest = new Suggest.Builder("{" +
                "  \"" + suggestionName + "\" : {" +
                "    \"text\" : \"the amsterdma meetpu\"," +
                "    \"term\" : {" +
                "      \"field\" : \"body\"" +
                "    }" +
                "  }" +
                "}").build();
        SuggestResult suggestResult = null ;
        List<SuggestResult.Suggestion> suggestionList = null ;
        try {
            suggestResult = esConfig.getClient().execute(suggest);
            log.info("suggestResult.isSucceeded() == " + suggestResult.isSucceeded());
            suggestionList = suggestResult.getSuggestions(suggestionName);
            log.info("suggestionList.size() == " + suggestionList.size());
            for(SuggestResult.Suggestion suggestion:suggestionList){
                System.out.println(suggestion.text);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return suggestionList ;
    }

    
    public <T> List<Hit<T, Void>> searchAll(String index , T o) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(index)
                .build();
        SearchResult result = null ;
        List<?> hits = null ;
        try {
            result = esConfig.getClient().execute(search);
            System.out.println("本次查询共查到："+result.getTotal()+"个关键字！");
            log.info("本次查询共查到："+result.getTotal()+"个关键字！");
            hits = result.getHits(o.getClass());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (List<Hit<T, Void>>) hits ;
    }

    
    public <T> List<Hit<T, Void>> createSearch(String keyWord , String type , T o , String... fields) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryStringQuery(keyWord));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        for(String field : fields){
            highlightBuilder.field(field);//高亮field
        }
        highlightBuilder.preTags("<em>").postTags("</em>");//高亮标签
        highlightBuilder.fragmentSize(200);//高亮内容长度
        searchSourceBuilder.highlighter(highlightBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(type).build();
        SearchResult result = null ;
        List<?> hits = null ;
        try {
            result = esConfig.getClient().execute(search);
            System.out.println("本次查询共查到："+result.getTotal()+"个结果！");

            hits = result.getHits(o.getClass());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return (List<Hit<T, Void>>) hits ;
    }

    
    public <T> void bulkIndex(String index, String type , T o) {
        Bulk bulk = new Bulk.Builder()
                .defaultIndex(index)
                .defaultType(type)
                .addAction(Arrays.asList(
                        new Index.Builder(o).build()
                )).build(); 
        try {
            esConfig.getClient().execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JestResult createIndex(String index) {
        CreateIndex index1 =  new CreateIndex.Builder(index).build();
        JestResult jestResult = null ;
        try {
            jestResult = esConfig.getClient().execute(index1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jestResult;
    }

    public <T> JestResult createIndex(T o, String index, String type) {
        Index index1 = new Index.Builder(o).index(index).type(type).build();
        JestResult jestResult = null ;
        try {
            jestResult = esConfig.getClient().execute(index1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jestResult;
    }

    
    public JsonObject searchEvent(String param) {
        JsonObject returnData = new JsonParser().parse(param).getAsJsonObject();
        Search search = new Search.Builder(returnData.toString()).addType("event").addIndex("pi").build();

//      Gson gs = new Gson();
//      System.out.println("输入参数为：" + "\n" + gs.toJson(search));

        SearchResult result = null ;
        try {
            result = esConfig.getClient().execute(search);
//          System.out.println("\n" + gs.toJson(result.getJsonObject()) + "\n" );
//          System.out.println("本次查询共查到：" + "\n" +result.getTotal()+"个结果！");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.getJsonObject();
    }
    
    public JsonObject searchEventHistogramByScroll(String scrollId) {
        SearchScroll scroll = new SearchScroll.Builder(scrollId, "1m").build();
        JestResult result = null ;
        try {
            result = esConfig.getClient().execute(scroll);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.getJsonObject();
    }

    
    public JsonObject searchInitEventHistogram(String param) {
        JsonObject returnData = new JsonParser().parse(param).getAsJsonObject();
        Search search = new Search.Builder(returnData.toString())
                .addIndex("pi")
                .addType("event")
                .setParameter(Parameters.SCROLL, "1m")
                .build();

        JestResult result = null;

        try {
            result = esConfig.getClient().execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.getJsonObject();
    }

}
