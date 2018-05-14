package org.wiki.ElasticDemo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import com.jayway.jsonpath.JsonPath;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONException;
import org.json.JSONObject;

public class SearchDemo {
    private static ObjectNode objectNode1;



    public static void main(String args[]) throws IOException, JSONException

    {

        RestClient restClientobj = RestClient.builder(new HttpHost("131.234.29.15", 6060, "http")).build();

        HttpEntity entity1 = new NStringEntity(
              /*  "{\n" +
                        "              \"size\" : 100000 ,\n" +
                        "    \"query\" : {\n" +
                        "   \"bool\": {\n"+
                        "              \"must\":[\n"+
                        "{\n"+
                        "    \"match\" : {\n"+
                        "                \"dbpediaRelation\" : \"award\"\n"+
                        "} \n"+
                        "}, \n"+
                        "{\n"+
                        "    \"match\" : {\n"+
                        "                \"pattern\" : \"awarded\"\n"+
                        "} \n"+
                        "} \n"+
                        "] \n"+
                        "} \n"+
                        "} \n"+
                        "}"*/
                "{\n" +
                        " \"from\" : 0,  \"size\" : 90354,\n" +
                        " \"query\":{\n" +
                        " \"match_all\":{\n"+
                        "} \n"+
                        "} \n"+
                        "} \n"  ,ContentType.APPLICATION_JSON);
        //System.out.println(entity1.toString());
        Response response = restClientobj.performRequest("GET", "wiki-factcheck/topicterms/_search", Collections.singletonMap("pretty", "true"), entity1);
        String json = EntityUtils.toString(response.getEntity());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode hits = mapper.readTree(json);
        JsonNode count = hits.get("hits").get("total");

        for(int i=0; i<count.asInt(); i++)
        {
            int jid = hits.get("hits").get("hits").get(i).get("_id").asInt();

            JSONObject obj = new JSONObject();
            JSONObject arrayElementOne = new JSONObject();
            JSONObject id = arrayElementOne.put("_id", jid);
            obj.put("index", arrayElementOne);
            System.out.println(jid);

            Double Coherence_NPMI =hits.get("hits").get("hits").get(i).get("_source").get("Coherence_NPMI").asDouble();
            Double Coherence_UCI  =hits.get("hits").get("hits").get(i).get("_source").get("Coherence_UCI").asDouble();
            String Term           =hits.get("hits").get("hits").get(i).get("_source").get("Term").asText();
            String Topic          =hits.get("hits").get("hits").get(i).get("_source").get("Topic").asText();

            JSONObject data = new JSONObject();

            data.put("Topic", Topic);
            data.put("Term", Term);
            data.put("NPMI", Coherence_NPMI);
            data.put("UCI",Coherence_UCI );



            String filename= "D://test/test2.json";
            FileWriter file = new FileWriter(filename,true); //the true will append the new data
            file.write(obj.toString());
            file.write("\n");
            file.write(data.toString());
            file.write("\n");
            file.close();



        }

        // bw.close();
        restClientobj.close();


    }


    public static double roundToDouble(double d, int decimalPlace) {
        BigDecimal bd = new BigDecimal(Float.toString((float) d));
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP);
        return bd.doubleValue();
    }
}
