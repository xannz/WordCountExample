/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sborny.wordcountexample;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author sander
 */
public class PrepareBolt extends BaseBasicBolt{
    private String message;

    public void prepare(String message){
        this.message = message;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("location", "collectionId", "concentration"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        JSONObject obj = new JSONObject(tuple.getStringByField(message));
        try {
            System.out.println("START_PREPARING_KAFKA_DATA");
            String collectionId = obj.getString("collectionId");
            JSONObject json = obj.getJSONObject(message);
            String location = json.getString("location");
            String concentration = json.getString("concentration");
            boc.emit(new Values(location, collectionId, concentration));            
        } catch (JSONException ex) {
            System.out.println("ERROR PARSING JSON");
        }
    }
    
}
