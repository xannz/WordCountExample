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

/**
 *
 * @author sander
 */
public class JsonBolt extends BaseBasicBolt{

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("message"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        String collectionId = tuple.getStringByField("collectionId");
        String location = tuple.getStringByField("location");
        String avg = tuple.getStringByField("avg");
        
        String json_message = "{ \"message\": { \"collectionId\": \"" + collectionId + "\", \"message\": {" +
                "\"location\": \"" + location + "\", \"avg\": \"" + avg + "\" } } }";
        
        boc.emit(new Values(json_message));
    }
    
}
