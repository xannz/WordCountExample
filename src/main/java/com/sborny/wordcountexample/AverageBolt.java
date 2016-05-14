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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import org.apache.commons.collections4.queue.CircularFifoQueue;

/**
 *
 * @author sander
 */
public class AverageBolt extends BaseBasicBolt{
    
    private HashMap<String, CircularFifoQueue<Integer>> averages = new HashMap<String, CircularFifoQueue<Integer>>();
    private final Integer SIZEQUEUE = 2;
    //HashMap<String, Queue<Integer>> averages;
    
    void addToQueue(String loc, Integer conc){
        if (! averages.containsKey(loc)){
            Queue<Integer> q = new CircularFifoQueue<>(SIZEQUEUE);
            averages.put(loc, (CircularFifoQueue<Integer>) q);
        }            
        averages.get(loc).add(conc);        
    }
    
    int calcAverage(String loc){
        int sum = 0;
        Iterator it = averages.get(loc).iterator();
        while(it.hasNext()){
            sum += (Integer)it.next();
        }        
        return sum/SIZEQUEUE;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("collectionId", "location", "avg"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        String collectionId = tuple.getStringByField("collectionId");
        String location = tuple.getStringByField("location");
        String concentration = tuple.getStringByField("concentration");
        addToQueue(location, Integer.parseInt(concentration));
        Integer avg = calcAverage(location);
        boc.emit(new Values(collectionId, location, avg.toString()));
    }
    
}
