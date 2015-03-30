package baidu.cae.storm.SingleJoin;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class App 
{
    public static void main( String[] args ) throws InterruptedException
    {
    	FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));
    	FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
    	
    	TopologyBuilder builder = new TopologyBuilder();
    	builder.setSpout("age", ageSpout);
    	builder.setSpout("gender", genderSpout);
    	builder.setBolt("single-join", new SingleJoinBolt(new Fields("age", "gender")))
    			.fieldsGrouping("age", new Fields("id"))
    			.fieldsGrouping("gender", new Fields("id"));
    	
        Config conf = new Config();
        conf.setDebug(false);
        
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("single-join", conf, builder.createTopology());
        
        for (int i = 0; i < 100000; ++i)
        {
        	String gender;
        	if (i % 2 == 0)
        	{
        		gender = "male";
        	}
        	else {
        		gender = "female";
        	}
        	
        	genderSpout.feed(new Values(i, gender));
        	ageSpout.feed(new Values(i, 10 + i % 50));
        }
        
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
