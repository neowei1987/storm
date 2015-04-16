package baidu.cae.storm.LogCollect;

import org.apache.log4j.PropertyConfigurator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import baidu.cae.storm.LogCollect.WordCounter;
import baidu.cae.storm.LogCollect.LogStreamWriter;
import baidu.cae.storm.LogCollect.LogReader;

public class LogCollect 
{
    public static void main( String[] args ) throws InterruptedException, AlreadyAliveException, InvalidTopologyException
    {
    	PropertyConfigurator.configure("log4j.properties");
    	
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("log-reader", new LogReader(1234));
        
        builder.setBolt("hbase-stream-log", new LogStreamWriter()).shuffleGrouping("log-reader");
        builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("hbase-stream-log", new Fields("word"));
        
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        conf.setDebug(true);
                
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("log_collect", conf, builder.createTopology());
        Thread.sleep(100000000000L);
        cluster.shutdown();
        //StormSubmitter.submitTopology("Getting-started-topologie", conf, builder.createTopology());
 
    }
}
