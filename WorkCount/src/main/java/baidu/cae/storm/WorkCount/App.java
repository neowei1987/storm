package baidu.cae.storm.WorkCount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import baidu.cae.storm.WorkCount.WordReader;
import baidu.cae.storm.WorkCount.WordNormalizer;
import baidu.cae.storm.WorkCount.WordCounter;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException, AlreadyAliveException, InvalidTopologyException
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));
        
        Config conf = new Config();
        conf.put("wordFile", args[0]);
        conf.setDebug(true);
        
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-started-topologie", conf, builder.createTopology());
        Thread.sleep(111111111111111111L);
        cluster.shutdown();
        //StormSubmitter.submitTopology("Getting-started-topologie", conf, builder.createTopology());
    }
}
