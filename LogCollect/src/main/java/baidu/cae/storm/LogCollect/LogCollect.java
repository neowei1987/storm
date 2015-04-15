package baidu.cae.storm.LogCollect;

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
import baidu.cae.storm.LogCollect.WordNormalizer;
import baidu.cae.storm.LogCollect.LogReader;

/**
 * Hello world!
 *
 */
public class LogCollect 
{
    public static void main( String[] args ) throws InterruptedException, AlreadyAliveException, InvalidTopologyException
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("log-reader", new QasNoticeLogReader(1234));
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("log-reader");
        builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));
        
        Config conf = new Config();
        conf.put("query_pv_file", "xxx");
        conf.put("smart_se_notice_log", "xxx");
        conf.put("smart_se_wf_log", "xxx");
        conf.put("qas_notice_log", "xxx");
        conf.put("qas_wf_log", "xxx");
      
        conf.setDebug(false);
                
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-started-topologie", conf, builder.createTopology());
        Thread.sleep(1000000);
        cluster.shutdown();
        //StormSubmitter.submitTopology("Getting-started-topologie", conf, builder.createTopology());
 
    }
}
