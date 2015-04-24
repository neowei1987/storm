import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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

public class LogCollect 
{
    public static void main( String[] args ) throws InterruptedException, AlreadyAliveException, InvalidTopologyException
    {
    	
    	DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    	Calendar myCalender = Calendar.getInstance();
    	Date d = null;
    	try {
			d = df.parse("2015-04-23");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	myCalender.setTimeInMillis(d.getTime());
    	
    	PropertyConfigurator.configure("log4j.properties");
    	
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("log-reader", new LogReader(1234));
        builder.setSpout("sync-timer", new HDFSSyncer(Integer.parseInt(args[0])));
        //builder.setBolt("hbase-stream-log", new LogStreamWriter()).shuffleGrouping("log-reader");
        builder.setBolt("preprocess-log-by-date", new DateLogPreprocessor(), 2).shuffleGrouping("log-reader");
        builder.setBolt("hdfs-write-log-by-date", new LogHdfsWriter(), 2)
        		.fieldsGrouping("preprocess-log-by-date", new Fields("date"))
        		.allGrouping("sync-timer");
        
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        conf.setDebug(true);
        
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("log_collect", conf, builder.createTopology());
        //Thread.sleep(100000000000L);
        //cluster.shutdown();
  
        StormSubmitter.submitTopology("log_collect", conf, builder.createTopology());
    }
}
