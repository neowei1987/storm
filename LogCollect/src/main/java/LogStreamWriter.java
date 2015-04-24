import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;












import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;

public class LogStreamWriter implements IRichBolt {
	
	private OutputCollector collector;
	HTable _xsp_log_stream_table;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		Configuration configuration = new Configuration();
		configuration = HBaseConfiguration.addHbaseResources(configuration);
		
		configuration.set("hbase.zookeeper.quorum", "cp01-ma-eval-001.cp01.baidu.com");
		configuration.set("hbase.zookeeper.property.clientPort", "8071");
		configuration.set("hbase.master.port", "8070");
		configuration.set("hbase.regionserver.port", "8072");
		
        try {
        	HBaseAdmin admin = new HBaseAdmin(configuration);   
        	_xsp_log_stream_table = new HTable(configuration, "xsp_log_stream");
        	//_xsp_log_stream_table = (HTable) admin.getConnection().getTable("xsp_log_stream");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		String sentence = input.getString(0);
		System.out.println(sentence);
		Put p = new Put(Bytes.toBytes(String.valueOf(Time.currentTimeSecs())));
		p.add(Bytes.toBytes("log"), Bytes.toBytes("log"), Bytes.toBytes(sentence));
		try {
			_xsp_log_stream_table.put(p);
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.collector.ack(input);
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
