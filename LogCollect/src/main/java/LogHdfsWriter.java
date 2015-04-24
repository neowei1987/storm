import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogHdfsWriter implements IRichBolt {

	private OutputCollector _collector;
	private static final SimpleDateFormat _date_format =  new SimpleDateFormat("yyyy-MM-dd");
	private static final String BASE_OUT_PUT_DIR = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/xsp/format_log/";
	private Map<Long, FSDataOutputStream> _date2LogWriter = new HashMap<Long, FSDataOutputStream>();
	FileSystem _hdfs = null;
	private int _counter = 0;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		
		Configuration conf = new Configuration();
	    if (true)
	    {
	        conf.set("fs.defaultFS", "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020");
	        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
	        
	        //conf.set("mapreduce.framework.name", "yarn");
	       // conf.set("yarn.resourcemanager.address", "cp01-ma-eval-001.cp01.baidu.com:8032");	
	        //conf.set("yarn.resourcemanager.resource-tracker.address", "cp01-ma-eval-001.cp01.baidu.com:8031");
	        //conf.set("yarn.resourcemanager.scheduler.address", "cp01-ma-eval-001.cp01.baidu.com:8030");
	        
	        //conf.set("mapreduce.jobhistory.address", "cp01-ma-eval-001.cp01.baidu.com:8044");
	        //conf.set("mapreduce.jobhistory.webapp.address", "cp01-ma-eval-001.cp01.baidu.com:8045");
	        //conf.set("mapreduce.jobhistory.done-dir", "/history/done");
	        //conf.set("mapreduce.jobhistory.intermediate-done-dir", "/history/done_intermediate");

	        //conf.set("mapreduce.app-submission.cross-platform", "true");
	        //conf.set("mapreduce.job.am-access-disabled", "true");
	    }
		
		try {
			_hdfs = FileSystem.get(new URI(BASE_OUT_PUT_DIR), conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		if (input.getSourceComponent().equals("sync-timer"))
		{
			_collector.ack(input);
			for (Entry<Long, FSDataOutputStream> entry : _date2LogWriter.entrySet())
			{
				try {
					entry.getValue().hflush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
			}
			return;
		}
		
		//collector.emit(new Values(lTime / 86400 * 86400, time, qid, query, baiduId, pageNo));
		long date = input.getLongByField("date");
		String time = input.getStringByField("time");
		String qid = input.getStringByField("qid");
		String query = input.getStringByField("query");
		String baiduId = input.getStringByField("baiduId");
		String pageNo = input.getStringByField("pageNo");
		
	
		FSDataOutputStream hdfsStream = _date2LogWriter.get(date);
		if (hdfsStream == null)
		{
			String strDate = _date_format.format(new Date(date * 1000));
			Path outputPath = new Path(BASE_OUT_PUT_DIR + strDate + "/" + "xsp.log.formated");
			
			try {
				if (_hdfs.exists(outputPath))
				{
					hdfsStream = _hdfs.append(outputPath);
				}
				else
				{
					hdfsStream = _hdfs.create(outputPath, false);
				}
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
			
			_date2LogWriter.put(date, hdfsStream);
		}
	
		try {
			hdfsStream.writeBytes(time + "\t" + qid + "\t" + query + "\t" + baiduId + "\t" + pageNo + "\n");;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		_collector.ack(input);
	}

	public void cleanup() {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
