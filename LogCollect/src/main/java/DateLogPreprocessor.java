import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.io.Text;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DateLogPreprocessor implements IRichBolt {

	private OutputCollector _collector;
	private SimpleDateFormat _date_format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_date_format.setTimeZone(TimeZone.getTimeZone("GMT+8"));
	}

	public void execute(Tuple input) {
		String log = input.getString(0);
		String[] inputs = log.split("NOTICE: | xsp \\* | \\{q=|,city=|,ck=|,qid=|,pn=|,tn=");
		if (inputs.length >= 8)
		{
			String time = inputs[1];
			String query = inputs[3];
			String baiduId = inputs[5];
			String qid = inputs[6];
			String pageNo = inputs[7];
			
			if (!query.isEmpty() && !time.isEmpty()  && !qid.isEmpty() && !pageNo.isEmpty())
			{
				long lTime = 0;
				try {
					lTime = (_date_format.parse(time)).getTime() / 1000;
				} catch (Exception e) {
					e.printStackTrace();
					lTime = 0;
				}
				
				if (lTime != 0)
				{
					//按日期分发
					_collector.emit(new Values(lTime / 86400 * 86400 - TimeZone.getDefault().getRawOffset() / 1000, time, qid, query, baiduId, pageNo));
				}
			}
		}
		
		_collector.ack(input);
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date", "time", "qid", "query", "baiduId", "pageNo"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
