package baidu.cae.storm.WorkCount;

import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader implements IRichSpout {

	private TopologyContext context;
	private FileReader fileReader;
	private SpoutOutputCollector collector;
	private Boolean complete = false;
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.context = context;
			this.fileReader = new FileReader(conf.get("wordFile").toString());
		}
		catch (FileNotFoundException e) {
			throw new RuntimeException("Error read file [" + conf.get("wordFile") + "]");
		}
		this.collector = collector;
	}

	public void close() {
		System.out.println("Close");
	}

	public void activate() {

	}

	public void deactivate() {

	}

	public void nextTuple() {
		if (complete) {
			try {
				Thread.sleep(1);
			}
			catch (InterruptedException e) {
				
			}
			return;
		}
		
		String str;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((str = reader.readLine()) != null) {
				this.collector.emit(new Values(str));
			}
		}
		catch (Exception e) {
			throw new RuntimeException("Error reading tuple ", e);
		}
		finally {
			complete = true;
		}
	}

	public void ack(Object msgId) {
		System.out.println("OK: " + msgId);

	}

	public void fail(Object msgId) {
		System.out.println("Fail: " + msgId);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
