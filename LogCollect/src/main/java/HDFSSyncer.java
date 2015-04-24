import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.net.*;

import org.apache.hadoop.util.Time;

public class HDFSSyncer implements IRichSpout {

	private TopologyContext context;
	private SpoutOutputCollector collector;
	private long syncInterval;
	private long lastSyncTime;
	
	public HDFSSyncer(int syncInterval)
	{
		this.syncInterval = syncInterval;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this.context = context;
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
		
		long curTime = System.currentTimeMillis() / 1000;
		if (curTime - lastSyncTime >= syncInterval)
		{
			lastSyncTime = curTime;
			this.collector.emit(new Values("sync at: " + lastSyncTime));
			return;
		}
		else
		{
			try {
				Thread.sleep(syncInterval * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void ack(Object msgId) {
		System.out.println("OK: " + msgId);

	}
	
	public void fail(Object msgId) {
		System.out.println("Fail: " + msgId);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("cmd"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
