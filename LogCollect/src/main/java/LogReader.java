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

public class LogReader implements IRichSpout {

	private TopologyContext context;
	private SpoutOutputCollector collector;
	private int port;
	private static BlockingQueue<String> dataQueue = new LinkedBlockingQueue<String>();
	private static LogReceiveServer _server;
	
	public LogReader(int port)
	{
		this.port = port;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this.context = context;
		this.collector = collector;
		
		if (_server == null)
		{
			_server = new LogReceiveServer(this.port, dataQueue);
			_server.startUp();
		}
	}

	public void close() {
		System.out.println("Close");
	}

	public void activate() {

	}

	public void deactivate() {

	}

	public void nextTuple() {
		
		String data = null;
		try {
			data = dataQueue.poll(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (data == null)
		{
			return;
		}
		
		System.out.println(data);
		
		this.collector.emit(new Values(data));
	}

	public void ack(Object msgId) {
		System.out.println("OK: " + msgId);

	}
	
	public void fail(Object msgId) {
		System.out.println("Fail: " + msgId);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
