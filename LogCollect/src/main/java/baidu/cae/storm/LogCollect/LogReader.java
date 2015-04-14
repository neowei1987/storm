package baidu.cae.storm.LogCollect;

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

	class DataRecvJob implements  Runnable {
		DatagramSocket _listen_socket =  null;
		BlockingQueue _dataQueue = null;
		
		public DataRecvJob(int port, BlockingQueue queue) throws SocketException 
		{
			_listen_socket = new DatagramSocket(port);
			_dataQueue = queue;
		}
		
		public void run() 
		{
			while (true)
			{
				byte[] buffer = new byte[10240];
				DatagramPacket p = new DatagramPacket(buffer, buffer.length);
				try 
				{
					_listen_socket.receive(p);
				}
				catch (IOException e) 
				{
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				
				System.out.println("RECV: " + p.getLength() + "Bytes");
				
				if (p.getLength() >= buffer.length)
				{
					continue;
				}
				
				_dataQueue.offer(p.getData());
			}
		}
	}
	
	private TopologyContext context;
	private SpoutOutputCollector collector;
	private static BlockingQueue dataQueue = new LinkedBlockingQueue<byte[]>();
	private static Thread dataRecvThread = null;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this.context = context;
		this.collector = collector;
		
		if (dataRecvThread == null)
		{
			try {
				dataRecvThread = new Thread(new DataRecvJob(8043, dataQueue));
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			dataRecvThread.start();
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
		
		byte[] data = null;
		try {
			data = (byte[]) dataQueue.poll(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (data == null)
		{
			return;
		}
		
		String str = new String(data);
		System.out.println(str);
		this.collector.emit(new Values(str));
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
