package baidu.cae.storm.WorkCount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounter implements IRichBolt {

	private OutputCollector collector;
	private Map<String, Integer> counter;
	private String name;
	private Integer id;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counter = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	public void execute(Tuple input) {
		String word = input.getString(0);
		if (!counter.containsKey(word))
		{
			counter.put(word, 1);
		}
		else {
			int c = counter.get(word) + 1;
			counter.put(word, c);
		}
		
		this.collector.ack(input);

	}

	public void cleanup() {
		System.out.println("--Word Count [" + name + "-" + id);
		for (Map.Entry<String, Integer> entry : this.counter.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
