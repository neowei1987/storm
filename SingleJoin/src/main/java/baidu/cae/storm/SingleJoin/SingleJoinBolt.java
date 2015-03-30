package baidu.cae.storm.SingleJoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.RotatingMap;

public class SingleJoinBolt extends BaseRichBolt {

	int _numSources;
	Fields _idFields;
	Fields _outFields;
	
	OutputCollector _collector;
	
	Map<String, GlobalStreamId> _fieldLocation;
	RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>> _pending;
	
	
	public SingleJoinBolt(Fields fields) {
		_outFields = fields;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		Set<String> idFields = null;
		
		_fieldLocation = new HashMap<String, GlobalStreamId>();
		_numSources = context.getThisSources().size();
		_collector = collector;
		
		int timeout = ((Number)stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
		_pending = new RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>>(timeout, new ExpireCallback());
		
		for (GlobalStreamId source : context.getThisSources().keySet()) {
			
			Fields fields = context.getComponentOutputFields(source.get_componentId(), source.get_streamId());
			Set<String> setFields = new HashSet<String>(fields.toList());

			if (idFields == null)
			{
				idFields = setFields;
			}
			else 
			{
				idFields.retainAll(setFields);
			}
			
			for (String outField : _outFields)
			{
				for (String sourceField : fields) 
				{
					if (outField.equals(sourceField))
					{
						_fieldLocation.put(outField, source);
					}
				}
			}
			
		}
		
		_idFields = new Fields(new ArrayList<String>(idFields));
		
		if (_fieldLocation.size() != _outFields.size()) 
		{
			throw new RuntimeException("Can not find all outfields among sources");
		}

	}

	public void execute(Tuple input) {
		List<Object> id = input.select(_idFields);
		GlobalStreamId streamId = new GlobalStreamId(input.getSourceComponent(), input.getSourceStreamId());
		
		if (!_pending.containsKey(id))
		{
			_pending.put(id,  new HashMap<GlobalStreamId, Tuple>());
		}
		
		Map<GlobalStreamId, Tuple> parts = _pending.get(id);
		if (parts.containsKey(streamId)) 
		{
			throw new RuntimeException("Receive same side of single join twice");
		}
		
		parts.put(streamId, input);
		
		if (parts.size() == _numSources)
		{
			_pending.remove(id);
			List<Object> joinResult = new ArrayList<Object>();
			for (String outField : _outFields)
			{
				GlobalStreamId loc = _fieldLocation.get(outField);
				joinResult.add(parts.get(loc).getValueByField(outField));
			}
			_collector.emit(parts.values(), joinResult);
			System.out.println(joinResult.toString());
			
			for (Tuple part : parts.values())
			{
				_collector.ack(part);
			}
		}

		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(_outFields);
	}

	private class ExpireCallback implements RotatingMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> 
	{
		public void expire(List<Object> id, Map<GlobalStreamId, Tuple> tuples) 
		{	
			for (Tuple tuple : tuples.values())
			{
				_collector.fail(tuple);
			}
		}
	}
}
