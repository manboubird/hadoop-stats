package cascading.operation.aggregator;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Sessionize input stream and output tuple consisting of:
 * <ul>
 *   <li>count - The number of elements within single session.</li>
 *   <li>session time - The difference between first element time and last element time. <br>
 *   Note that, in single session, time differences between successive elements are within 60 seconds.
 *   </li>
 * </ul>
 * First element of the input tuple is assumed UNIX timestamp.
 * The input tuple must be sorted by this timestamp.
 */
public class AnalyzeSession extends BaseOperation implements Buffer {

	public static final String FN_QUERY_COUNT	= "count";
	public static final String FN_SESSION_TIME	= "sessionTime";
	
	public static final int INPUT_ARGS_NUM = 1;
	public static final Fields OUTPUT_FIELDS = new Fields(FN_QUERY_COUNT, FN_SESSION_TIME);
	
	public static final long MAX_QUERY_INTERVAL = 60 * 1000;
	
	public AnalyzeSession(Fields outputFields) {
		super(INPUT_ARGS_NUM, outputFields);
	}
	
	public AnalyzeSession() {
		super(INPUT_ARGS_NUM, OUTPUT_FIELDS);
	}
	
	public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
		
		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
		TupleEntry tupleEntry = arguments.next();
		
		long firstQueryTime = tupleEntry.getLong(0);
		long lastQueryTime = firstQueryTime;
		long count = 1;
		long currentQueryTime;
		while(arguments.hasNext()) {
			tupleEntry = arguments.next();
			currentQueryTime = tupleEntry.getLong(0);
			if((currentQueryTime - lastQueryTime) > MAX_QUERY_INTERVAL) {
				long sessionTime = lastQueryTime - firstQueryTime;
				addOutputTuple(count, sessionTime, bufferCall);
				count = 0;
				firstQueryTime = currentQueryTime;
			}
			count++;
			lastQueryTime = currentQueryTime;
		}
		long sessionTime = lastQueryTime - firstQueryTime;
		addOutputTuple(count, sessionTime, bufferCall);
	}

	private void addOutputTuple(long count, long sessionTime, BufferCall bufferCall) {
		Tuple result = new Tuple(count, sessionTime);
		bufferCall.getOutputCollector().add(result);
	}
}
