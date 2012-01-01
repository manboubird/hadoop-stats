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
 * sessionId を含む tuple ストリームにたいし、要素数の総数とユニーク数をカウントします。
 * @author toshiaki_toyama
 *
 */
public class CountTotalAndUnique extends BaseOperation implements Buffer {

	public static final String FN_TOTAL = "total";
	public static final String FN_UNIQUE_COUNT = "uniqueCount";

	public static final int INPUT_ARGS_NUM = 1;
	public static final Fields OUTPUT_FIELDS = new Fields(FN_TOTAL, FN_UNIQUE_COUNT);

	public CountTotalAndUnique() {
		super(INPUT_ARGS_NUM, OUTPUT_FIELDS);	
	}

	/* (non-Javadoc)
	 * @see cascading.operation.Buffer#operate(cascading.flow.FlowProcess, cascading.operation.BufferCall)
	 */
	public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

		long count = 0;
		long uniqueCount = 0;
		String lastSid = "";
		
		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

		while(arguments.hasNext()) {
			count++;
			String sid = arguments.next().getString(0);
			if(lastSid.equals(sid) == false) {
				uniqueCount++;
				lastSid = sid;
			}
		}
		Tuple result = new Tuple(count, uniqueCount);
		bufferCall.getOutputCollector().add(result);
	}
}