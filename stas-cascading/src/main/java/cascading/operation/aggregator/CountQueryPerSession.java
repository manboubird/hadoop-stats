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
 * 検索の１セッション当たりの検索回数とセッションの時間（はじめのクエリーの問い合わせから最後のクエリーの時間の差）
 * を集計します。
 * 同一セッション内では連続するクエリーの問い合わせ間隔は60秒以内とします。
 * @author toshiaki_toyama
 */
public class CountQueryPerSession extends BaseOperation implements Buffer {

	public static final String FN_QUERY_COUNT 		= "queryCount";
	public static final String FN_FIRST_QUERY_TIME	= "firstQueryTime";
	public static final String FN_LAST_QUERY_TIME	= "lastQueryTime";
	public static final String FN_SESSION_TIME		= "sessionTime";
	
	public static final int INPUT_ARGS_NUM = 1;
	public static final Fields OUTPUT_FIELDS = new Fields(FN_QUERY_COUNT, FN_FIRST_QUERY_TIME, 
					 									  FN_LAST_QUERY_TIME, FN_SESSION_TIME);
	
	public static final long MAX_QUERY_INTERVAL = 60 * 1000;
	
	public CountQueryPerSession() {
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
				Tuple result = new Tuple(count, firstQueryTime, lastQueryTime, sessionTime);
				bufferCall.getOutputCollector().add(result);
				count = 0;
				firstQueryTime = currentQueryTime;
			}
			count++;
			lastQueryTime = currentQueryTime;
		}
		long sessionTime = lastQueryTime - firstQueryTime;
		Tuple result = new Tuple(count, firstQueryTime, lastQueryTime, sessionTime);
		bufferCall.getOutputCollector().add(result);
	}
}
