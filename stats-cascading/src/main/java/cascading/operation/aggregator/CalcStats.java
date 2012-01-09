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
 * Calculate variance and median based on precomputed average.
 */
public class CalcStats extends BaseOperation implements Buffer {

	// Output fields definition
	public static final String OUTPUT_FN_COUNT		= "count";
	public static final String OUTPUT_FN_AVERAGE	= "average";
	public static final String OUTPUT_FN_VARIANCE	= "variance";
	public static final String OUTPUT_FN_MEDIAN		= "median";
	public static final String OUTPUT_FN_MIN		= "min";
	public static final String OUTPUT_FN_MAX		= "max";

	public static final int INPUT_ARGS_NUM = 6;
	public static final int INPUT_ARGS_FN_RECORD_TYPE	= 0;
	public static final int INPUT_ARGS_FN_QUERY_COUNT	= 1;
	public static final int INPUT_ARGS_FN_COUNT			= 2;
	public static final int INPUT_ARGS_FN_AVERAGE		= 3;
	public static final int INPUT_ARGS_FN_MIN			= 4;
	public static final int INPUT_ARGS_FN_MAX			= 5;
	
	public static final Fields OUTPUT_FIELDS = new Fields(OUTPUT_FN_COUNT, OUTPUT_FN_AVERAGE, OUTPUT_FN_VARIANCE, OUTPUT_FN_MEDIAN, OUTPUT_FN_MIN, OUTPUT_FN_MAX);

	public CalcStats() {
		super(INPUT_ARGS_NUM, OUTPUT_FIELDS);
	}

	public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

		TupleEntry tupleEntry = arguments.next();
		if(tupleEntry.getInteger(INPUT_ARGS_FN_RECORD_TYPE) != 0) {
			throw new IllegalStateException("First field's type must be 0, got: " + tupleEntry.toString() );
	    }
		double average = tupleEntry.getDouble(INPUT_ARGS_FN_AVERAGE);
		long count = tupleEntry.getLong(INPUT_ARGS_FN_COUNT);
		long min = tupleEntry.getLong(INPUT_ARGS_FN_MIN);
		long max = tupleEntry.getLong(INPUT_ARGS_FN_MAX);

		boolean isCountOdd = count % 2 == 1;
		long meanTargetCounter = isCountOdd 
									? (count + 1) / 2 
									: count / 2;
		
		double varianceAmount = 0.0D;
		double median = Double.MIN_VALUE;
		long actualCount = 0;
		while(arguments.hasNext()) {
			actualCount++;
			tupleEntry = arguments.next();
			long queryCount = tupleEntry.getLong(INPUT_ARGS_FN_QUERY_COUNT);
			varianceAmount += Math.pow((queryCount - average), 2);
			
			if(actualCount == meanTargetCounter) {
				// get Xth value as median.
				median = queryCount;
			}else if(isCountOdd == false && actualCount == meanTargetCounter + 1) {
				// when n is odd, add (X+1)th value to X th and divide by 2.
				median = (median + queryCount) / 2;
			}
		}
		if(count != actualCount) {
			throw new IllegalStateException("The number of elements is not equal: count=" + count + ", actualCount=" + actualCount);
		}
		double variance = varianceAmount / count;
		Tuple result = new Tuple(count, average, variance, median, min, max);
		bufferCall.getOutputCollector().add(result);
	}
}