package cascading.operation.aggregator;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CalcVarianceAndMedian  extends BaseOperation implements Buffer {

	// 出力するフィールド名の定義
	// *Input のサッフィクスがフィールド名は入力フィールド名とかぶらないようにするためです。
	public static final String FN_COUNT		= "countInput";
	public static final String FN_AVERAGE	= "averageInput";
	public static final String FN_VARIANCE	= "variance";
	public static final String FN_MEDIAN	= "median";
	public static final String FN_MIN		= "minInput";
	public static final String FN_MAX		= "maxInput";

	public static final int INPUT_ARGS_NUM = 6;
	public static final int INPUT_ARGS_FN_RECORD_TYPE	= 0;
	public static final int INPUT_ARGS_FN_QUERY_COUNT	= 1;
	public static final int INPUT_ARGS_FN_COUNT			= 2;
	public static final int INPUT_ARGS_FN_AVERAGE		= 3;
	public static final int INPUT_ARGS_FN_MIN			= 4;
	public static final int INPUT_ARGS_FN_MAX			= 5;
	
	public static final Fields OUTPUT_FIELDS = new Fields(FN_COUNT, FN_AVERAGE, FN_VARIANCE, FN_MEDIAN, FN_MIN, FN_MAX);

	public CalcVarianceAndMedian() {
		super(INPUT_ARGS_NUM, OUTPUT_FIELDS);
	}

	public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

		TupleEntry tupleEntry = arguments.next();
		if(tupleEntry.getInteger(INPUT_ARGS_FN_RECORD_TYPE) != 0) {
			throw new IllegalStateException("First field's recordType must be 0, got: " + tupleEntry.toString() );
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
				// x 番目の中央値の取得（n が偶数、奇数の時）
				median = queryCount;
			}else if(isCountOdd == false && actualCount == meanTargetCounter + 1) {
				// n が奇数の場合は x+1 番目の値を x 番目の値と足して、２で割る。
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