package cascading.pipe.assembly;

import cascading.operation.Function;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Insert specified time granularity field into tuple.
 */
public class GroupedTimeInsertionPipe  extends SubAssembly {

	private static final long serialVersionUID = 2559738009565066232L;

	public GroupedTimeInsertionPipe(Pipe pipe) {
		this(pipe, "sessionTime", "groupedTime", 10);
	}
	
	public GroupedTimeInsertionPipe(Pipe pipe, String inputTimeFieldName, String ouptputGroupedTimeFieldName, int groupinGranularityInSec) {
		Pipe groupedTimeInsertionPipe = new Pipe("grouped time insertion pipe: inputTimeFieldName = " + inputTimeFieldName + ", ouptputGroupedTimeFieldName = " + ouptputGroupedTimeFieldName + ", groupingGranularityInSec = " + groupinGranularityInSec, pipe);
		String exp = String.format("%s - (%s  %% (%d * 1000))", 
									inputTimeFieldName, inputTimeFieldName, groupinGranularityInSec);
		Function expFunc = new ExpressionFunction(
									new Fields(ouptputGroupedTimeFieldName), exp, 
									new String[] { inputTimeFieldName }, new Class[]{ long.class });
		groupedTimeInsertionPipe = new Each(pipe, Fields.ALL, expFunc, Fields.ALL);
		setTails(groupedTimeInsertionPipe);
	}
}
