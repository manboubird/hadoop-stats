package cascading.pipe.assembly;

import cascading.operation.aggregator.AnalyzeSession;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Sessionize input stream and out the number of elements each session and session time.
 */
public class SessionAnalysisPipe extends SubAssembly {
	
	private static final long serialVersionUID = 4888561522015116475L;
	
	public static final InputArgs DEFAULT_INPUT_ARGS = new InputArgs("sid", "time");
	public static final OutputArgs DEFAULT_OUTPUT_ARGS = new OutputArgs(AnalyzeSession.FN_QUERY_COUNT, AnalyzeSession.FN_SESSION_TIME);

	public SessionAnalysisPipe(Pipe pipe) {
		this(pipe, DEFAULT_INPUT_ARGS, DEFAULT_OUTPUT_ARGS);
	}
	
	public SessionAnalysisPipe(Pipe pipe, String sidFieldName, String timestampFieldName) {
		this(pipe, new InputArgs(sidFieldName, timestampFieldName), DEFAULT_OUTPUT_ARGS);
	}
	
	public SessionAnalysisPipe(Pipe pipe, InputArgs inputArgs, OutputArgs outputArgs) {
		Pipe countPipe = new Pipe("session analysis pipe", pipe);
		Fields groupFileds = new Fields(inputArgs.sidFieldName);
		Fields sortFileds = new Fields(inputArgs.timestampFieldName);
		countPipe = new GroupBy(countPipe, groupFileds, sortFileds);
		countPipe = new Every(countPipe, 
							new Fields(inputArgs.timestampFieldName), 
							new AnalyzeSession(new Fields(outputArgs.queryCountFieldName, outputArgs.sessionTimeFieldName)), 
							new Fields(outputArgs.queryCountFieldName, outputArgs.sessionTimeFieldName));
	    setTails(countPipe);
	}
	
	public static class InputArgs {
		public InputArgs(String sidFieldName, String timestampFieldName) {
			this.sidFieldName = sidFieldName;
			this.timestampFieldName = timestampFieldName;
		}
		public String sidFieldName;
		public String timestampFieldName;
	}
	
	public static class OutputArgs {
		public OutputArgs(String queryCountFieldName, String sessionTimeFieldName) {
			this.queryCountFieldName = queryCountFieldName;
			this.sessionTimeFieldName = sessionTimeFieldName;
		}
		public String queryCountFieldName;
		public String sessionTimeFieldName; 
	}
}
