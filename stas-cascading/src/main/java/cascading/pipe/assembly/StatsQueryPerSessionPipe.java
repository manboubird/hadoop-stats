package cascading.pipe.assembly;

import cascading.operation.aggregator.CountQueryPerSession;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class StatsQueryPerSessionPipe extends SubAssembly {
	
	private static final long serialVersionUID = 4888561522015116475L;

	public StatsQueryPerSessionPipe(Pipe pipe) {
		
		Pipe countPipe = new Pipe("stats query per session pipe", pipe);
		
		Fields groupFileds = new Fields("sid");
		Fields sortFileds = new Fields("time");
		countPipe = new GroupBy(countPipe, groupFileds, sortFileds);
		countPipe = new Every(countPipe, 
						new Fields("time"), 
						new CountQueryPerSession(), 
						new Fields("sid", 
									CountQueryPerSession.FN_FIRST_QUERY_TIME, 
									CountQueryPerSession.FN_LAST_QUERY_TIME, 
									CountQueryPerSession.FN_QUERY_COUNT,
									CountQueryPerSession.FN_SESSION_TIME));
	    setTails(countPipe);
	}
}
