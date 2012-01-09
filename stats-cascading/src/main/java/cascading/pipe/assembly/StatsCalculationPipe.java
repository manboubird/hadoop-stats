package cascading.pipe.assembly;

import org.apache.log4j.Logger;

import cascading.operation.Insert;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.CalcStats;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Max;
import cascading.operation.aggregator.Min;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * calculate count, average, variance, median, min, max each time guranularity.
 */
public class StatsCalculationPipe extends SubAssembly {

	private static final long serialVersionUID = -7546107897198035679L;

	private static final Logger LOG = Logger.getLogger(StatsCalculationPipe.class);

	// temporary field names
	private static final String MID_FN_COUNT	= StatsCalculationPipe.class.getSimpleName() + "Count";
	private static final String MID_FN_AVERAGE	= StatsCalculationPipe.class.getSimpleName() + "Average";
	private static final String MID_FN_MIN		= StatsCalculationPipe.class.getSimpleName() + "Min";
	private static final String MID_FN_MAX		= StatsCalculationPipe.class.getSimpleName() + "Max";
	private static final String MID_FN_TYPE		= StatsCalculationPipe.class.getSimpleName() + "Type";
	
	public StatsCalculationPipe(Pipe pipe, InputArgs inputArgs) {
		this(pipe, inputArgs, new OutputArgs(CalcStats.OUTPUT_FN_COUNT, 
											 CalcStats.OUTPUT_FN_AVERAGE, 
											 CalcStats.OUTPUT_FN_VARIANCE, 	
											 CalcStats.OUTPUT_FN_MEDIAN, 
											 CalcStats.OUTPUT_FN_MIN, 
											 CalcStats.OUTPUT_FN_MAX));
	}
	
	/**
	 * Calculate the number of records and stats (average, variance, median, min, max) each time granularity.
	 * @param pipe
	 * @param inputArgs
	 * @param outputArgs
	 */
	public StatsCalculationPipe(Pipe pipe, InputArgs inputArgs, OutputArgs outputArgs) {
		LOG.info("StatsCalculationPipe args: " + inputArgs + ", " + outputArgs);
		String timeFieldName 		= inputArgs.timeFieldName;
		String statsTargetFieldName = inputArgs.statsTagetFieldName;
		int groupinGranularityInSec = inputArgs.groupingGranularityInSec;
				
		Pipe statsCalculationPipe = new Pipe("stats calculation pipe" , pipe);
		
		String groupedTimeFieldName ="groupedTime";
		statsCalculationPipe = new GroupedTimeInsertionPipe(statsCalculationPipe, timeFieldName, groupedTimeFieldName, groupinGranularityInSec);
		
		Pipe pipe1 = new Pipe("first stats calculation pipe", statsCalculationPipe);
		// group by groupedTimeFieldName and get Count, Average, Min and Max.
		pipe1 = new GroupBy(pipe1, new Fields(groupedTimeFieldName));
		Fields statsTargetFields = new Fields(statsTargetFieldName);
		pipe1 = new Every(pipe1, statsTargetFields, new Count(new Fields(MID_FN_COUNT)));
		pipe1 = new Every(pipe1, statsTargetFields, new Average(new Fields(MID_FN_AVERAGE)));
		pipe1 = new Every(pipe1, statsTargetFields, new Min(new Fields(MID_FN_MIN)));
		pipe1 = new Every(pipe1, statsTargetFields, new Max(new Fields(MID_FN_MAX)));

		// Insert record type and statsTargetFieldName fields
		// To indicate stats record, record type value is set 0.
		pipe1 = new Each(pipe1, new Insert(new Fields(MID_FN_TYPE, statsTargetFieldName), 0, null), 
								new Fields(MID_FN_TYPE, groupedTimeFieldName, statsTargetFieldName, MID_FN_COUNT, MID_FN_AVERAGE, MID_FN_MIN, MID_FN_MAX));

		// Branch the statsCalculationPipe
		Pipe pipe2 = new Pipe("second stats calculation pipe", statsCalculationPipe);
		// Insert record type, MID_FN_TYPE, MID_FN_COUNT, MID_FN_AVERAGE, MID_FN_MIN and MID_FN_MAX.
		// To indicate this branched record, record type value is set 1.
		pipe2 = new Each(pipe2, new Insert(new Fields(MID_FN_TYPE, MID_FN_COUNT, MID_FN_AVERAGE, MID_FN_MIN, MID_FN_MAX), 1, null, null, null, null), 
								new Fields(MID_FN_TYPE, groupedTimeFieldName, statsTargetFieldName, MID_FN_COUNT, MID_FN_AVERAGE, MID_FN_MIN, MID_FN_MAX));

		// Group pipe1 and pipe2 by groupedTimeFieldName and sort them by MID_FN_TYPE.
		Fields groupFields = new Fields(groupedTimeFieldName);
		Fields sortFields = new Fields(MID_FN_TYPE);
		Pipe mergedPipe = new GroupBy(Pipe.pipes(pipe1, pipe2), groupFields, sortFields);
		// Calculate variance and median.
		mergedPipe = new Every(mergedPipe, 
				new Fields(MID_FN_TYPE, statsTargetFieldName, MID_FN_COUNT, MID_FN_AVERAGE, MID_FN_MIN, MID_FN_MAX), 
				new CalcStats(), 
				new Fields(groupedTimeFieldName, outputArgs.countFieldName, outputArgs.averageFieldName, outputArgs.varianceFieldName, outputArgs.medianFieldName, outputArgs.minFieldName, outputArgs.maxFieldName));

		setTails(mergedPipe);
	}
	
	public static class InputArgs {
		public InputArgs(String timeFieldName, String statsTagetFieldName, int groupingGranularityInSec) {
			this.timeFieldName = timeFieldName;
			this.statsTagetFieldName = statsTagetFieldName;
			this.groupingGranularityInSec = groupingGranularityInSec;
		}
		public String timeFieldName;
		public String statsTagetFieldName;
		public int groupingGranularityInSec;
		@Override
		public String toString() {
			return "InputArgs [timeFieldName=" + timeFieldName
					+ ", statsTagetFieldName=" + statsTagetFieldName
					+ ", groupingGranularityInSec=" + groupingGranularityInSec
					+ "]";
		}
	}
	
	public static class OutputArgs {
		public OutputArgs(String countFieldName, String averageFieldName,
				String varianceFieldName, String medianFieldName,
				String minFieldName, String maxFieldName) {
			this.countFieldName = countFieldName;
			this.averageFieldName = averageFieldName;
			this.varianceFieldName = varianceFieldName;
			this.medianFieldName = medianFieldName;
			this.minFieldName = minFieldName;
			this.maxFieldName = maxFieldName;
		}
		public String countFieldName;
		public String averageFieldName;
		public String varianceFieldName;
		public String medianFieldName;
		public String minFieldName;
		public String maxFieldName;
		
		@Override
		public String toString() {
			return "OutputArgs [countFieldName=" + countFieldName
					+ ", averageFieldName=" + averageFieldName
					+ ", varianceFieldName=" + varianceFieldName
					+ ", medianFieldName=" + medianFieldName
					+ ", minFieldName=" + minFieldName + ", maxFieldName="
					+ maxFieldName + "]";
		}
	}
}
