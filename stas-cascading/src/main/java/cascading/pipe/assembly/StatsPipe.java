package cascading.pipe.assembly;

import cascading.operation.Insert;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.CalcVarianceAndMedian;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.CountQueryPerSession;
import cascading.operation.aggregator.Max;
import cascading.operation.aggregator.Min;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * @author toshiaki_toyama
 *
 */
public class StatsPipe extends SubAssembly {

	private static final long serialVersionUID = -7546107897198035679L;

	/**
	 * valueFiled で指定したフィールドのセッション時間が10秒ごとの統計情報（要素数、平均、分散、最小値、最大値、中央値）を計算します。
	 * @param pipe
	 * @param valueFiled
	 */
	public StatsPipe(Pipe pipe) {
		Pipe timeGroupingPipe = new Pipe("time grouping pipe", pipe);
		
		timeGroupingPipe = new Each(pipe, new Fields("queryCount", "sessionTime"), 
				new ExpressionFunction(
						// sessionTime を10秒刻みでグルーピング
						new Fields("groupingTime"), "sessionTime - (sessionTime % (10 * 1000))"
						, new String[] {"queryCount", "sessionTime"}, new Class[]{long.class, long.class}
						)
				, new Fields("groupingTime", "queryCount", "sessionTime"));

		Pipe firstStatsPipe = new Pipe("first stats pipe", timeGroupingPipe);
		// グループごとにCount, Average, Min, Max を計算します。
		firstStatsPipe = new GroupBy(firstStatsPipe, new Fields("groupingTime"));
		Fields targetFieldName = new Fields(CountQueryPerSession.FN_QUERY_COUNT);
		firstStatsPipe = new Every(firstStatsPipe, targetFieldName, new Count());
		firstStatsPipe = new Every(firstStatsPipe, targetFieldName, new Average());
		firstStatsPipe = new Every(firstStatsPipe, targetFieldName, new Min());
		firstStatsPipe = new Every(firstStatsPipe, targetFieldName, new Max());
		
		// 分散を計算するパイプにフィールドに合わせて、
		// "recordType", "queryCount", "sessionTime" フィールドをInsertします。
		// 1次集計結果を示すフラグとして recordType を 0 に設定します。
		firstStatsPipe = new Each(firstStatsPipe, 
							new Insert(new Fields("recordType", "queryCount", "sessionTime"), 0, null, null), 
							new Fields("recordType", "groupingTime", "queryCount", "sessionTime", "count", "average", "min", "max"));

		Pipe groupPipeWithStatsFields = new Pipe("varinace pipe", timeGroupingPipe);
		// 時間でグルーピングしたログに統計結果のフィールドを追加します。
		// 分散を計算するパイプのフィールドに合わせて、
		// "recordType", "count", "average", "min", "max" フィールドをInsertします。
		// 時間でグルーピングしたレコードを示すフラグとして recordType を 1 に設定します。
		groupPipeWithStatsFields = new Each(groupPipeWithStatsFields, 
								new Insert(new Fields("recordType", "count", "average", "min", "max"), 1, null, null, null, null), 
								new Fields("recordType", "groupingTime", "queryCount", "sessionTime", "count", "average", "min", "max"));

		Fields groupFields = new Fields("groupingTime");
		Fields sortFields = new Fields("recordType");
		// 1次集計結果（firstStatsPipe）と時間でグルーピングしたログ（groupPipeWithStatsFields）
		// をマージし、groupingTimeでグルーピングし、recordTypeでソートします。
		Pipe variancePipe = new GroupBy(Pipe.pipes(firstStatsPipe, groupPipeWithStatsFields), groupFields, sortFields);
		// 分散値と中央値を計算します。
		variancePipe = new Every(variancePipe, 
				new Fields("recordType", "queryCount", "count", "average", "min", "max"), 
				new CalcVarianceAndMedian(), 
				new Fields("groupingTime", "countInput", "averageInput", "variance", "median", "minInput", "maxInput"));

		setTails(variancePipe);
	}
}
