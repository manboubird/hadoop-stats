package cascading.stats;


import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.StatsPipe;
import cascading.pipe.assembly.StatsQueryPerSessionPipe;
import cascading.scheme.TextDelimited;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * appassemblerプラグインで生成したスクリプトで実行方法<br />
 * <br />
 * [1] ローカルモードでの実行する場合。
 * <pre>
 * mvn -Dmaven.test.skip=true clean package 
 * sh ./target/appassembler/bin/stats.sh ./src/test/resources/data/excite-small.log ./target/output
 * </pre>
 * [2] localhostのhadoopクラスターにジョブをサブミットする場合。<br />
 * src/profiles/pseudo/config 配下のhadoopの設定ファイルを適用します。
 * 
 * <pre>
 * mvn -Dmaven.test.skip=true -P pseudo clean package 
 * sh ./target/appassembler/bin/stats.sh ./src/test/resources/data/excite-small.log ./target/output
 * </pre>
 * @author toshiaki_toyama
 *
 */
public class StatsMain {
	
	public static void main(String[] args) {

		String inputPath = args[0];
		String logsPath = args[1] + "/logs/";
		String outputPath = args[1];

		Cascade cascade = createCompletableCascade(inputPath, logsPath, outputPath);
		cascade.complete();
	}
	
	public static Cascade createCompletableCascade(String inputPath, String logsPath, String outputDirPath) {
		
		Properties properties = new Properties();
		// job のjarの設定。
		FlowConnector.setApplicationJarClass(properties, StatsMain.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		CascadeConnector cascadeConnector = new CascadeConnector();

		String outputPath = outputDirPath + "/stats/";
		String minPath = outputPath + "min";
				
		// ローカル、または、HDFSからをソースに指定
		// タブ区切りのフィールド定義:
		//		"セッションID", "UNIXタイムスタンプ" "空白区切りの検索ワード"
		Tap logTap = inputPath.matches("^[^:]+://.*") 
				? new Hfs(new TextDelimited(new Fields("sid", "time", "words"), "\t"), inputPath) 
				: new Lfs(new TextDelimited(new Fields("sid", "time", "words"), "\t"), inputPath);

		Pipe importPipe = new Pipe("import");
		// 1セッション当たりの検索回数とセッションの時間を集計
		Pipe tmPipe = new StatsQueryPerSessionPipe(importPipe);
		// セッション時間の１０秒刻みごとに、1セッション当たりの検索回数の統計情報（セッション数、検索回数の平均、分散、最大値、最小値、中央値）を計算
		tmPipe = new StatsPipe(tmPipe);
		Tap tmSinkTap = new Hfs(new TextLine(), minPath);

		Flow flow = flowConnector.connect(logTap, tmSinkTap,tmPipe);

		// Flowのグラフを出力（オプション）
		flow.writeDOT("target/stats.dot");

		// connect the flows by their dependencies, order is not significant
		return cascadeConnector.connect(flow);
	}
}
