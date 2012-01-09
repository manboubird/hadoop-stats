package cascading.stats;


import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.SessionAnalysisPipe;
import cascading.pipe.assembly.StatsCalculationPipe;
import cascading.pipe.assembly.StatsCalculationPipe.InputArgs;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Execution via appassembler plugin<br />
 * <br />
 * [1] local mode
 * <pre>
 * mvn -Dmaven.test.skip=true clean package 
 * sh ./target/appassembler/bin/stats.sh ./src/test/resources/data/excite-small.log ./target/output
 * ... log ... 
 * cat ./target/output/part-00000
 * </pre>
 * [2] pseudo distributed mode<br />
 *  hadoop configs under Src/profiles/pseudo/config directory.
 * 
 * <pre>
 * mvn -Dmaven.test.skip=true -P pseudo clean package 
 * sh ./target/appassembler/bin/stats.sh ./src/test/resources/data/excite-small.log ./target/output
 * ... log ... 
 * hadoop fs -cat ./target/output/part-00000
 * </pre>
 *
 * <p>[ Option ]</p>
 * Convert flow graph dot file to jpg. 
 * <pre>
 * dot -Tjpg ./target/stats.dot -o ./target/stats.jpg
 * open ./target/stats.jpg
 * </pre>
 * Requirement: Gaphviz installation
 */
public class SearchSessionStatsMain {
	
	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];
		Cascade cascade = createCompletableCascade(inputPath, outputPath);
		cascade.complete();
	}

	public static Cascade createCompletableCascade(String inputPath, String outputDirPath) {

		Properties properties = new Properties();
		// set job jar.
		FlowConnector.setApplicationJarClass(properties, SearchSessionStatsMain.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		CascadeConnector cascadeConnector = new CascadeConnector();
				
		// specify local filesystem or HDFS as source.
		// Tab separated field definition:
		//		"session ID", "UNIX timestamp" "search query"
		String sidFieldName			= "sid";
		String timestampFieldName	= "time";
		String wordsFieldName		= "words";
		Scheme scheme = new TextDelimited(new Fields(sidFieldName, timestampFieldName, wordsFieldName), "\t");
		Tap logTap = inputPath.matches("^[^:]+://.*") 
				? new Hfs(scheme, inputPath) 
				: new Lfs(scheme, inputPath);

		Pipe importPipe = new Pipe("import pipe");
	
		String queryCountFieldName = "queryCount";
		String sessionTimeFieldName = "sessionTime";

		// Sessinaize search log.
		SessionAnalysisPipe.InputArgs saInputArgs = new SessionAnalysisPipe.InputArgs(sidFieldName, timestampFieldName);
		SessionAnalysisPipe.OutputArgs saOutputArgs = new SessionAnalysisPipe.OutputArgs(queryCountFieldName, sessionTimeFieldName);
		Pipe sessionAnalysisPipe = new SessionAnalysisPipe(importPipe, saInputArgs, saOutputArgs);
		
		// Each session time guranularity (10s),
		// calculate the number of log records and the number of queries stats(average, variance, median, min, max)
		int groupinGranularityInSec = 10;
		StatsCalculationPipe.InputArgs scInputArgs = new StatsCalculationPipe.InputArgs(sessionTimeFieldName, queryCountFieldName, groupinGranularityInSec); 
		sessionAnalysisPipe = new StatsCalculationPipe(sessionAnalysisPipe, scInputArgs);

		Tap sinkTap = new Hfs(new TextLine(), outputDirPath);
		Flow flow = flowConnector.connect(logTap, sinkTap, sessionAnalysisPipe);

		// Output flow graph (optional)
		flow.writeDOT("target/stats.dot");
		
		return cascadeConnector.connect(flow);
	}
}
