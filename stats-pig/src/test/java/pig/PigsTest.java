package pig;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Pig script test.
 * 
 * Jvm args may be required for standalone mode: 
 * <pre>
 * -Xmx512m -XX:PermSize=128m -XX:MaxPermSize=512m
 * </pre>
 */
public class PigsTest {
	
	private static final boolean useCluster = true;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		if(useCluster) {
			System.getProperties().setProperty("pigunit.exectype.cluster", "true");
		}
	}

	@Test
	public void test_search_session_stats() throws IOException, ParseException, URISyntaxException {
		String[] args = {};

		File file = new File("scripts/search_session_stats.pig");
		PigTest test = new PigTest(file.getAbsolutePath(), args);

		String[] input = { 
				"A	10	word1 word2\n" + 
				"B	10	word1\n" + 
				"B	20	word2\n" +
				"C	30	word3\n" +
				"C	20030	word4"};

		String[] output = {
			"(0,2,1.5,1,0.25,1.0,2.0)", 
			"(20000,1,2.0,2,0.0,2.0,2.0)"};

		test.assertOutput("records", input, "ret2", output);
	}
}
