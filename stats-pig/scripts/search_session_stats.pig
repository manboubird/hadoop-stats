register 'stats-pig-0.0.1-SNAPSHOT.jar';
define analyzeSession pig.AnalyzeSession();
define calcStats pig.CalcStats();

%default grouping_time_granularity 10

-- load log file
records = load 'excite-small.log' as (sid:chararray, time:long, word:chararray);

-- Group by sid and analyze session.
grouped = group records by sid;
analyzed = foreach grouped {
	sorted = order records by time;
	generate flatten(analyzeSession(sorted.time));
};

-- Insert session_time field which is grouping time granularity. defualt is 10 sec.
rcds = foreach analyzed generate record_count, (session_time - (session_time % ($grouping_time_granularity * 1000))) as grouped_time;

-- calculate count, average, min and max.
grouped = group rcds by grouped_time;
ret1 = foreach grouped {
	sorted = order rcds by grouped_time;
	generate group as grouped_time:long, COUNT(rcds.record_count) as count, AVG(rcds.record_count) as average, MIN(rcds.record_count) as min, MAX(rcds.record_count) as max;
};

-- calculate variance with ret1.
joined = cogroup ret1 by grouped_time, rcds by grouped_time;
ret2 = foreach joined generate group, flatten(calcStats(ret1, rcds));
store ret2 into 'output/search_session_stats';
