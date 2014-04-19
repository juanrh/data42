package storm.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Build with:
 * 
 * $ mvn clean package  
 * Launch with:
 * 	- local mode:
 * $ storm jar target/storm-py-twitter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.twitter.TweetsTopology
 *  *  - cluster mode:
 * $ storm jar target/storm-py-twitter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.twitter.TweetsTopology topologyName
 * 
 * WARNING: when a topology running in local mode is interrupted, those shell process (e.g. a python process) started by the 
 * topology might NOT be stopped, and so be killed by hand (e.g. with killall)  
 * */
public class TweetsTopology {
	/**
	 * TODO: revisar hints de paralelismo 
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		// This spout has no parallelism 
		builder.setSpout("PlacesSpout", new PlacesSpout(), 1);
			// fieldsGrouping is not really needed here, there is no sub state that needs to be processed together
		// builder.setBolt("TrendsBolt", new TrendsBolt(), 4).fieldsGrouping("PlacesSpout", new Fields(TopologyFields.PLACE));
		builder.setBolt("TrendsBolt", new TrendsBolt(), 4).shuffleGrouping("PlacesSpout");
		builder.setBolt("GetTweetsBolt", new GetTweetsBolt(), 4*2).shuffleGrouping("TrendsBolt"); 
		builder.setBolt("DBStoreBolt", new DBStoreBolt(), 4*2).shuffleGrouping("GetTweetsBolt");
		
		Config conf = new Config();
		conf.setDebug(true);
		if(args != null && args.length > 0) {
			conf.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
		}

	}

}
