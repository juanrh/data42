package storm.twitter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * When this bolt extends BaseRichBolt it fails after the first execution. The difference is that BaseBasicBolt
 * acks the tuples automatically, so probably it has to do with the fact that this bolt emits no tuple
 * */
public class DBStoreBolt extends BaseBasicBolt {

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost/blog";

	   //  Database credentials
	static final String DEFAULT_DB_USER = "blogger";
    static final String DEFAULT_DB_PASS = "spot";
    
    private String dbUser;
    private String dbPass;
    private Connection conn = null;
    private PreparedStatement stmt = null;
	
    public DBStoreBolt(){
    	this(DEFAULT_DB_USER, DEFAULT_DB_PASS);
    }
    
    public DBStoreBolt(String dbUser, String dbPass){
    	this.dbUser = dbUser;
    	this.dbPass = dbPass;
    }
    
	/**
	 * Generated by Eclipse
	 */
	private static final long serialVersionUID = 9204080939471606559L;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			// Register JDBC driver
		    Class.forName(JDBC_DRIVER);
		    // Open connection to database
		    System.out.println("Connecting to a MySQL database...");
		    conn = DriverManager.getConnection(DB_URL, dbUser, dbPass);
		    System.out.println("Connected to database successfully");
		    String insertString = "INSERT INTO storm_tweets" +   		
		    		" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)" +
		    		" ON DUPLICATE KEY UPDATE hits = hits + 1";
			stmt = conn.prepareStatement(insertString);
		} 
		catch (SQLException se) {
			System.err.println("Error accessing database");
			throw new RuntimeException(se);
		} 
		catch (ClassNotFoundException cnfe) {
			System.err.println("Cannot find driver class " + JDBC_DRIVER);
			throw new RuntimeException(cnfe);
		}
	}

	@Override
	public void cleanup() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		super.cleanup();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			/*
			 * From GetTweetsBolt:
			 * 	declarer.declare(new Fields(TopologyFields.AUTHOR_SCREEN_NAME, TopologyFields.CREATED_AT,
				TopologyFields.FAV_COUNT, TopologyFields.HASHTAGS_TEXTS, TopologyFields.IN_REPLY_TO_SCREEN_NAME, 
				TopologyFields.LANG, TopologyFields.RETWEET_COUNT, TopologyFields.RETWEETED, 
				TopologyFields.SOURCE, TopologyFields.PLACE, TopologyFields.POSSIBLY_SENSITIVE,
				TopologyFields.TEXT, TopologyFields.TOPIC_NAME));
			 * */
			stmt.setString(10, input.getStringByField(TopologyFields.AUTHOR_SCREEN_NAME));
			stmt.setTimestamp(8, Timestamp.valueOf(input.getStringByField(TopologyFields.CREATED_AT)));
			Long favCount = input.getLongByField(TopologyFields.FAV_COUNT);
			// note precision conversion
			stmt.setInt(2, favCount == null ? null : favCount.intValue());
			stmt.setString(11, input.getStringByField(TopologyFields.HASHTAGS_TEXTS));
			stmt.setString(4, input.getStringByField(TopologyFields.IN_REPLY_TO_SCREEN_NAME));
			stmt.setString(7, input.getStringByField(TopologyFields.LANG));
			// note precision conversion
			Long retweetCount = input.getLongByField(TopologyFields.RETWEET_COUNT);
			stmt.setInt(5, retweetCount == null ? null : retweetCount.intValue());
			Boolean retweeted = input.getBooleanByField(TopologyFields.RETWEETED);
			if (retweeted == null) {
				// otherwise the conversion from a null Boolean to boolean raises a null pointer exception
				stmt.setNull(3, java.sql.Types.BOOLEAN);
			}
			else { 
				stmt.setBoolean(3, retweeted);
			}
			stmt.setString(9, input.getStringByField(TopologyFields.SOURCE));
			stmt.setString(12, input.getStringByField(TopologyFields.PLACE));	
			Boolean possiblySensitive = input.getBooleanByField(TopologyFields.POSSIBLY_SENSITIVE);
			if (possiblySensitive == null) {
				// otherwise the conversion from a null Boolean to boolean raises a null pointer exception
				stmt.setNull(6, java.sql.Types.BOOLEAN);
			}
			else {
				stmt.setBoolean(6, possiblySensitive);
			}
			stmt.setString(1, input.getStringByField(TopologyFields.TEXT));
			stmt.setString(13, input.getStringByField(TopologyFields.TOPIC_NAME));
			
			stmt.executeUpdate();
		}
		catch (SQLException se) {
			// log error and try with the next tuple
			System.err.println("Error updating database");
			se.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No output field, will just write to db
	}

}
