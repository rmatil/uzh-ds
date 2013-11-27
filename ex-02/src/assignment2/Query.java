package assignment2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Query {
	private Connection connection = null;
	private Statement statement = null;
	private List<String> inputWords = new ArrayList<String>();

	private void connectToDatabase(String pPath) {
		// load the sqlite JDBC driver using the current class loader
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			System.err.println("JDBC class loading failed.");
			e.printStackTrace();
		}

		// create a database connection
		// database will be created if it doesn't exist...
		try {
			connection = DriverManager.getConnection("jdbc:sqlite:" + pPath);
			statement = connection.createStatement();
			statement.setQueryTimeout(30);
		} catch (SQLException e) {
			System.err.println("Connection to database failed...");
			e.printStackTrace();
		}
	}

	private void readFile(String pPath) {
		try {
			Configuration config = new Configuration();
			FileSystem fs = FileSystem.getLocal(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("file:///" + pPath))));

			String line = "";
			while ((line = br.readLine()) != null) {
				inputWords.add(line); // on each line only one word
			}
		} catch (Exception e) {
			System.err.println("Reading from file '" + pPath + "' failed.");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void runQueries() {
		for (String entity : inputWords) {
			executeQuery(entity);
		}
	}
	
	private void executeQuery(String pWord) {
		String query = "SELECT " +
					   "task3.word, task3.occurences, task4.movieName, task4.movieActors " + 
					   "FROM task3 " + 
					   "INNER JOIN task4 " +
					   "ON task3.movieId=task4.movieId " + 
					   "WHERE task3.word='" + pWord + "' " + 
					   "ORDER BY occurences DESC";
					   					   					   
		try {
			ResultSet rs = statement.executeQuery(query);
			int ctr = 0;
			while (rs.next() && ctr < 10) {
				// get for the 10 highest results the moviename and the actors from this movie
				String movieName = rs.getString("movieName");
				String movieActors = rs.getString("movieActors");
				System.out.println(movieName + "\t" + movieActors);
				++ctr;
			}
			
		} catch (SQLException e) {
			System.err.println("Query failed: " + query);
			e.printStackTrace();
		}
		
	}

	public static void main(String[] pArgs) {
		if (pArgs.length < 2) {
			System.err.println("---------------------------------------------------");
			System.err.println("Usage: locationOfDb queryTextFile");
			System.err.println("Please try again...");
			System.err.println("---------------------------------------------------");
			System.exit(1);		
		}

		Query query = new Query();
		query.connectToDatabase(pArgs[0]);
		query.readFile(pArgs[1]);
		query.runQueries();
		
	}

}
