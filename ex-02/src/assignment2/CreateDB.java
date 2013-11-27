package assignment2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateDB {

	// contains elements of task4
	private Map<String, Map<String, String>> task4 = new HashMap<String, Map<String, String>>(); 
	private Map<String, Map<String, String>> task3 = new HashMap<String, Map<String, String>>();
	private Connection connection = null;
	private Statement statement = null;

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
			// create table for task4-results
			statement.executeUpdate("drop table if exists task4");
			statement.executeUpdate("create table task4 (movieId integer, movieName string, movieActors string)");
			// create table for task3-results
			statement.executeUpdate("drop table if exists task3");
			statement.executeUpdate("create table task3 (id string, word string, movieId string, occurences integer)");

		} catch (SQLException e) {
			System.err.println("Connection to database failed...");
			e.printStackTrace();
		}
	}

	private void insertIntoDatabase() {
		// insert task3 results
		String id = "";
		String word = "";
		String movieId = "";
		String occurences = "";

		// add all values to the database
		for (Map.Entry<String, Map<String, String>> entity : task3.entrySet()) {
			word = entity.getKey();

			// iterate over inner Map
			for (Map.Entry<String, String> innerEntity : entity.getValue().entrySet()) {
				movieId = innerEntity.getKey();
				occurences = innerEntity.getValue();
			}
			// primary key: word + movieId
			id =  word + movieId;

			// insert elements
			String query = "INSERT INTO task3 " 
					+ "(id, word, movieId, occurences) "
					+ "VALUES "
					+ "(\"" + id + "\", \"" + word + "\",\"" + Integer.parseInt(movieId) + "\",\"" + Integer.parseInt(occurences) + "\")";
			
			try {
				statement.executeUpdate(query);
			} catch (SQLException e) {
				System.err.println("Insert statement failed. Query: " + query);
				e.printStackTrace();
				System.exit(1);
			}

		}

		// insert task4 results
		movieId = "";
		String movieName = "";
		String movieActors = "";

		// add all values to the database
		for (Map.Entry<String, Map<String, String>> entity : task4.entrySet()) {
			movieId = entity.getKey();

			// iterate over the inner map
			for (Map.Entry<String, String> innerEntity : entity.getValue().entrySet()) {
				movieName = innerEntity.getKey();
				movieActors = innerEntity.getValue();
			}
			movieName = movieName.replace("\"", "'");
			movieActors = movieActors.replace("\"", "'");
			// syntax: statement.executeUpdate("insert into person values(1, 'leo')");
			String query = "INSERT INTO task4 " 
					+ "(movieId, movieName, movieActors) "
					+ "VALUES "
					+ "("  + Integer.parseInt(movieId) + ",\"" + movieName + "\",\"" + movieActors + "\")";

			try {
				statement.executeUpdate(query);
			} catch (SQLException e) {
				System.err.println("Insert statement failed. Query: " + query);
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	private void readTask3File(String pPath) {
		// File has the format: movieId \t movieName \t actor1 \t actor2 \n
		try{
			List<String> fileList = new ArrayList<String>(); // contains all files which have part-xxxxx in their name

			Configuration config = new Configuration();
			//config.addResource(new Path("/home/hadoop/hadoop-1.2.1/conf/core-site.xml"));
			FileSystem fs = FileSystem.get(config);
			Path pt = new Path("hdfs://" + pPath);


			// read number of files which contain part-xxxxx
			FileStatus[] fileStatus = fs.listStatus(pt);
			if(fileStatus != null) {
				for (FileStatus filestatus : fileStatus) {
					String name = filestatus.getPath().getName();
					if(!filestatus.isDir()) {
						// new file found
						// check file whether it contains "part-xxxxx" or not
						if (name.contains("part-")) {
							fileList.add(name);
						}
					} 
				}
			}

			// open all found files which contain part-xxxx in their names
			for (int i=0; i<fileList.size(); i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs://" + pPath + "/" + fileList.get(i)))));

				String line = "";
				while ((line = br.readLine()) != null) {

					String word = "";
					String movieId = "";
					String numberOfOccurences = "";

					Map<String, String> tmp = new HashMap<String, String>(); // contains movieName, movieActors
					String[] tokens = line.split(" ");
					word = tokens[0];
					movieId = tokens[1];
					numberOfOccurences = tokens[2];

					tmp.put(movieId, numberOfOccurences);					
					task3.put(word, tmp);
				}
			}


		}catch(Exception e){
			e.printStackTrace();
		}
	}

	private void readTask4File(String pPath) {
		// File has the format: movieId \t movieName \t actor1 \t actor2 \n
		try{
			List<String> fileList = new ArrayList<String>(); // contains all files which have part-xxxxx in their name

			Configuration config = new Configuration();
			//config.addResource(new Path("/home/hadoop/hadoop-1.2.1/conf/core-site.xml"));
			FileSystem fs = FileSystem.get(config);
			Path pt = new Path("hdfs://" + pPath);


			// read number of files which contain part-xxxxx
			FileStatus[] fileStatus = fs.listStatus(pt);
			if(fileStatus != null) {
				for (FileStatus filestatus : fileStatus) {
					String name = filestatus.getPath().getName();
					if(!filestatus.isDir()) {
						// new file found
						// check file whether it contains "part-xxxxx" or not
						if (name.contains("part-")) {
							fileList.add(name);
						}
					} 
				}
			}

			// open all found files which contain part-xxxx in their names
			for (int i=0; i<fileList.size(); i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs://" + pPath + "/" + fileList.get(i)))));

				String line = "";
				while ((line = br.readLine()) != null) {

					String movieId = "";
					String movieName = "";
					String movieActors = "";

					Map<String, String> tmp = new HashMap<String, String>(); // contains movieName, movieActors
					String[] tokens = line.split("\t");
					movieId = tokens[0];
					movieName = tokens[1];
					// add movieActors
					for (int j=2; j<tokens.length; j++) {
						// don't add space at last position
						if (j == tokens.length-2) {
							movieActors += tokens[j];
						} else {
							movieActors += tokens[j] + " ";
						}
					}
					tmp.put(movieName, movieActors);					
					task4.put(movieId, tmp);
				}
			}


		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void main(String[] pArgs) {

		if (pArgs.length < 3) {
			System.err.println("---------------------------------------------------");
			System.err.println("Usage: outputPathOfEx3 outputPathOfEx4 locationOfDb");
			System.err.println("Please try again...");
			System.err.println("---------------------------------------------------");
			System.exit(1);
		}

		CreateDB db = new CreateDB();
		db.readTask3File(pArgs[0]);
		db.readTask4File(pArgs[1]);
		db.connectToDatabase(pArgs[2]);
		db.insertIntoDatabase();

	}



}
