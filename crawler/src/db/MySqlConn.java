package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;


public class MySqlConn {

	Connection conn;
	Statement stmt;
	ResultSet rs;

	/**
	 * Costruttore.
	 * Imposta il parametro Conn e stabilisce la connessione con il DB.
	 * @param db
	 * @param user
	 * @param psw
	 */
	
	
	// jdbc:mysql://mydbinstance.abcdefghijkl.us-east-1.rds.amazonaws.com:3306/employees?user=sa&password=mypassword
	public MySqlConn(String db, String user, String psw, String hostname, String port)
	{
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
		}
		catch (Exception e)
		{
			System.out.println("Error - Driver jdbc non presente: " + e.getMessage());
		}

		try 
		{
			System.out.println("Connecting to the DB...");
	     // String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
			String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port + "/" + db + "?user=" + user + "&password=" + psw;
			conn = DriverManager.getConnection(jdbcUrl);	
			//conn = DriverManager.getConnection("jdbc:mysql://128.16.2.148/" + db + "?" + "user=" + user + "&password=" + psw);	
			System.out.println("Connected");
		}
		catch (SQLException ex) 
		{
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
	}

	/**
	 * Assegna i valori a ResultSet e Statement.
	 * Riceve in input la query e restituisce in output il ResultSet.
	 * @param query
	 * @return
	 */
	public ResultSet queryDB (String query)
	{
		try 
		{
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			return rs;
		}
		catch (SQLException ex)
		{
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
		return null;
	}

	/**
	 * Libera le risorse ResultSet e Statement.
	 * Ricordarsi di effettuare tale operazione ogni qualvolta non devono piu' essere fatte le query!!!
	 */
	public void closeConn ()
	{
		if (rs != null) 
		{
			try 
			{
				rs.close();
			} 
			catch (SQLException sqlEx) 
			{ }
			rs = null;
		}

		if (stmt != null) 
		{
			try 
			{
				stmt.close();
			}
			catch (SQLException sqlEx) 
			{ } 
			stmt = null;
		}	
	}


	public void executeQuery (String query)
	{
		try 
		{
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
		} 
		catch(SQLException ex) { System.err.println("SQLException: " + ex.getMessage()); }
	}

	public int executePreparedQuery (String query,String tweet)
	{
		int count = 0;
		try 
		{
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setString(1, tweet);
			ResultSet resultSet = ps.executeQuery();
			if(resultSet.next()) {
			    count = resultSet.getInt(1);
			    return count;
			}
			
		} 
		catch(SQLException ex) { System.err.println("SQLException: " + ex.getMessage()); }
		return count;
	}

	/*	public static ResultSet queryDBStatic (String query)
	{

		Connection conn = null;
		try 
		{
			conn = DriverManager.getConnection("jdbc:mysql://localhost/bibsonomy?" +
	                                      "user=folk&password=folk");
		} 
		catch (SQLException ex) 
		{
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}

		//querying the DB
		Statement stmt = null;
		ResultSet rs = null;
		try 
		{
			stmt = conn.createStatement();
		    rs = stmt.executeQuery(query);
	    	while (rs.next()) 
	    	{
	    		int user = 
	    		// Get the data from the row using the column index
	    		String s = rs.getString(1);
	    		System.out.println("IdUser: " + s);

	    		// Get the data from the row using the column name
	    		s = rs.getString("Tag");
	    		System.out.println("Tag: " + s);    		    	
	    		System.out.println("");    		    	
	    	}     	   
    	}

		catch (SQLException ex)
		{
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
		finally 
		{
			// it is a good idea to release
		    // resources in a finally{} block
		    // in reverse-order of their creation
		    // if they are no-longer needed

		    if (rs != null) 
		    {
		    	try 
		        {
		    		rs.close();
		        } 
		        catch (SQLException sqlEx) 
		        { } // ignore

		        rs = null;
		    }

		    if (stmt != null) 
		    {
		    	try 
		        {
		    		stmt.close();
		        }
		        catch (SQLException sqlEx) 
		        { } // ignore

		        stmt = null;
		    }	
		}
		System.out.println("Fine funzione");
		return rs;
	}*/


}


