package news;

import connector.Connector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

 //过滤tag，至少两个用户，五部电影
public class TagProcess {
	private int splitSymbol=28555;
	
	public void insertTable_tags() throws ClassNotFoundException, SQLException, IOException
	{
		Connector connect=new Connector();
		connect.conn.setAutoCommit(false);
		ArrayList<String> tags=new ArrayList<String>();
		PreparedStatement pstmt=connect.conn.prepareStatement("insert into tags(userID,movieID,tag,tagID,rating,avg_rating) values (?,?,?,?,?,?)");
		PreparedStatement pstmt2=connect.conn.prepareStatement("select rating from ratingslens where userID=? and movieID=?");
		PreparedStatement pstmt3=connect.conn.prepareStatement("select avg(rating) from ratingslens where userID=?");
		BufferedReader input=new BufferedReader(new FileReader("F:\\研讨班\\数据集\\movielens\\tags.dat"));
		String str=input.readLine();
		int count=0;
		while(str!=null)
		{
			String[] ff=str.split("::");
			int userID=0;
			int movieID=0;
			try{
				userID=Integer.parseInt(ff[0]);
				movieID=Integer.parseInt(ff[1]);
			}
			catch(java.lang.ArrayIndexOutOfBoundsException e){
				System.out.println(count);
			}
			
			String tag=ff[2].toLowerCase().trim();
			if(tag.length()<30)
			{
				if(!tags.contains(tag))
				{
					tags.add(tag);
				}
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setString(3,tag);
				pstmt.setInt(4, tags.indexOf(tag));
				pstmt2.setInt(1, userID);
				pstmt2.setInt(2, movieID);
				pstmt3.setInt(1, userID);
				ResultSet result=pstmt2.executeQuery();
				ResultSet result2=pstmt3.executeQuery();
				if(result.next())
				{
					pstmt.setDouble(5, result.getDouble(1));
				}
				else
				{
					pstmt.setDouble(5, 0);
				}
				result.close();
				if(result2.next())
				{
					pstmt.setDouble(6,result2.getDouble(1));
				}
				else
				{
					pstmt.setDouble(6, 0);
				}
				result2.close();
				pstmt.executeUpdate();
				count++;
			}
			if(count%10000==9999)
			{
				connect.conn.commit();
			}
			str=input.readLine();
		}
		pstmt.close();
		pstmt2.close();
		pstmt3.close();
		connect.conn.commit();
		connect.conn.close();
	}
	
	public void newTable_tags() throws ClassNotFoundException, SQLException
	{
		Connector connect=new Connector();
		PreparedStatement pstmt=connect.conn.prepareStatement("drop table if exists tags");
		pstmt.execute();
		pstmt.close();
		pstmt=connect.conn.prepareStatement("create table tags(id int not null primary key auto_increment, userID int not null, movieID int not null , tag varchar(30) not null, tagID int not null , rating double not null, avg_rating double not null)");
		pstmt.execute();
		pstmt.close();
		connect.conn.close();
	}
	
	public void newTable_temptags1() throws ClassNotFoundException, SQLException
	{
		Connector connect=new Connector();
		PreparedStatement pstmt=connect.conn.prepareStatement("drop table if exists temptags1");
		pstmt.execute();
		pstmt.close();
		pstmt=connect.conn.prepareStatement("create table temptags1(id int not null primary key auto_increment, userID int not null, movieID int not null , tag varchar(30) not null, tagID int not null)");
		pstmt.execute();
		pstmt.close();
		connect.conn.close();
	}
	
	public void insertTable_temptags1() throws ClassNotFoundException, SQLException
	{
		Connection conn=new Connector().conn;
		PreparedStatement pstmt=conn.prepareStatement("select distinct tagID from tags where rating!=0 and rating>avg_rating");
		ResultSet result=pstmt.executeQuery();
		ArrayList<Integer> tagIDs1=new ArrayList<Integer>();
		while(result.next())
		{
			tagIDs1.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		pstmt=conn.prepareStatement("select distinct tagID from tags where rating!=0 and rating<avg_rating");
		result=pstmt.executeQuery();
		ArrayList<Integer> tagIDs2=new ArrayList<Integer>();
		while(result.next())
		{
			tagIDs2.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		pstmt=conn.prepareStatement("select userID, movieID, tag, tagID from tags");
		result=pstmt.executeQuery();
		conn.setAutoCommit(false);
		pstmt=conn.prepareStatement("insert into temptags1(userID,movieID,tag,tagID) values (?,?,?,?)");
		int count=0;
		ArrayList<String> tags=new ArrayList<String>();
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			String tag=result.getString(3);
			int tagID=result.getInt(4);
			if(tagIDs1.contains(tagID)&&tagIDs2.contains(tagID))
			{
				if(!tags.contains(tag))
				{
					tags.add(tag);
				}
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setString(3,tag);
				pstmt.setInt(4, tags.indexOf(tag));
				pstmt.executeUpdate();
				count++;
			}
			if(count%1000==999)
			{
				conn.commit();
			}
		}
		conn.commit();
		conn.close();
	}
	
	public void newTable_temptags2() throws ClassNotFoundException, SQLException
	{
		Connector connect=new Connector();
		PreparedStatement pstmt=connect.conn.prepareStatement("drop table if exists temptags2");
		pstmt.execute();
		pstmt.close();
		pstmt=connect.conn.prepareStatement("create table temptags2(id int not null primary key auto_increment, userID int not null, movieID int not null , tag varchar(30) not null, tagID int not null)");
		pstmt.execute();
		pstmt.close();
		connect.conn.close();
	}
	
	public void insertTable_temptags2() throws ClassNotFoundException, SQLException
	{
		Connection conn=new Connector().conn;
		ArrayList<Integer> tags=new ArrayList<Integer>();
		PreparedStatement pstmt=conn.prepareStatement("select distinct tagID from temptags1 group by tagID having count(distinct userID)>2");
		ResultSet result=pstmt.executeQuery();
		while(result.next())
		{
			tags.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		pstmt=conn.prepareStatement("select userID, movieID, tagID, tag from temptags1");
		result=pstmt.executeQuery();
		conn.setAutoCommit(false);
		pstmt=conn.prepareStatement("insert into temptags2(userID,movieID,tag,tagID) values (?,?,?,?)");
		int count=0;
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			int tagID=result.getInt(3);
			String tag=result.getNString(4);
			if(tags.contains(tagID))
			{
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setString(3,tag);
				pstmt.setInt(4, tags.indexOf(tagID));
				pstmt.executeUpdate();
				count++;
			}
			if(count%1000==999)
			{
				conn.commit();
			}
		}
		conn.commit();
		conn.close();
	}
	
	public void newTable_temptags3() throws ClassNotFoundException, SQLException
	{
		Connector connect=new Connector();
		PreparedStatement pstmt=connect.conn.prepareStatement("drop table if exists temptags3");
		pstmt.execute();
		pstmt.close();
		pstmt=connect.conn.prepareStatement("create table temptags3(id int not null primary key auto_increment, userID int not null, movieID int not null , tag varchar(30) not null, tagID int not null)");
		pstmt.execute();
		pstmt.close();
		pstmt=connect.conn.prepareStatement("drop table if exists temptags3target");
		pstmt.execute();
		pstmt.close();
		pstmt=connect.conn.prepareStatement("create table temptags3target(id int not null primary key auto_increment, userID int not null, movieID int not null , tag varchar(30) not null, tagID int not null)");
		pstmt.execute();
		pstmt.close();
		connect.conn.close();
	}
	
	public void insertTable_temptags3() throws ClassNotFoundException, SQLException
	{
		Connection conn=new Connector().conn;
		PreparedStatement pstmt=conn.prepareStatement("select distinct tagID from temptags2 where userID<"+splitSymbol);
		ResultSet result=pstmt.executeQuery();
		ArrayList<Integer> tags=new ArrayList<Integer>();
		while(result.next())
		{
			tags.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		pstmt=conn.prepareStatement("select userID, movieID, tagID, tag from temptags2 where userID<"+splitSymbol);
		result=pstmt.executeQuery();
		conn.setAutoCommit(false);
		PreparedStatement pstmt1=conn.prepareStatement("insert into temptags3(userID,movieID,tag,tagID) values (?,?,?,?)");
		int count1=0;
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			int tagID=result.getInt(3);
			String tag=result.getNString(4);			
				pstmt1.setInt(1, userID);
				pstmt1.setInt(2, movieID);
				pstmt1.setString(3,tag);
				pstmt1.setInt(4,tags.indexOf(tagID));
				pstmt1.executeUpdate();
				count1++;
			if(count1%1000==999)
			{
				conn.commit();
			}
		}
		conn.commit();
		conn.close();
	}
	
	public void insertTable_temptags3_target() throws ClassNotFoundException, SQLException
	{
		Connection conn=new Connector().conn;
		PreparedStatement pstmt=conn.prepareStatement("select distinct tagID from temptags2 where userID>="+splitSymbol);
		ResultSet result=pstmt.executeQuery();
		ArrayList<Integer> tags=new ArrayList<Integer>();
		while(result.next())
		{
			tags.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		pstmt=conn.prepareStatement("select userID, movieID, tagID, tag from temptags2 where userID>="+splitSymbol);
		result=pstmt.executeQuery();
		conn.setAutoCommit(false);
		PreparedStatement pstmt1=conn.prepareStatement("insert into temptags3target(userID,movieID,tag,tagID) values (?,?,?,?)");
		int count1=0;
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			int tagID=result.getInt(3);
			String tag=result.getNString(4);
			if(userID>=splitSymbol)
			{
				pstmt1.setInt(1, userID);
				pstmt1.setInt(2, movieID);
				pstmt1.setString(3,tag);
				pstmt1.setInt(4,tags.indexOf(tagID));
				pstmt1.executeUpdate();
				count1++;
			}
			if(count1%1000==999)
			{
				conn.commit();
			}
		}
		conn.commit();
		conn.close();
	}
	
	public void newTable_tagnames() throws ClassNotFoundException, SQLException
	{
		Connector connect=new Connector();
		PreparedStatement pstmt=connect.conn.prepareStatement("drop table if exists tagnames");
		pstmt.execute();
		pstmt.close();
		pstmt=connect.conn.prepareStatement("create table tagnames(id int not null primary key auto_increment, userID int not null, movieID int not null , tag varchar(30) not null, tagID int not null)");
		pstmt.execute();
		pstmt.close();
		connect.conn.close();
	}
	
	public void insertTable_tagnames() throws ClassNotFoundException, SQLException
	{
		Connection conn=new Connector().conn;
		ArrayList<Integer> movieIDs=new ArrayList<Integer>();
		PreparedStatement pstmt=conn.prepareStatement("select distinct movieID from temptags3 group by movieID having count(distinct tag)> 15");
		ResultSet result=pstmt.executeQuery();
		while(result.next())
		{
			movieIDs.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		pstmt=conn.prepareStatement("select userID, movieID, tag from temptags3");
		result=pstmt.executeQuery();
		conn.setAutoCommit(false);
		pstmt=conn.prepareStatement("insert into tagnames(userID,movieID,tag,tagID) values (?,?,?,?)");
		int count=0;
		ArrayList<String> tags=new ArrayList<String>();
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			String tag=result.getString(3);
			if(movieIDs.contains(movieID))
			{
				if(!tags.contains(tag))
				{
					tags.add(tag);
				}
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setString(3,tag);
				pstmt.setInt(4, tags.indexOf(tag));
				pstmt.executeUpdate();
				count++;
			}
			if(count%1000==999)
			{
				conn.commit();
			}
		}
		conn.commit();
		conn.close();
	}
	
	public void newTable_tagnames_target() throws ClassNotFoundException, SQLException
	{
		Connector connect=new Connector();
		PreparedStatement pstmt=connect.conn.prepareStatement("drop table if exists tagnamestarget");
		pstmt.execute();
		pstmt.close();
		pstmt=connect.conn.prepareStatement("create table tagnamestarget(id int not null primary key auto_increment, userID int not null, movieID int not null , tag varchar(30) not null, tagID int not null)");
		pstmt.execute();
		pstmt.close();
		connect.conn.close();
	}
	
	public void insertTable_tagnames_target() throws ClassNotFoundException, SQLException
	{
		Connection conn=new Connector().conn;
		ArrayList<Integer> movieIDs=new ArrayList<Integer>();
		PreparedStatement pstmt=conn.prepareStatement("select distinct movieID from temptags3target group by movieID having count(distinct tag)< 10 and count(distinct tag)>=5");
		ResultSet result=pstmt.executeQuery();
		while(result.next())
		{
			movieIDs.add(result.getInt(1));
		}
		result.close();
		pstmt.close();
		pstmt=conn.prepareStatement("select userID, movieID, tag from temptags3target");
		result=pstmt.executeQuery();
		conn.setAutoCommit(false);
		pstmt=conn.prepareStatement("insert into tagnamestarget(userID,movieID,tag,tagID) values (?,?,?,?)");
		int count=0;
		ArrayList<String> tags=new ArrayList<String>();
		while(result.next())
		{
			int userID=result.getInt(1);
			int movieID=result.getInt(2);
			String tag=result.getString(3);
			if(movieIDs.contains(movieID))
			{
				if(!tags.contains(tag))
				{
					tags.add(tag);
				}
				pstmt.setInt(1, userID);
				pstmt.setInt(2, movieID);
				pstmt.setString(3,tag);
				pstmt.setInt(4, tags.indexOf(tag));
				pstmt.executeUpdate();
				count++;
			}
			if(count%1000==999)
			{
				conn.commit();
			}
		}
		conn.commit();
		conn.close();
	}
}
