package news;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import transfer.*;
import sourcecluster.*;


public class Main {
	public static int topicCount=110;
	public static double max_dis=0.3;//表示最大得距离阈值
	public static double xishudu=0.1;
	private double cfmae=0;
	private double cfrsme=0;
	private double zjmae=0;
	private double zjrsme=0;
	private double mae=0;
	private double rsme=0;
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException{
		Main m=new Main();
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/movies", "root", "123456");
		PreparedStatement pstmt=conn.prepareStatement("insert into results(counttopic,maxdistance,ceshijimovie,xishudu,cfmae,cfrsme,zjmae,zjrsme,mae,rsme) values(?,?,?,?,?,?,?,?,?,?)");
		conn.setAutoCommit(false);
		for(int i=0;i<5;i++){
			m.change_xishudu();
			pstmt.setInt(1, topicCount);
			pstmt.setDouble(2, max_dis);
			//pstmt.setInt(3, ceshiji_movie);
			pstmt.setDouble(4, xishudu);
			pstmt.setDouble(5, m.cfmae);
			pstmt.setDouble(6, m.cfrsme);
			pstmt.setDouble(7, m.zjmae);
			pstmt.setDouble(8, m.zjrsme);
			pstmt.setDouble(9, m.mae);
			pstmt.setDouble(10, m.rsme);
			pstmt.executeUpdate();
			conn.commit();
		}
		for(int i=0;i<5;i++){
			topicCount=35;
			m.change_topicCount();
			pstmt.setInt(1, topicCount);
			pstmt.setDouble(2, max_dis);
		//	pstmt.setInt(3, ceshiji_movie);
			pstmt.setDouble(4, xishudu);
			pstmt.setDouble(5, m.cfmae);
			pstmt.setDouble(6, m.cfrsme);
			pstmt.setDouble(7, m.zjmae);
			pstmt.setDouble(8, m.zjrsme);
			pstmt.setDouble(9, m.mae);
			pstmt.setDouble(10, m.rsme);
			pstmt.executeUpdate();
			conn.commit();
		}
//		pstmt.setInt(1, topicCount);
//		pstmt.setDouble(2, max_dis);
//		pstmt.setInt(3, ceshiji_movie);
//		pstmt.setDouble(4, m.xishudu);
//		pstmt.setDouble(5, m.cfmae);
//		pstmt.setDouble(6, m.cfrsme);
//		pstmt.setDouble(7, m.zjmae);
//		pstmt.setDouble(8, m.zjrsme);
//		pstmt.setDouble(9, m.mae);
//		pstmt.setDouble(10, m.rsme);
//		pstmt.executeUpdate();
//		conn.commit();
		pstmt.close();
		conn.close();
	}
	
	public void whole_process()  throws ClassNotFoundException, SQLException, IOException{
		TagProcess tp=new TagProcess();
		//temptags1用来过滤掉那些代表用户偏好的tag，如很好，非常差等。
		tp.newTable_temptags1();
		tp.insertTable_temptags1();
		//temptags2用来保证每一个tag都至少被两个用户使用，而且被至少用于5个电影。
		tp.newTable_temptags2();
		tp.insertTable_temptags2();
		//划分源数据集和目标数据集，源集放在temptags3，目标集放在temptags3_target中。
		tp.newTable_temptags3();
		tp.insertTable_temptags3();
		tp.insertTable_temptags3_target();
		//训练集中tagnames的表中的每一个movie都包含至少15个tag
		tp.newTable_tagnames();
		tp.insertTable_tagnames();
		//tagnames_target的表中每一个movie都包含不多于10个tag
		tp.newTable_tagnames_target();
		tp.insertTable_tagnames_target();
		//下面是在源和目标数据集中进行
		tagprocess6.process();//计算q
		tagprocess7.process();//计算Q
		tagprocess4.process();//计算共现分布,存放在distribution中		
		MovieCluster1_new.process();//计算标签之间的距离，存放在distance中
		tagprocess6target.process();//计算q
		tagprocess7target.process();//计算Q
		tagprocess4target.process();//计算共现分布,存放在distribution中	
		BookCluster1.process();//计算target数据集上的JSD距离
		
		//在目标数据集上进行
		ratingProcess_new.process();//过滤评分，使得评分中的电影ID和用户ID都在tagnamestarget中有出现，结果存储在ratingstemp中		
		//下面同于change_topicCount()
		this.change_topicCount();
	}
	public void change_topicCount() throws ClassNotFoundException, SQLException, IOException{
		cluster.process();
		MovieCluster2_new.process();
		this.change_maxdis();
	}
	
	public void change_maxdis() throws ClassNotFoundException, SQLException, IOException{
		BookCluster2_new.process();
		BookCluster3_new.process();
		//下面对目标数据集进行聚类
		Clusterzj.process();
		MovieCluster2_newzj.process();
		this.change_xishudu();
	}
	
	
	public void change_xishudu() throws ClassNotFoundException, SQLException, IOException{
		RatingProcess2_new.process();//从ratingstemp中按照定义好的稀疏度来稀疏数据，存储在finalratings和predictratings中
		tagprocess5_new.process();//计算一个tag在一部电影中的权重
		TagProcess6_new.process();
		tagprocess7_new.process();//计算一个topic在一部电影中的权重
		tagprocess8_new.process();//计算topic评分
		TagProcess6_newzj.process();
		tagprocess7_newzj.process();//计算一个topic在一部电影中的权重
		tagprocess8_newzj.process();//计算topic评分
		double[] result=user_based_new.process();
		this.cfmae=result[0];	
		this.cfrsme=result[1];
		result=recommendation_new.process();
		this.mae=result[0];
		this.rsme=result[1];
		result=recommendation_newzj.process();
		this.zjmae=result[0];
		this.zjrsme=result[1];
		System.out.println(this.cfmae);
		System.out.println(this.cfrsme);
		System.out.println(this.mae);
		System.out.println(this.rsme);
		System.out.println(this.zjmae);
		System.out.println(this.zjrsme);
	}
}
