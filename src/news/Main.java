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
	public static double max_dis=0.3;//��ʾ���þ�����ֵ
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
		//temptags1�������˵���Щ�����û�ƫ�õ�tag����ܺã��ǳ���ȡ�
		tp.newTable_temptags1();
		tp.insertTable_temptags1();
		//temptags2������֤ÿһ��tag�����ٱ������û�ʹ�ã����ұ���������5����Ӱ��
		tp.newTable_temptags2();
		tp.insertTable_temptags2();
		//����Դ���ݼ���Ŀ�����ݼ���Դ������temptags3��Ŀ�꼯����temptags3_target�С�
		tp.newTable_temptags3();
		tp.insertTable_temptags3();
		tp.insertTable_temptags3_target();
		//ѵ������tagnames�ı��е�ÿһ��movie����������15��tag
		tp.newTable_tagnames();
		tp.insertTable_tagnames();
		//tagnames_target�ı���ÿһ��movie������������10��tag
		tp.newTable_tagnames_target();
		tp.insertTable_tagnames_target();
		//��������Դ��Ŀ�����ݼ��н���
		tagprocess6.process();//����q
		tagprocess7.process();//����Q
		tagprocess4.process();//���㹲�ֲַ�,�����distribution��		
		MovieCluster1_new.process();//�����ǩ֮��ľ��룬�����distance��
		tagprocess6target.process();//����q
		tagprocess7target.process();//����Q
		tagprocess4target.process();//���㹲�ֲַ�,�����distribution��	
		BookCluster1.process();//����target���ݼ��ϵ�JSD����
		
		//��Ŀ�����ݼ��Ͻ���
		ratingProcess_new.process();//�������֣�ʹ�������еĵ�ӰID���û�ID����tagnamestarget���г��֣�����洢��ratingstemp��		
		//����ͬ��change_topicCount()
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
		//�����Ŀ�����ݼ����о���
		Clusterzj.process();
		MovieCluster2_newzj.process();
		this.change_xishudu();
	}
	
	
	public void change_xishudu() throws ClassNotFoundException, SQLException, IOException{
		RatingProcess2_new.process();//��ratingstemp�а��ն���õ�ϡ�����ϡ�����ݣ��洢��finalratings��predictratings��
		tagprocess5_new.process();//����һ��tag��һ����Ӱ�е�Ȩ��
		TagProcess6_new.process();
		tagprocess7_new.process();//����һ��topic��һ����Ӱ�е�Ȩ��
		tagprocess8_new.process();//����topic����
		TagProcess6_newzj.process();
		tagprocess7_newzj.process();//����һ��topic��һ����Ӱ�е�Ȩ��
		tagprocess8_newzj.process();//����topic����
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
