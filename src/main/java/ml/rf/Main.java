/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ml.rf;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import ml.secrf.TreeCore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author herley
 */
public class Main {

    public static void main(String[] args) {
        try {

            Class.forName("org.apache.derby.jdbc.ClientDriver");
            Connection connection = DriverManager.getConnection("jdbc:derby://localhost:1527/pm", args[0], args[1]);

            //<editor-fold defaultstate="collapsed" desc="String Manipulation Trial">
//            String a = "select count ( y ) as hitung from X where ( HOUSING = 'no' ) AND ( PREVIOUS BETWEEN 0 AND 0) AND ( CAMPAIGN BETWEEN 1 AND 5) AND ( AGE BETWEEN 22 AND 61) AND ( JOB = 'services' ) AND ( JOB = 'technician') AND ( BALANCE BETWEEN 1205 AND 45248) AND ( LOAN = 'no') AND ( HOUSING = 'yes') AND ( DURATION BETWEEN 742 AND 2033) ";
//
//            Pattern p = Pattern.compile("\\((.*?)\\)");
//            Matcher m = p.matcher(a);
//
//            while (m.find()) {
//                System.out.println(m.group(1));
//            }
//</editor-fold>
            int jumlahTree = 3;
//            for (int i = 0; i < jumlahTree; i++) {
//                final TreeCore treeCore = new TreeCore(300, connection);
//                treeCore.saveTree();
//                System.out.println("TREE "+i+" SELESAI");
//            }

            final TreeCore treeCore = new TreeCore(connection);
            Collection<File> listFiles = FileUtils.listFiles(new File("D:\\Netbeans Project\\RF\\riwayat"), new String[]{"tree"}, true);
            treeCore.deployPrediction(listFiles);

//            treeCore.deployPrediction(treeCore.loadTree(new File("pohon.tree")));
//            treeCore.saveTree();
//            treeCore.deployPrediction();
            //<editor-fold defaultstate="collapsed" desc="TreeCore Trial">
//            final Core core = new Core(1, connection);
//            final TreeNode<String> n1 = new ArrayMultiTreeNode("root");
//            TreeNode<String> n2 = new ArrayMultiTreeNode<>("A");
//            TreeNode<String> nx = new ArrayMultiTreeNode<>("X");
//            TreeNode<String> n3 = new ArrayMultiTreeNode<>("B");
//            TreeNode<String> n4 = new ArrayMultiTreeNode<>("C");
//            TreeNode<String> n5 = new ArrayMultiTreeNode<>("D");
//            n2.add(n3);
//            n3.add(n4);
//            n1.add(n2);
//            n1.add(nx);
//            System.out.println(n1);
//            System.out.println("----------------------");
//            
//            Collection<? extends TreeNode<String>> path = n1.path(n4);
//            List<TreeNode<String>> aa = new ArrayList();
//            aa.addAll(path);
//            Collections.reverse(aa);
//            System.out.println(aa.get(0));
//            System.out.println(aa.get(1));
            // Creating the tree nodes
//            TreeNode<String> n1 = new ArrayMultiTreeNode<>("n1");
//            TreeNode<String> n2 = new ArrayMultiTreeNode<>("n2");
//            TreeNode<String> n3 = new ArrayMultiTreeNode<>("n3");
//            TreeNode<String> n4 = new ArrayMultiTreeNode<>("n4");
//            TreeNode<String> n5 = new ArrayMultiTreeNode<>("n5");
//            TreeNode<String> n6 = new ArrayMultiTreeNode<>("n6");
//            TreeNode<String> n7 = new ArrayMultiTreeNode<>("n7");
//
//            n1.add(n2);
//            n1.add(n3);
//            n1.add(n4);
//            n2.add(n5);
//            n2.add(n6);
//            n4.add(n7);
//            
//            System.out.println("Level: "+n5.level());
//            
//            System.out.println(n1);
//</editor-fold>
            //<editor-fold defaultstate="collapsed" desc="First Level Entry">
//            try {
//                final HashMap<String,Integer> bulan = new HashMap();
//                bulan.put("jan", 1);
//                bulan.put("feb", 2);
//                bulan.put("mar", 3);
//                bulan.put("apr", 4);
//                bulan.put("may", 5);
//                bulan.put("jun", 6);
//                bulan.put("jul", 7);
//                bulan.put("aug", 8);
//                bulan.put("sep", 9);
//                bulan.put("oct", 10);
//                bulan.put("nov", 11);
//                bulan.put("dec", 12);
//                PreparedStatement ps = connection.prepareStatement("insert into x (age ,job ,marital , education ,credit_default  , balance , housing , loan , contact ,day ,month  , duration , campaign , pdays , previous , poutcome ,y ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
//                final BufferedReader br = new BufferedReader(new FileReader(new File("bank.csv")));
//                int counter = 0;
//                String total = "";
//                while ((total = br.readLine()) != null && counter < 500) {
//                    List<String> tokenList = new StringTokenizer(total, ",").getTokenList();
//                    for (int i = 0; i < tokenList.size(); i++) {
//                        if (i == 10) {
//                            ps.setObject(i+1, bulan.get(tokenList.get(i)));
//                        } else {
//                            ps.setObject(i+1, tokenList.get(i));
//                        }
//                    }
//                    ps.executeUpdate();
//                    counter++;
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//</editor-fold>
            //<editor-fold defaultstate="collapsed" desc="Third Level Entry">
            //            ResultSet rs = connection.prepareStatement("select * from bank").executeQuery();
//            final DescriptiveStatistics age = new DescriptiveStatistics();
//            final DescriptiveStatistics balance = new DescriptiveStatistics();
//            final DescriptiveStatistics duration = new DescriptiveStatistics();
//            final DescriptiveStatistics campaign = new DescriptiveStatistics();
//            final DescriptiveStatistics pdays = new DescriptiveStatistics();
//            final DescriptiveStatistics previous = new DescriptiveStatistics();
//            System.out.println("Feeding Statistics...");
//            while (rs.next()) {
//                age.addValue(rs.getDouble("age"));
//                balance.addValue(rs.getDouble("balance"));
//                duration.addValue(rs.getDouble("duration"));
//                campaign.addValue(rs.getDouble("campaign"));
//                pdays.addValue(rs.getDouble("pdays"));
//                previous.addValue(rs.getDouble("previous"));
//            }
//            System.out.println("Feeding Statistics Completed");
//            
//            rs = connection.prepareStatement("select * from bank").executeQuery();
//            int counter = 0;
//            while (rs.next()) {
//                String popula = "insert into zbank(age , admin , bluecollar , entrepeneur , housemaid , management , retired , selfemployed , services , student , technician , unemployed , job_unknown , divorced , married , single , education_primary , education_secondary , education_tertiary , education_unknown , credit_yes , credit_no ,balance ,housing_yes , housing_no , loan_default_yes , loan_default_no , cellular , telephone , contact_unknown ,day1 , day2 ,day3 ,day4 ,day5 ,day6 ,day7 ,day8 ,day9 ,day10 ,day11 ,day12 ,day13 ,day14 ,day15 ,day16 ,day17 ,day18 ,day19 ,day20 ,day21 ,day22 ,day23 ,day24 ,day25 ,day26 ,day27 ,day28 ,day29 ,day30 ,day31 ,jan , feb , mar , apr , may , june , july , aug , sep , oct , nov , december ,duration , campaign , pdays , previous , outcome_failure , outcome_other , outcome_succes , outcome_unknown , term_deposit) values (";
//                for(int i=0; i<82; i++){
//                    if(i == (82-1)){
//                        popula+="?";
//                    }else{
//                        popula+="?,";
//                    }
//                }
//                popula+=")";
//                PreparedStatement ps = connection.prepareStatement(popula);
//                ps.setDouble(1, Main.zscore(rs.getDouble("age"), age));
//                for(int i=2; i<=22; i++){
//                    ps.setInt(i, rs.getInt(i));
//                }
//                ps.setDouble(23, Main.zscore(rs.getDouble("balance"), balance));
//                for(int i=24; i<=73; i++){
//                    ps.setInt(i, rs.getInt(i));
//                }
//                ps.setDouble(74, Main.zscore(rs.getDouble("duration"),duration));
//                ps.setDouble(75, Main.zscore(rs.getDouble("campaign"),campaign));
//                ps.setDouble(76, Main.zscore(rs.getDouble("pdays"),pdays));
//                ps.setDouble(77, Main.zscore(rs.getDouble("previous"),previous));
//                for(int i=78; i<=82; i++){
//                    ps.setInt(i, rs.getInt(i));
//                }
//                ps.executeUpdate();
//                if(counter%1000==0){
//                    System.out.println("Passing: "+counter);
//                }
//                counter++;
//            }
//</editor-fold>
            //<editor-fold defaultstate="collapsed" desc="Second Level Entry">
//            PreparedStatement psSatu = connection.prepareStatement("select * from stringdb");
//            PreparedStatement psTiga = connection.prepareStatement("select * from bank");
//            //rsmd
//            ResultSetMetaData rsmd = psTiga.executeQuery().getMetaData();
//            int jumlahKolom = rsmd.getColumnCount();
//            ResultSet rs = psSatu.executeQuery();
//            int counter = 0;
//            while (rs.next()) {
//                String popula = "insert into bank(age , admin , bluecollar , entrepeneur , housemaid , management , retired , selfemployed , services , student , technician , unemployed , job_unknown , divorced , married , single , education_primary , education_secondary , education_tertiary , education_unknown , credit_yes , credit_no ,balance ,housing_yes , housing_no , loan_default_yes , loan_default_no , cellular , telephone , contact_unknown ,day1 , day2 ,day3 ,day4 ,day5 ,day6 ,day7 ,day8 ,day9 ,day10 ,day11 ,day12 ,day13 ,day14 ,day15 ,day16 ,day17 ,day18 ,day19 ,day20 ,day21 ,day22 ,day23 ,day24 ,day25 ,day26 ,day27 ,day28 ,day29 ,day30 ,day31 ,jan , feb , mar , apr , may , june , july , aug , sep , oct , nov , december ,duration , campaign , pdays , previous , outcome_failure , outcome_other , outcome_succes , outcome_unknown , term_deposit) values (";
//                for(int i=0; i<jumlahKolom; i++){
//                    if(i == (jumlahKolom-1)){
//                        popula+="?";
//                    }else{
//                        popula+="?,";
//                    }
//                }
//                popula+=")";
//                
//                final PreparedStatement psDua = connection.prepareStatement(popula);
//                psDua.setDouble(1, rs.getDouble("age"));
//                psDua.setInt(2, Main.intResponse(rs.getString("job"), "admin"));
//                psDua.setInt(3, Main.intResponse(rs.getString("job"), "bluecollar"));
//                psDua.setInt(4, Main.intResponse(rs.getString("job"), "entrepreneur"));
//                psDua.setInt(5, Main.intResponse(rs.getString("job"), "housemaid"));
//                psDua.setInt(6, Main.intResponse(rs.getString("job"), "management"));
//                psDua.setInt(7, Main.intResponse(rs.getString("job"), "retired"));
//                psDua.setInt(8, Main.intResponse(rs.getString("job"), "selfemployed"));
//                psDua.setInt(9, Main.intResponse(rs.getString("job"), "services"));
//                psDua.setInt(10, Main.intResponse(rs.getString("job"), "student"));
//                psDua.setInt(11, Main.intResponse(rs.getString("job"),"technician"));
//                psDua.setInt(12, Main.intResponse(rs.getString("job"),"unemployed"));
//                psDua.setInt(13, Main.intResponse(rs.getString("job"),"unknown"));
//                psDua.setInt(14, Main.intResponse(rs.getString("marital"), "single"));
//                psDua.setInt(15, Main.intResponse(rs.getString("marital"), "married"));
//                psDua.setInt(16, Main.intResponse(rs.getString("marital"), "divorced"));
//                psDua.setInt(17, Main.intResponse(rs.getString("education"), "primary"));
//                psDua.setInt(18, Main.intResponse(rs.getString("education"), "secondary"));
//                psDua.setInt(19, Main.intResponse(rs.getString("education"),"tertiary"));
//                psDua.setInt(20, Main.intResponse(rs.getString("education"),"unknown"));
//                psDua.setInt(21, Main.intResponse(rs.getString("credit_default"),"yes"));
//                psDua.setInt(22, Main.intResponse(rs.getString("credit_default"),"no"));
//                psDua.setDouble(23, rs.getDouble("balance"));
//                psDua.setInt(24, Main.intResponse(rs.getString("housing"),"yes"));
//                psDua.setInt(25, Main.intResponse(rs.getString("housing"),"no"));
//                ///////////////
//                psDua.setInt(26, Main.intResponse(rs.getString("loan"),"yes"));
//                psDua.setInt(27, Main.intResponse(rs.getString("loan"),"no"));
//                psDua.setInt(28, Main.intResponse(rs.getString("contact"),"cellular"));
//                psDua.setInt(29, Main.intResponse(rs.getString("contact"),"telephone"));
//                psDua.setInt(30, Main.intResponse(rs.getString("contact"),"unknown"));
//                int psCounter = 31;
//                for(int i=1; i<=31; i++){
//                    psDua.setInt(psCounter, Main.intResponse(rs.getString("day"), String.valueOf(i)));
//                    psCounter++;
//                }
//                psDua.setInt(63-1, Main.intResponse(rs.getString("month"), "jan"));
//                psDua.setInt(64-1, Main.intResponse(rs.getString("month"), "feb"));
//                psDua.setInt(65-1, Main.intResponse(rs.getString("month"), "mar"));
//                psDua.setInt(66-1, Main.intResponse(rs.getString("month"), "apr"));
//                psDua.setInt(67-1, Main.intResponse(rs.getString("month"), "may"));
//                psDua.setInt(68-1, Main.intResponse(rs.getString("month"), "jun"));
//                psDua.setInt(69-1, Main.intResponse(rs.getString("month"), "jul"));
//                psDua.setInt(70-1, Main.intResponse(rs.getString("month"),"aug"));
//                psDua.setInt(71-1, Main.intResponse(rs.getString("month"), "sep"));
//                psDua.setInt(72-1, Main.intResponse(rs.getString("month"), "oct"));
//                psDua.setInt(73-1, Main.intResponse(rs.getString("month"), "nov"));
//                psDua.setInt(74-1, Main.intResponse(rs.getString("month"), "dec"));
//                psDua.setDouble(75-1, rs.getDouble("duration"));
//                psDua.setDouble(76-1, rs.getDouble("campaign"));
//                psDua.setDouble(77-1, rs.getDouble("pdays"));
//                psDua.setDouble(78-1, rs.getDouble("previous"));
//                psDua.setInt(79-1, Main.intResponse(rs.getString("poutcome"), "other"));
//                psDua.setInt(80-1, Main.intResponse(rs.getString("poutcome"), "failure"));
//                psDua.setInt(81-1, Main.intResponse(rs.getString("poutcome"), "success"));
//                psDua.setInt(82-1, Main.intResponse(rs.getString("poutcome"), "unknown"));
//                if(rs.getString("Y").equals("yes")){
//                    psDua.setInt(83-1, 1);
//                }else{
//                    psDua.setInt(83-1, 0);
//                }
//                psDua.executeUpdate();
//                if(counter%5000==0){
//                    System.out.println("Passing: "+counter);
//                }
//                counter++;
//            }
//</editor-fold>
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static double zscore(double xi, DescriptiveStatistics d) {
        return (xi - d.getMean()) / d.getStandardDeviation();
    }

    private static int intResponse(String satu, String dua) {
        if (StringUtils.equalsIgnoreCase(satu, dua)) {
            return 1;
        } else {
            return 0;
        }
    }
}
