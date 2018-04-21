/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ml.rf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import org.apache.commons.text.StringTokenizer;

/**
 *
 * @author herley
 */
public class StringDB {

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
            Connection connection = DriverManager.getConnection("jdbc:derby://localhost:1527/pm", "herley", "rahasia");

            final BufferedReader br = new BufferedReader(new FileReader(new File("bank.csv")));
            String total = "";
            PreparedStatement ps = connection.prepareStatement("insert into stringdb (age,job,marital,education,credit_default,balance,housing,loan,contact,day,month,duration,campaign,pdays,previous,poutcome,y) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            int counter = 0;
            while ((total = br.readLine()) != null) {
                final List<String> tokenList = new StringTokenizer(total, ",").getTokenList();
                ps.setDouble(1, Double.parseDouble(tokenList.get(0)));
                for (int i = 2; i <= 11; i++) {
                    ps.setString(i, tokenList.get(i - 1).toLowerCase());
                }

                for (int i = 12; i <= 15; i++) {
                    ps.setDouble(i, Double.parseDouble(tokenList.get(i - 1)));
                }
                
                for(int i=16; i<=17; i++){
                    ps.setString(i, tokenList.get(i-1));
                }
                ps.executeUpdate();

                if (counter % 1000 == 0) {
                    System.out.println("Passing: " + counter);
                }

                counter++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
