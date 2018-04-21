/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ml.rf;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author herley
 */
public class Classer {

    private String namaAtribut;
    private String kueriCabang;
    private int kelas = -1;
    private boolean targetClass = false;
    /**
     * 0: Nama Kolom 1: Nama Sub Kolom
     */
    private final HashMap<List<String>, Boolean> unfinishedQuery = new HashMap();

    /**
     *
     * @param kelas
     */
    public Classer(int kelas) {
        this.kelas = kelas;
        this.targetClass = true;
    }

    /**
     * 
     * @param namaAtribut 
     */
    public Classer(String namaAtribut){
        this.namaAtribut = namaAtribut;
    }
    
    /**
     * Constructor.
     *
     * @param namaAtribut
     * @param kueriCabang
     * @param connection
     */
    public Classer(String namaAtribut,  Connection connection) {
        this.namaAtribut = namaAtribut;

        try {
            if (connection != null) {
                if (kueriCabang == null) {
                    ResultSet rs = connection.prepareStatement("select distinct  " + this.namaAtribut + " from X order by " + this.namaAtribut).executeQuery();
                    final List<String> namaAtributSementara = new ArrayList();
                    while (rs.next()) {
                        namaAtributSementara.add(rs.getString(this.namaAtribut));
                    }

                    Iterator<String> iterSatu = namaAtributSementara.iterator();
                    while (iterSatu.hasNext()) {
                        String ne = iterSatu.next();
                        final List<String> apocal = new ArrayList();
                        apocal.add(this.namaAtribut);
                        apocal.add(ne);
                        this.unfinishedQuery.put(apocal, Boolean.FALSE);
                    }
                } else {
                    System.err.println("Kueri cabang tidak NULL detected.");
                    System.exit(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Mendapatkan query yang belum terselesaikan.
     * @return 
     */
    public List<String> getUnfinishedQuery() {
        List<String> hasil = null;
        Iterator<Map.Entry<List<String>, Boolean>> iterSatu = this.unfinishedQuery.entrySet().iterator();
        while (iterSatu.hasNext()) {
            Map.Entry<List<String>, Boolean> ne = iterSatu.next();
            if (!ne.getValue()) {
                hasil = ne.getKey();
                break;
            }
        }
        if (hasil != null) {
            this.unfinishedQuery.put(hasil, Boolean.TRUE);
        }
        return hasil;
    }

    public String getQueryBranch() {
        return this.kueriCabang;
    }

    @Override
    public String toString() {
        if (this.kelas == -1) {
            return this.namaAtribut;
        } else {
            if (this.kelas == 0) {
                return "NO";
            } else {
                return "YES";
            }
        }
    }

    /**
     * @return the targetClass
     */
    public boolean isTargetClass() {
        return targetClass;
    }
    
    /**
     * Unfinished query size
     * @return 
     */
    public int unfinishedQuerySize(){return this.unfinishedQuery.size();}
}
