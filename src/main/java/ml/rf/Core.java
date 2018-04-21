/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ml.rf;

import com.scalified.tree.TreeNode;
import com.scalified.tree.multinode.ArrayMultiTreeNode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.text.TextStringBuilder;
import org.magicwerk.brownies.collections.BigList;

/**
 *
 * @author herley
 */
public class Core {

    public static final int NO = 0;
    public static final int YES = 1;

    private List<String> namaKolom;

    // Root
    private TreeNode<Classer> root;

    // Numerical Atribut list.
    private final Set<String> numericalAtr = new HashSet();
    private final Map<String, Double> giniGainHashMap = new TreeMap();

    /**
     *
     * @param numberOfTree
     * @param connection
     */
    public Core(int numberOfTree, Connection connection) {

        // Numerical Atribute.
        this.numericalAtr.add("AGE");
        this.numericalAtr.add("BALANCE");
        this.numericalAtr.add("DURATION");
        this.numericalAtr.add("CAMPAIGN");
        this.numericalAtr.add("PDAYS");
        this.numericalAtr.add("PREVIOUS");
        this.numericalAtr.add("DAY");
        this.numericalAtr.add("MONTH");

        System.out.println("Menyusun pohon...");
        try {
            ResultSet rs = connection.prepareStatement("select count(Y) as jumlah from X where Y = 'no'").executeQuery();
            rs.next();
            double kelasA = rs.getDouble("jumlah");
            rs = connection.prepareStatement("select count(Y) as jumlah from X where y='yes'").executeQuery();
            rs.next();
            double kelasB = rs.getDouble("jumlah");
            rs = connection.prepareStatement("select count(*) as total_semua from X ").executeQuery();
            rs.next();
            double totalSemua = rs.getDouble("total_semua");
            kelasA = kelasA / totalSemua;
            kelasB = kelasB / totalSemua;

            double giniClass = kelasA * kelasB;

            rs = connection.prepareStatement("select * from X fetch first 1 rows only").executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            this.namaKolom = new BigList();
            // Karena kolom kelas tidak perlu ikut.
            for (int i = 1; i <= metaData.getColumnCount() - 1; i++) {
                namaKolom.add(metaData.getColumnName(i));
            }
            // Lanjut gini
            Iterator<String> columnNameIterator = namaKolom.iterator();
            while (columnNameIterator.hasNext()) {
                final String colName = columnNameIterator.next();
                if (this.numericalAtr.contains(colName)) {
                    System.out.println("Nama Atribut Numerik: " + colName);
                    /**
                     * Dapatkan nilai terendah dan tertingginya.
                     */
                    TextStringBuilder tsb = new TextStringBuilder();
                    tsb.append("select distinct ");
                    tsb.append(colName);
                    tsb.append(" from X order by ");
                    tsb.append(colName);
                    rs = connection.prepareStatement(tsb.toString()).executeQuery();
                    final List<Double> nilai = new BigList();
                    while (rs.next()) {
                        nilai.add(rs.getDouble(colName));
                    }
                    System.out.println("Perhitungan Pengali Pertama...");
                    List<TextStringBuilder> pengaliDalamTsb = new BigList();
                    List<Double> kelasNo = new BigList();
                    List<Double> kelasYes = new BigList();
                    List<Double> koefisienPengali = this.pengaliLuarNumerik(colName, nilai, pengaliDalamTsb, totalSemua, connection);
                    System.out.println("Perhitungan Pengali Pertama Selesai");
                    System.out.println("Perhitungan Pengali Kedua...");
                    this.pengaliDalam(pengaliDalamTsb, kelasNo, kelasYes, connection);
                    System.out.println("Perhitungan Pengali Kedua Selesai");
                    double probabilitasSatuAtribut = this.penjumlahanSatuAtribut(koefisienPengali, kelasNo, kelasYes);
                    double giniGain = giniClass - probabilitasSatuAtribut;
                    this.giniGainHashMap.put(colName, giniGain);
                    System.out.println("Gini Gain: " + giniGain);
                    System.out.println("****************************************");
                } else {
                    System.out.println("Nama Atribut Kategorik: " + colName);
                    final List<String> subColName = new ArrayList();
                    final TextStringBuilder tsb = new TextStringBuilder("select distinct " + colName + " from X order by " + colName + " asc");
                    rs = connection.prepareStatement(tsb.toString()).executeQuery();
                    while (rs.next()) {
                        subColName.add(rs.getString(colName));
                    }

                    final List<Double> pengaliLuar = new BigList();
                    final List<Double> pengaliDalamSatu = new BigList();
                    final List<Double> pengaliDalamDua = new BigList();

                    Iterator<String> iterDua = subColName.iterator();
                    while (iterDua.hasNext()) {
                        String namaSubKolom = iterDua.next();
                        Double pengaliLuarKategorik = this.pengaliLuarKategorik(colName, namaSubKolom, totalSemua, connection);
                        double[] pengaliDalamKategorik = this.pengaliDalamKategorik(colName, namaSubKolom, totalSemua, connection);
                        /**
                         * Penyimpanan nilai atribut.
                         */
                        pengaliLuar.add(pengaliLuarKategorik);
                        pengaliDalamSatu.add(pengaliDalamKategorik[Core.NO]);
                        pengaliDalamDua.add(pengaliDalamKategorik[Core.YES]);
                    }

                    double totalNilai = 0;
                    for (int i = 0; i < pengaliLuar.size(); i++) {
                        totalNilai += pengaliLuar.get(i) * pengaliDalamSatu.get(i) * pengaliDalamDua.get(i);
                    }

                    this.giniGainHashMap.put(colName, giniClass - totalNilai);
                    System.out.println("Total Nilai: " + totalNilai);
                    System.out.println("****************************************");
                }
            }

            MyComparator comp = new MyComparator(this.giniGainHashMap);
            Map<String, Double> treeMap = new TreeMap(comp);
            treeMap.putAll(this.giniGainHashMap);

            if (this.numericalAtr.contains(treeMap.entrySet().iterator().next().getKey())) {
                System.err.println("Alerted Numerical Attribute as this node");
                System.exit(0);
            } else {
                this.root = new ArrayMultiTreeNode(new Classer(treeMap.entrySet().iterator().next().getKey(), connection));

                System.out.println("##########################");
                System.out.println(this.root);
                TreeNode<Classer> parent = this.root;

                for (int i = 0; i < parent.data().unfinishedQuerySize(); i++) {
                    List<String> unfinishedQueryList = parent.data().getUnfinishedQuery();
                    int reachedClass = this.isClassReached(unfinishedQueryList, connection);
                    if (reachedClass != -1) {
                        TreeNode<Classer> beforeLeaf = new ArrayMultiTreeNode(new Classer(unfinishedQueryList.get(1)));
                        final TreeNode<Classer> leaf = new ArrayMultiTreeNode(new Classer(reachedClass));
                        parent.add(beforeLeaf);
                        beforeLeaf.add(leaf);
                        System.out.println(this.root);
                    } else {
                        TreeNode<Classer> beforeLeaf = new ArrayMultiTreeNode(new Classer(unfinishedQueryList.get(1)));
                        parent.add(beforeLeaf);
                    }
                }

                int safeCounter = 0;
                TreeNode<Classer> belumSelesai = this.getUnfinishedLeaf();
                while ((belumSelesai != null) && (safeCounter < 1)) {
                    // Mendapatkan leaf yang belum selesai.
                    belumSelesai = this.getUnfinishedLeaf();
                    if (this.numericalAtr.contains(belumSelesai.toString())) {
                        System.err.println("Numerical Attibute Detected on belumSelesai, System exit.");
                        System.exit(0);
                    } else {
                        Double giniClassTreeNode = this.giniClassTreeNode(belumSelesai, connection);
                        this.giniAttributTreeNode(belumSelesai, connection);

                        // Node yang lain???
                    }
                    safeCounter++;
                }
                System.out.println("##########################");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Double giniAttributTreeNode(TreeNode<Classer> node, Connection connection) {
        try {
            TreeNode<Classer> semen = node;
            final List<String> data = new ArrayList();
            while (semen != this.root) {
                data.add(semen.data().toString());
                semen = semen.parent();
            }
            data.add(this.root.data().toString());
            Collections.reverse(data);
            
            //xxxxxxxxxxxxxxxxxxxxxxxxx
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param node
     * @param connection
     * @return
     */
    private Double giniClassTreeNode(TreeNode<Classer> node, Connection connection) {
        try {
            TreeNode<Classer> semen = node;
            final List<String> data = new ArrayList();
            while (semen != this.root) {
                data.add(semen.data().toString());
                semen = semen.parent();
            }
            data.add(this.root.data().toString());
            Collections.reverse(data);
//            System.out.println("Data: "+data.size());

            String query = "select count(*) as hitung from X where ";
            for (int i = 0; i < data.size(); i = i + 2) {
                if ((i + 2) >= (data.size() - 1)) {
                    query += "(" + data.get(i) + " = '" + data.get(i + 1) + "')";
                } else {
                    query += "(" + data.get(i) + " = '" + data.get(i + 1) + "')" + " AND ";
                }
            }
            String ya = query + " AND (Y='yes')";
            String tidak = query + " AND (Y='no')";

            ResultSet rs = connection.prepareStatement(ya).executeQuery();
            rs.next();
            double kelasYa = rs.getDouble("hitung");
            rs = connection.prepareStatement(tidak).executeQuery();
            rs.next();
            double kelasTidak = rs.getDouble("hitung");
            double total = kelasYa + kelasTidak;
            kelasYa = kelasYa / total;
            kelasTidak = kelasTidak / total;

//            System.out.println(kelasYa);
//            System.out.println(kelasTidak);
//            System.out.println("Gini: "+(kelasYa * kelasTidak));
            return kelasYa * kelasTidak;

//            System.out.println(query);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Mengembalikan node tidak null.
     *
     * @return
     */
    private TreeNode<Classer> getUnfinishedLeaf() {
        Iterator<TreeNode<Classer>> iterSatu = this.root.postOrdered().iterator();
        while (iterSatu.hasNext()) {
            TreeNode<Classer> next = iterSatu.next();
            if ((next != null) && !(next.isLeaf() && next.data().isTargetClass())) {
                return next;
            }
        }
        return null;
    }

    /**
     *
     *
     * @param kolom
     * @param connection
     * @return
     */
    private int isClassReached(List<String> kolom, Connection connection) {
        try {
            String namaKolom = kolom.get(0);
            String namaSubKolom = kolom.get(1);

            String query = "select count (" + "'" + namaKolom + "'" + ") as hitung from X where " + namaKolom + " = " + "'" + namaSubKolom + "'" + " AND y='yes' ";

            ResultSet rs = connection.prepareStatement(query).executeQuery();
            rs.next();
            int kelasYes = rs.getInt("hitung");

            query = "select count (" + "'" + namaKolom + "'" + ") as hitung from X where " + namaKolom + " = " + "'" + namaSubKolom + "'" + " AND y='no' ";

            rs = connection.prepareStatement(query).executeQuery();
            rs.next();
            int kelasNo = rs.getInt("hitung");

            if ((kelasYes >= 0 && kelasNo == 0) || (kelasYes == 0 && kelasNo >= 0)) {
                if (kelasYes > 0) {
                    return 1;
                } else if (kelasNo > 0) {
                    return 0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     *
     * @param koefisienPengali
     * @param kelasNo
     * @param kelasYes
     * @return
     */
    private double penjumlahanSatuAtribut(List<Double> koefisienPengali, List<Double> kelasNo, List<Double> kelasYes) {
        double jumlah = 0.0;
        for (int i = 0; i < koefisienPengali.size(); i++) {
            jumlah += koefisienPengali.get(i) * kelasNo.get(i) * kelasYes.get(i);
        }
        return jumlah;
    }

    /**
     *
     * @param pengaliDalam
     * @param kelasNo
     * @param kelasYes
     * @param connection
     */
    private void pengaliDalam(List<TextStringBuilder> pengaliDalam, List<Double> kelasNo, List<Double> kelasYes, Connection connection) {
        try {
            Iterator<TextStringBuilder> iterSatu = pengaliDalam.iterator();
            int counter = 0;
            while (iterSatu.hasNext()) {
                final TextStringBuilder tsb = iterSatu.next();
                final TextStringBuilder tsbSatu = new TextStringBuilder(tsb.toString());
                final TextStringBuilder tsbDua = new TextStringBuilder(tsb.toString());
                String ya = " AND (Y='yes')";
                String tidak = " AND (Y='no')";
                tsbSatu.append(ya);
                tsbDua.append(tidak);
                ResultSet rs = connection.prepareStatement(tsbSatu.toString()).executeQuery();
                rs.next();
                double yes = rs.getDouble("hitung");
                rs = connection.prepareStatement(tsbDua.toString()).executeQuery();
                rs.next();
                double no = rs.getDouble("hitung");
                double total = yes + no;
                yes = yes / total;
                no = no / total;
                kelasNo.add(no);
                kelasYes.add(yes);
                if (counter % 1000 == 0) {
                    System.out.println("Passing: " + counter);
                }
                counter++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private double[] pengaliDalamKategorik(String colName, String subColName, double totalSemua, Connection connection) {
        try {
            final TextStringBuilder tsb = new TextStringBuilder();
            tsb.append("select count ");
            tsb.append("(" + colName + ")");
            tsb.append(" as hitung from X where " + colName + " = " + "'" + subColName + "'");
            tsb.append("AND (Y='no')");
            System.out.println(tsb.toString());
            ResultSet rs = connection.prepareStatement(tsb.toString()).executeQuery();
            rs.next();
            double nilaiNo = rs.getDouble("hitung");

            tsb.clear();
            tsb.append("select count ");
            tsb.append("(" + colName + ")");
            tsb.append(" as hitung from X where " + colName + " = " + "'" + subColName + "'");
            tsb.append("AND (Y='yes')");
            rs = connection.prepareStatement(tsb.toString()).executeQuery();
            rs.next();
            double nilaiYes = rs.getDouble("hitung");
            final double[] nilaiKelas = new double[2];
            nilaiKelas[Core.NO] = nilaiNo / totalSemua;
            nilaiKelas[Core.YES] = nilaiYes / totalSemua;
            return nilaiKelas;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param colName
     * @param subColName
     * @param totalSemua
     * @param connection
     * @return
     */
    private Double pengaliLuarKategorik(String colName, String subColName, double totalSemua, Connection connection) {
        try {
            final TextStringBuilder tsb = new TextStringBuilder();
            tsb.append("select count ");
            tsb.append("(" + colName + ")");
            tsb.append(" as hitung from X where " + colName + " = " + "'" + subColName + "'");
            ResultSet rs = connection.prepareStatement(tsb.toString()).executeQuery();
            rs.next();
            return rs.getDouble("hitung") / totalSemua;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param colName
     * @param nilai
     * @param pengaliDalam
     * @param totalSemua
     * @param connection
     * @return
     */
    private List<Double> pengaliLuarNumerik(String colName, List<Double> nilai, List<TextStringBuilder> pengaliDalam, double totalSemua, Connection connection) {
        final List<Double> hasil = new BigList();
        double nilaiBesar = nilai.get(nilai.size() - 1);
        System.out.println(colName);
        System.out.println("Value Size: " + nilai.size());
        for (int i = 0; i < nilai.size(); i++) {
            double nilaiKecil = nilai.get(i);
            TextStringBuilder tsb = new TextStringBuilder();
            tsb.append("select count");
            tsb.append("(" + colName + ")");
            tsb.append(" as hitung from X where ");
            tsb.append(colName);
            tsb.append(" between ");
            tsb.append(nilaiKecil);
            tsb.append(" AND ");
            tsb.append(nilaiBesar);
            try {
                pengaliDalam.add(tsb);
                ResultSet rs = connection.prepareStatement(tsb.toString()).executeQuery();
                rs.next();
                hasil.add(rs.getDouble("hitung") / totalSemua);
            } catch (SQLException ex) {
                Logger.getLogger(Core.class.getName()).log(Level.SEVERE, null, ex);
            }
            if (i % 500 == 0) {
                System.out.println("Passing: " + i);
            }
        }
        return hasil;
    }
}

class MyComparator implements Comparator {

    Map map;

    public MyComparator(Map map) {
        this.map = map;
    }

    @Override
    public int compare(Object o1, Object o2) {
        return ((Double) map.get(o2)).compareTo((Double) map.get(o1));

    }
}
