/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ml.secrf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.scalified.tree.TreeNode;
import com.scalified.tree.multinode.ArrayMultiTreeNode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.collections4.OrderedMap;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.text.StringTokenizer;

/**
 *
 * @author herley
 */
public class TreeCore {

    public static final String CLASS_YES = "(y = 'yes')";
    public static final String CLASS_NO = "(y = 'no')";

    public static final String SAME_NUMBER_OF_CLASSES = TreeCore.CLASS_YES;

    private final Set<String> numericalAtr = new HashSet();
    private final List<String> columnNumber = new ArrayList();
    private final SetMultimap<String, String> featureAttribute = HashMultimap.create();
    private ResultSet rs;
    private TreeNode<String> root = null;
    private boolean repeatSubTreeParent = false;

    /**
     *
     * @param numericSeparator
     * @param connection
     */
    public TreeCore(int numericSeparator, Connection connection) {
        this.numericalAtr.add("AGE");
        this.numericalAtr.add("BALANCE");
        this.numericalAtr.add("DURATION");
        this.numericalAtr.add("CAMPAIGN");
        this.numericalAtr.add("PDAYS");
        this.numericalAtr.add("PREVIOUS");
        this.numericalAtr.add("DAY");
        this.numericalAtr.add("MONTH");
        try {
            /**
             * Mendapatkan nama kolom.
             */
            this.rs = connection.prepareStatement("select * from X fetch first 1 rows only").executeQuery();
            ResultSetMetaData rsmd = this.rs.getMetaData();
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                columnNumber.add(rsmd.getColumnName(i + 1));
            }

            /**
             * Memetakan subkolom.
             */
            Iterator<String> iterSatu = this.columnNumber.iterator();
            while (iterSatu.hasNext()) {
                final String namKol = iterSatu.next();
                if (this.numericalAtr.contains(namKol)) {
                    final List<Integer> sementara = new ArrayList();
                    // Numerical Attribute.
                    this.rs = connection.prepareStatement("select distinct " + namKol + " from X order by " + namKol + " ASC").executeQuery();
                    while (this.rs.next()) {
                        sementara.add(this.rs.getInt(namKol));
                    }
                    List<List<Integer>> partitionMaster = Lists.partition(sementara, (numericSeparator));

                    Iterator<List<Integer>> iterDua = partitionMaster.iterator();
                    while (iterDua.hasNext()) {
                        List<Integer> ne = iterDua.next();
                        int min = ne.get(0);
                        int max = ne.get(ne.size() - 1);
                        String penyimpan = "( " + namKol + " BETWEEN " + min + " AND " + max + " )";
                        this.featureAttribute.put(namKol, penyimpan);
                    }
                } else {
                    this.rs = connection.prepareStatement("select distinct " + namKol + " from X order by " + namKol + " ASC").executeQuery();
                    while (this.rs.next()) {
                        String kata = this.rs.getString(namKol);
                        kata = "'" + kata + "'";
                        this.featureAttribute.put(namKol, "(" + namKol + " = " + kata + ")");
                    }
                }
            }

            /**
             * Perhitungan root tree.
             */
            this.rs = connection.prepareStatement("select count(*) as hitung from X where y='yes'").executeQuery();
            this.rs.next();
            double jumlahKelasYes = this.rs.getDouble("hitung");
            this.rs = connection.prepareStatement("select count(*) as hitung from X where y='no'").executeQuery();
            this.rs.next();
            double jumlahKelasNo = this.rs.getDouble("hitung");
            double jumlahBarisKeseluruhan = jumlahKelasYes + jumlahKelasNo;
            double giniClass = (jumlahKelasYes / (jumlahKelasYes + jumlahKelasNo)) * (jumlahKelasNo / (jumlahKelasYes + jumlahKelasNo));

            HashMap<String, Double> nilaiAtribut = new HashMap();
            Iterator<Map.Entry<String, String>> iterDua = this.featureAttribute.entries().iterator();
            while (iterDua.hasNext()) {
                Map.Entry<String, String> ev = iterDua.next();
                if (!ev.getValue().equals("y")) {
                    String query = "select count(" + ev.getKey() + ") as hitung from X where ( " + ev.getValue() + ")";
                    this.rs = connection.prepareStatement(query).executeQuery();
                    this.rs.next();
                    double jumlahBarisPadaAtributIni = this.rs.getDouble("hitung");
                    double pengaliSatu = jumlahBarisPadaAtributIni / jumlahBarisKeseluruhan;
                    query = "select count(" + ev.getKey() + ") as hitung from X where ( " + ev.getValue() + ")" + " AND " + TreeCore.CLASS_YES;
                    this.rs = connection.prepareStatement(query).executeQuery();
                    this.rs.next();
                    double pengaliDua = this.rs.getDouble("hitung") / jumlahBarisPadaAtributIni;
                    query = "select count(" + ev.getKey() + ") as hitung from X where ( " + ev.getValue() + ")" + " AND " + TreeCore.CLASS_NO;
                    this.rs = connection.prepareStatement(query).executeQuery();
                    this.rs.next();
                    double pengaliTiga = this.rs.getDouble("hitung") / jumlahBarisPadaAtributIni;
                    double totalPengali = pengaliSatu * pengaliDua * pengaliTiga;
                    if (nilaiAtribut.containsKey(ev.getKey())) {
                        nilaiAtribut.put(ev.getKey(), (nilaiAtribut.get(ev.getKey()) + totalPengali));
                    } else {
                        nilaiAtribut.put(ev.getKey(), totalPengali);
                    }
                }
            }
            // Gini Gain
            Iterator<Map.Entry<String, Double>> iterTiga = nilaiAtribut.entrySet().iterator();
            final OrderedMap<Double, String> attributeMapper = new LinkedMap();
            while (iterTiga.hasNext()) {
                Map.Entry<String, Double> ne = iterTiga.next();
                attributeMapper.put(giniClass - ne.getValue(), ne.getKey());
            }

            this.root = new ArrayMultiTreeNode(attributeMapper.get(attributeMapper.lastKey()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        int ukuran = 1;
        {
            Iterator<String> iterDua = this.columnNumber.iterator();
            while (iterDua.hasNext()) {
                ukuran = ukuran * this.featureAttribute.get(iterDua.next()).size();
            }
        }

        int countSafer = 0;
        int maxCounter = 0;
        this.createSubTree(this.root, connection);
        TreeNode<String> subTreeParent = this.subTreeParent();
        while (subTreeParent != null && maxCounter < 120) {
//            System.out.println(subTreeParent.data());
            this.createSubTree(subTreeParent, connection);
            subTreeParent = this.subTreeParent();
            if (this.repeatSubTreeParent) {
                subTreeParent = this.subTreeParent();
                this.repeatSubTreeParent = false;
            }
            System.out.println("Counter : " + countSafer);
            countSafer++;
            maxCounter++;
        }

//        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$");
//        System.out.println(this.root);
    }

    /**
     * Mengembalikan parent dari satu subtree.
     *
     * @return
     */
    private TreeNode<String> subTreeParent() {
        Iterator<TreeNode<String>> iterSatu = this.root.postOrdered().iterator();
        while (iterSatu.hasNext()) {
            TreeNode<String> ne = iterSatu.next();
            if (ne == null) {
                return ne;
            } else {

                if (ne.isLeaf()) {

                    if (ne.data() != null) {
                        if (!(ne.data().equals(TreeCore.CLASS_YES) || ne.data().equals(TreeCore.CLASS_NO))) {
                            return ne;
                        }
                    } else {
                        ne.setData(TreeCore.SAME_NUMBER_OF_CLASSES);
                        this.repeatSubTreeParent = true;
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Membuat subtrees.
     *
     * @param parent
     * @param connection
     */
    private void createSubTree(TreeNode<String> parent, Connection connection) {
        TreeNode<String> pointerNode = parent;
        final List<String> sejarah = new ArrayList();
        while (pointerNode != null) {
            sejarah.add(pointerNode.data());
            pointerNode = pointerNode.parent();
        }

        Iterator<String> iterSatu = sejarah.iterator();
        while (iterSatu.hasNext()) {
            String atribut = iterSatu.next();
            Iterator<String> iterDua = this.featureAttribute.get(atribut).iterator();
            while (iterDua.hasNext()) {
                String pilihanAtribut = iterDua.next();
                TreeNode<String> nodeBayangan = parent;
                boolean tambahkan = true;
                while (nodeBayangan != null) {
                    Pattern p = Pattern.compile("\\((.*?)\\)");
                    Matcher m = p.matcher(pilihanAtribut);
                    m.find();
                    String satu = m.group(1);
                    String dua = nodeBayangan.data();

                    if (dua.contains("(")) {
//                        System.out.println("ADAAAAA --> "+xk.data()+" HEEY");
                        m = p.matcher(dua);
                        m.find();
                        dua = m.group(1);
                    }

                    if ((new StringTokenizer(satu).getTokenList().get(0).equals(new StringTokenizer(dua).getTokenList().get(0))) && (new StringTokenizer(satu).getTokenList().size() == new StringTokenizer(dua).getTokenList().size())) {
//                        System.out.println("Dua " + dua);
//                        System.out.println("Satu: " + satu);
//                        System.out.println();
                        tambahkan = false;
                    }
//                    System.out.println("Dua: "+dua);

                    nodeBayangan = nodeBayangan.parent();
                }
                if (tambahkan) {
                    parent.add(new ArrayMultiTreeNode(pilihanAtribut));
                }
//                parent.add(new ArrayMultiTreeNode(pilihanAtribut));
            }
        }

        /**
         * Penambahan node kelas.
         */
        {
            sejarah.clear();
            Iterator<? extends TreeNode<String>> iterDua = parent.subtrees().iterator();
            while (iterDua.hasNext()) {
                String where = "";
                TreeNode<String> currentParent = iterDua.next();
                pointerNode = currentParent;
                while (pointerNode != null) {
                    if (new StringTokenizer(pointerNode.data()).getTokenList().size() > 1) {
                        where += pointerNode.data() + " AND ";
                    }
                    pointerNode = pointerNode.parent();
                }
                List<String> tokenList = new StringTokenizer(where).getTokenList();
                where = "";
                for (int i = 0; i < tokenList.size() - 1; i++) {
                    where += tokenList.get(i) + " ";
                }

                try {

                    String query = "select count (y) as hitung from X where " + where + " AND " + TreeCore.CLASS_YES;
                    this.rs = connection.prepareStatement(query).executeQuery();
                    this.rs.next();
                    int jumlahYa = this.rs.getInt("hitung");

                    query = "select count (y) as hitung from X where " + where + " AND " + TreeCore.CLASS_NO;
                    this.rs = connection.prepareStatement(query).executeQuery();
                    this.rs.next();
                    int jumlahTidak = this.rs.getInt("hitung");

                    if (jumlahYa > 0 || jumlahTidak > 0) {
                        if (jumlahYa == 0) {
                            // Berarti ini kelas tidak.
                            currentParent.add(new ArrayMultiTreeNode(TreeCore.CLASS_NO));
                        } else if (jumlahTidak == 0) {
                            currentParent.add(new ArrayMultiTreeNode(TreeCore.CLASS_YES));
                        }
                    } // Kasus dimana jumlah kelas Ya sama dengan jumlah kelas Tidak.
                    else if (jumlahYa == jumlahTidak) {
                        currentParent.add(new ArrayMultiTreeNode(TreeCore.SAME_NUMBER_OF_CLASSES));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                sejarah.clear();
            }
        }

        /**
         * Pemeriksaan node yang bukan kelas.
         */
        /**
         * Same parent removal.
         */
        {
            Iterator<? extends TreeNode<String>> iterTiga = parent.subtrees().iterator();
            Set<String> pop = new HashSet();
            while (iterTiga.hasNext()) {
                TreeNode<String> ne = iterTiga.next();
                TreeNode<String> dua = ne.parent();
                boolean benarAda = false;
                while (dua != null) {
                    if (dua.data().equals(ne.data())) {
                        benarAda = true;
                    }

                    if (new StringTokenizer(dua.data()).size() == 1) {
                        pop.add(dua.data());
                    }

                    dua = dua.parent();
                }

                if (benarAda) {
                    parent.remove(ne);
                }
            }
            pop.remove(parent.data());
            iterTiga = parent.subtrees().iterator();
            while (iterTiga.hasNext()) {
                TreeNode<String> ne = iterTiga.next();
                String neData = new StringTokenizer(ne.data()).getTokenList().get(1);
                if (pop.contains(neData)) {
                    parent.remove(ne);
                }
            }
        }
        final List<TreeNode<String>> bukanKelas = new ArrayList();
//        Iterator<? extends TreeNode<String>> iterDua = parent.subtrees().iterator();
        Iterator<? extends TreeNode<String>> iterDua = this.root.preOrdered().iterator();

        while (iterDua.hasNext()) {
            TreeNode<String> next = iterDua.next();
//            if (next.isLeaf() && !(StringUtils.equalsIgnoreCase(next.data(), CLASS_NO) || StringUtils.equalsIgnoreCase(next.data(), CLASS_YES))) {
//                bukanKelas.add(next);
//            }

            if (next.isLeaf()) {

//                System.out.println("Leaf Data: "+next.data());
                if (next.data().equals(TreeCore.CLASS_NO) || next.data().equals(TreeCore.CLASS_YES)) {

                } else {
                    bukanKelas.add(next);
                }
            }

        }

        /**
         * Pemeriksaan element= bukan kelas
         */
        {

//            System.out.println(this.root);
//            Iterator<TreeNode<String>> iterTiga = bukanKelas.iterator();
//            while (iterTiga.hasNext()) {
//                TreeNode<String> elementBukanKelas = iterTiga.next();
//                System.out.println(elementBukanKelas);
//                System.out.println("Elemen bukan kelas: "+elementBukanKelas.data());
//                System.out.println("Elemen bukan kelas parent: "+elementBukanKelas.parent().data());
//                System.out.println();
//            }
        }

        if (bukanKelas.isEmpty()) {
//            System.out.println(parent.parent());
//            System.out.println(this.root);

            TreeNode<String> find = this.root.find("LOAN");
            System.out.println(find);
            System.out.println(find.isLeaf());
        }

        Iterator<TreeNode<String>> iterTiga = bukanKelas.iterator();
        sejarah.clear();
        while (iterTiga.hasNext()) {
            TreeNode<String> nodeSekarang = iterTiga.next();
            pointerNode = nodeSekarang;
            sejarah.clear();
            while (pointerNode != null) {
                sejarah.add(pointerNode.data());
                pointerNode = pointerNode.parent();
            }
            String where = "";
            String select = "";

//            if(sejarah.size() == 3){
//                System.out.println(sejarah);
//                System.out.println(this.root);
//            }else{
//                System.out.println(sejarah);
//            }
            if (sejarah.size() % 2 == 0) {

                for (int i = 0; i < sejarah.size(); i++) {
                    if (i % 2 == 0) {
                        where += " " + sejarah.get(i) + " " + " AND ";
                    } else {
                        select += sejarah.get(i) + " , ";
                    }
                }

                List<String> tokenList = new StringTokenizer(select).getTokenList();
                select = "";
                for (int i = 0; i < tokenList.size() - 1; i++) {
                    select += tokenList.get(i);
                }
                tokenList = new StringTokenizer(where).getTokenList();
                where = "";

                for (int i = 0; i < tokenList.size() - 1; i++) {
                    where += tokenList.get(i) + " ";
                }

                String query = "select count (y) as hitung from X where " + where;
                try {
                    this.rs = connection.prepareStatement(query).executeQuery();
                    rs.next();
                    double jumlahSemuaKelas = rs.getDouble("hitung");
                    String kelasTidak = query + " AND " + TreeCore.CLASS_NO;
                    rs = connection.prepareStatement(kelasTidak).executeQuery();
                    rs.next();
                    double jumlahKelasTidak = rs.getDouble("hitung");
                    String kelasYa = query + " AND " + TreeCore.CLASS_YES;
                    rs = connection.prepareStatement(kelasYa).executeQuery();
                    rs.next();
                    double jumlahKelasYa = rs.getDouble("hitung");
                    double giniClass = (jumlahKelasYa / jumlahSemuaKelas) * (jumlahKelasTidak / (jumlahSemuaKelas));

                    // Peluan Setiap atribut
                    final Set<String> hashSet = new HashSet();
                    Iterator<String> iterEmpat = new StringTokenizer(where).getTokenList().iterator();
                    while (iterEmpat.hasNext()) {
                        String apocal = iterEmpat.next();
                        if (this.columnNumber.contains(apocal.toUpperCase())) {
                            hashSet.add(apocal);
                        }
                    }

                    Iterator<String> iterLima = this.columnNumber.iterator();
                    final HashMap<String, Double> atributAkumulator = new HashMap();
                    while (iterLima.hasNext()) {
                        String apocal = iterLima.next();
                        if (!hashSet.contains(apocal) && !apocal.equals("Y")) {
                            // Allowed Gini Calculation.
                            Iterator<String> apocalSet = this.featureAttribute.get(apocal).iterator();
                            while (apocalSet.hasNext()) {
                                String apocalSetKata = apocalSet.next();
                                String queryDua = "select count (" + apocal + ") as hitung from X where " + where + " AND " + apocalSetKata;
                                this.rs = connection.prepareStatement(queryDua).executeQuery();
                                this.rs.next();
                                double jumlahKeseluruhanAtribut = this.rs.getDouble("hitung");
                                if (jumlahKeseluruhanAtribut == Double.NaN) {
                                    jumlahKeseluruhanAtribut = 0.0;
                                }
                                String atributKelasYa = queryDua + " AND " + TreeCore.CLASS_YES + " AND " + apocalSetKata;
                                this.rs = connection.prepareStatement(atributKelasYa).executeQuery();
                                this.rs.next();
                                double jumlahYaAtribut = this.rs.getDouble("hitung");
                                if (jumlahYaAtribut == Double.NaN) {
                                    jumlahYaAtribut = 0.0;
                                }

                                String atributKelasTidak = queryDua + " AND " + TreeCore.CLASS_NO + " AND " + apocalSetKata;
                                this.rs = connection.prepareStatement(atributKelasTidak).executeQuery();
                                this.rs.next();
                                double jumlahTidakAtribut = this.rs.getDouble("hitung");
                                if (jumlahTidakAtribut == Double.NaN) {
                                    jumlahTidakAtribut = 0.0;
                                }

                                double giniAtribut = (jumlahKeseluruhanAtribut / jumlahSemuaKelas) * (jumlahYaAtribut / jumlahKeseluruhanAtribut) * (jumlahTidakAtribut / jumlahKeseluruhanAtribut);
                                if (giniAtribut == Double.NaN) {
                                    giniAtribut = 0.0;
                                }

                                if (giniAtribut != Double.NaN) {
                                    if (atributAkumulator.get(apocal) == null) {
                                        atributAkumulator.put(apocal, giniAtribut);
                                    } else {
                                        atributAkumulator.put(apocal, atributAkumulator.get(apocal) + giniAtribut);
                                    }
                                }
                            }
                        }
                    }

                    final OrderedMap<Double, String> atributMapper = new LinkedMap();
                    final List<Double> bufferedDouble = new ArrayList();
                    Iterator<Map.Entry<String, Double>> iterEnam = atributAkumulator.entrySet().iterator();
                    while (iterEnam.hasNext()) {
                        Map.Entry<String, Double> ne = iterEnam.next();
                        atributMapper.put(giniClass - ne.getValue(), ne.getKey());
                        bufferedDouble.add(giniClass - ne.getValue());
                    }

                    Collections.sort(bufferedDouble);
                    Collections.reverse(bufferedDouble);

                    Iterator<Double> buffIterator = bufferedDouble.iterator();
                    Double nilaiPanggilan = null;
                    while (buffIterator.hasNext() && (nilaiPanggilan == null)) {
                        Double nilaiBuf = buffIterator.next();
                        if (nilaiBuf != Double.NaN) {
                            String stringMapper = atributMapper.get(nilaiBuf);
                            boolean adanya = false;

                            final List<TreeNode<String>> apax = new ArrayList();
                            apax.addAll(this.root.path(nodeSekarang));
                            Collections.reverse(apax);

                            for (int i = 1; i < apax.size(); i++) {
                                if (stringMapper.equals(apax.get(i).data())) {
                                    adanya = true;
                                }
                            }

                            /////////
                            if (!adanya) {
                                nilaiPanggilan = nilaiBuf;
//                            concasa.add(stringMapper);
                                break;
                            }
                        }
                    }
                    TreeNode<String> baru = new ArrayMultiTreeNode(atributMapper.get(nilaiPanggilan));
//                TreeNode<String> baru = new ArrayMultiTreeNode(atributMapper.get(atributMapper.lastKey()));
                    nodeSekarang.add(baru);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            sejarah.clear();
        }

        System.out.println(this.root);
        System.out.println("*********************************************");
    }

}
