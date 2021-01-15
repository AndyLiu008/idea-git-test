import com.xc.transform.DMLTransform;
import com.xc.transform.entity.CustomPattern;
import com.xc.transform.utils.ConfigurationUtils;
import com.xc.transform.utils.ConstantsUtils;
import com.xc.transform.utils.FileUtils;
import com.xc.transform.utils.PatternUtils;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class Test1 {
    static String[] fuhaoArray={":",  "\"",  ".",  "(",  ")",  " ",  "\t",  ";",  ",",  "\n"};
    @Test
    public void test1() throws Exception {

//        List<String> list=FileUtils.readFileList(
//                "C:\\Users\\andy\\Desktop\\inceptor-hive互转\\inceptor-all\\dml\\level1\\prc_ast_prd_secu_real_union.sql");
//        List<CustomPattern> dmlReplacePatterns = DMLTransform.dmlReplacePatterns;
//        PatternUtils.initPatternListNew(ConfigurationUtils.getConfMap(), dmlReplacePatterns);
//        System.out.println(dmlReplacePatterns);
        System.out.println(" ".equals(" "));
        System.out.println("     select basi_code,basi_name,curr_code,t_pont,pont_rat,t1_pont,basi_clas,inta_mode from mod20200825.rate_basi where t_date='20200225' );".length());

    }

    @Test
    public void sortTest1() throws Exception {
        String[] strings={"mid_l1_cal_prd_dtl","mid_l1_cal_basi","mid_l1_cal_basi_dtl","mid_l1_cal_prd","mid_l1_cal_prd_dtl2"};
        List<String> stringList=Arrays.asList(strings);
        System.out.println(stringList);
        stringList.sort((o1, o2) -> {return  o2.compareTo(o1);});
        System.out.println(stringList);
    }
    private void printMap(Map map, int count){
        Set set = map.keySet();
        for(Object key: set){

            Object value = map.get(key);

            for(int i=0; i<count; i++){
                System.out.print("    ");
            }

            if(value instanceof Map) {

                System.out.println(key+":");
                printMap((Map)value, count+1);
            }else if(value instanceof List){

                System.out.println(key+":");
                for(Object obj: (List)value){
                    for(int i=0; i<count; i++){
                        System.out.print("    ");
                    }
                    System.out.println("    - "+obj.toString());
                }
            }else{

                System.out.println(key + ": " + value);
            }
        }
    }





}
