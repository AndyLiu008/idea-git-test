package com.xc.transform;

import com.xc.transform.entity.DmlIdx;
import com.xc.transform.entity.ShellInfo;
import com.xc.transform.utils.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TransformJob {

    public static boolean isPrint=false;
    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("无配置文件！");
            System.exit(1);
        }

        ConfigurationUtils.init(args[0]);//初始化yaml配置

        System.out.println("inceptor_all:"+ConfigurationUtils.getSourceAllPath());
        System.out.println("hive_all:"+ConfigurationUtils.getTargetAllPath());
        System.out.println("inceptor_update:"+ConfigurationUtils.getSourceUpdatePath());
        System.out.println("hive_update:"+ConfigurationUtils.getTargetUpdatePath());

        FileUtils.deleteEveryThing(ConfigurationUtils.getTargetAllPath());//删除原来的hive-all文件
        FileUtils.deleteEveryThing(ConfigurationUtils.getTargetUpdatePath());//删除原来的hive-update文件


        FileUtils.mkDir(ConfigurationUtils.getTargetUpdatePath());

        if(ConfigurationUtils.needTestHiveUpdate()){
            FileUtils.deleteEveryThing(ConfigurationUtils.getTestHiveUpdatePath());
            FileUtils.mkDir(ConfigurationUtils.getTestHiveUpdatePath());
        }

        List<String> ddlDirs=FileUtils.getSubDirs(ConfigurationUtils.getSourceAllPath()+ConstantsUtils.ddlName);//获取ddl子目录
        List<String> dmlDirs=FileUtils.getSubDirs(ConfigurationUtils.getSourceAllPath()+ConstantsUtils.dmlName);//获取dml子目录


        FileUtils.mkDdlDmlDir(ConfigurationUtils.getTargetAllPath()+ConstantsUtils.ddlName,ddlDirs);//创建 hive all ddl子目录
        FileUtils.mkDdlDmlDir(ConfigurationUtils.getTargetAllPath()+ConstantsUtils.dmlName,dmlDirs);//创建 hive all dml子目录
        FileUtils.mkDdlDmlDir(ConfigurationUtils.getTargetAllPath()+ConstantsUtils.shellName,dmlDirs);//创建 hive all shell子目录

        for(int i=0;i<ddlDirs.size();i++){//子目录遍历 如level0 level1 level2
            String soucePath=ConfigurationUtils.getSourceAllPath()+ConstantsUtils.ddlName+ddlDirs.get(i);
            String targetPath=ConfigurationUtils.getTargetAllPath()+ConstantsUtils.ddlName+ddlDirs.get(i);

            File file=new File(soucePath);
            for(File subFile:file.listFiles()){//具体文件遍历
                ddlTransfrom(soucePath,subFile.getName(),targetPath);
            }
        }

        ConstantsUtils.setSortDbTableMapMap(ConstantsUtils.getSortMap(ConstantsUtils.dbTableMap));//使得排序大的字符串在前。如 mid_l2_prd_ast_clas_dtl 排在 mid_l2_prd_ast_clas前面
        ConstantsUtils.setSortTableMapMap(ConstantsUtils.getSortMap(ConstantsUtils.tableMap));
        System.out.println("ConstantsUtils.sortMap:"+ConstantsUtils.sortDbTableMapMap);

        if(ConstantsUtils.inceptor_to_hive.equals(ConfigurationUtils.getTransformDir())){
            for(int i=0;i<dmlDirs.size();i++){//子目录遍历 如level1 level2
                String soucePath=ConfigurationUtils.getSourceAllPath()+ConstantsUtils.dmlName+dmlDirs.get(i);
                String targetDmlPath=ConfigurationUtils.getTargetAllPath()+ConstantsUtils.dmlName+dmlDirs.get(i);
                String targetShellPath=ConfigurationUtils.getTargetAllPath()+ConstantsUtils.shellName+dmlDirs.get(i);

                File file=new File(soucePath);
                for(File subFile:file.listFiles()){//具体文件遍历
                    dmlTransfrom(soucePath,subFile.getName(),dmlDirs.get(i),targetDmlPath,targetShellPath);

                }
            }
        }


        if(ConstantsUtils.hive_to_hive.equals(ConfigurationUtils.getTransformDir())){
            ConstantsUtils.addDbTableMap();
            ConstantsUtils.setSortDbTableMapMap(ConstantsUtils.getSortMap(ConstantsUtils.dbTableMap));

            for(int i=0;i<dmlDirs.size();i++){//子目录遍历 如level1 level2
                String soucePath=ConfigurationUtils.getSourceAllPath()+ConstantsUtils.dmlName+dmlDirs.get(i);
                String targetDmlPath=ConfigurationUtils.getTargetAllPath()+ConstantsUtils.dmlName+dmlDirs.get(i);
                String targetShellPath=ConfigurationUtils.getTargetAllPath()+ConstantsUtils.shellName+dmlDirs.get(i);

                File file=new File(soucePath);
                for(File subFile:file.listFiles()){//具体文件遍历
                    hive_hive_dmlTransfrom(soucePath,subFile.getName(),dmlDirs.get(i),targetDmlPath,targetShellPath);
                }
            }

            for(int i=0;i<dmlDirs.size();i++){
                String sourcePath=ConfigurationUtils.getSourceAllPath()+ConstantsUtils.shellName+dmlDirs.get(i);
                String targetPath=ConfigurationUtils.getTargetAllPath()+ConstantsUtils.shellName+dmlDirs.get(i);
                File file=new File(sourcePath);
                if(!file.exists()){
                    FileUtils.deleteEveryThing(ConfigurationUtils.getTargetAllPath()+ConstantsUtils.shellName);
                    break;
                }
                for(File subFile:file.listFiles()){//具体文件遍历
                    List<String> shellStrings= FileUtils.readFileList(subFile.getAbsolutePath());
                    DMLTransform.replaceDmlDb(shellStrings);
                    DMLTransform.replaceDmlTb(shellStrings);
                    FileUtils.writeFileList(targetPath+ShellTransform.getTargetShellFileName(subFile.getName()),shellStrings,false);
                }
            }
        }

        UpdateFileUtils.getUpdateFile();//获取要跟新的DDL DML文件
        UpdateFileUtils.moveUpdateFile();//把转换好的hive-all文件中的部分复制到hive-update中

        System.out.println("ddlUpdate:"+UpdateFileUtils.ddlUpdate);
        System.out.println("dmlUpdate:"+UpdateFileUtils.dmlUpdate);


        PatternUtils.printCustomPatternSet("errorCompile:",PatternUtils.errorCompile);
        PatternUtils.printCustomPatternSet("hasGroupInKey:",PatternUtils.hasGroupInKey);
        PatternUtils.printCustomPatternSet("hasGroupInValue:",PatternUtils.hasGroupInValue);

    }

    public static void hive_hive_dmlTransfrom(String sourcePath,String sourceFile,String sourceDir,String targetDmlPath,String targetShellPath) {

        String targetDmlFile = DMLTransform.getTargetDmlFileName(sourceFile);
        List<String> stringList = FileUtils.readFileList(sourcePath + sourceFile);

            DMLTransform.replaceDmlDb(stringList);//替换库名
            DMLTransform.replaceDmlTb(stringList);//替换表名
            FileUtils.writeFileList(targetDmlPath + targetDmlFile, stringList, false);

    }


    public static void dmlTransfrom(String sourcePath,String sourceFile,String sourceDir,String targetDmlPath,String targetShellPath)   {

        String targetDmlFile=DMLTransform.getTargetDmlFileName(sourceFile);
        String sourceDbTable=DMLTransform.getMapDbTable(sourceDir,sourceFile);//sourceDir 代表的是Level0、Level1、Level2，用来计算库名
//        System.out.println(sourceFile+","+sourceDir+","+targetDmlFile+","+sourceDbTable);
        List<String> stringList= FileUtils.readFileList(sourcePath+sourceFile);

            DmlIdx dmlIdx=DmlIdx.getDmlIdx(sourceDbTable,stringList);
            dmlIdx.filePath=sourcePath+sourceFile;

            List<String> subStringList=new ArrayList<>(DMLTransform.getSubList(stringList,dmlIdx));

            DMLTransform.addLine(sourceDbTable,subStringList);//增加配置行
            List<String> commentList=DMLTransform.getCommentList(stringList);
            subStringList.addAll(0,commentList);//增加注释

            DMLTransform.replaceDmlDb(subStringList);//替换库名
            DMLTransform.replaceDmlTb(subStringList);//替换表名
            DMLTransform.replaceDml(sourceDbTable,subStringList);
            DMLTransform.replaceSpecial(subStringList);//处理一些inceptor编程不规范带来的副作用。1.替换中文括号。 2.插入语句漏了分号，commit之后才写分号

            ShellInfo shellInfo=shellTransfrom( stringList, dmlIdx, sourceDbTable, targetDmlFile, targetShellPath);
            if(shellInfo!=null){
                ShellTransform.repalceShellVar(shellInfo,subStringList);//替换成shell变量名。如v_is_trd_dt 替换成${hivevar:v_is_trd_dt}
                ShellTransform.replaceDigitVar(shellInfo,subStringList);//替换编程不规范的变量名(数字开头的)。如1d_later替换成 var_1d_later
            }
            FileUtils.writeFileList(targetDmlPath+targetDmlFile,subStringList,false);

    }


    public static ShellInfo shellTransfrom(List<String> stringList,DmlIdx dmlIdx,String sourceDbTable,String targetDmlFile,String targetShellPath)   {
        ShellInfo shellInfo= ShellTransform.getShellInfo(stringList,dmlIdx);
        if(shellInfo!=null){
            DMLTransform.replaceDmlDb(shellInfo.shellStrings);
            DMLTransform.replaceDmlTb(shellInfo.shellStrings);
            DMLTransform.replaceDml(sourceDbTable,shellInfo.shellStrings);
            ShellTransform.replaceSpecial(shellInfo.shellStrings);
            ShellTransform.getExecuteStr(shellInfo,targetDmlFile);
            ShellTransform.replaceDigitVar(shellInfo,shellInfo.shellStrings);
            FileUtils.writeFileList(targetShellPath+(targetDmlFile.replace(".hql",".sh")),
                    shellInfo.shellStrings,false);
        }

        return shellInfo;
    }

    public static void ddlTransfrom(String sourcePath,String sourceFile,String targetPath){

        List<String> stringList= FileUtils.readFileList(sourcePath+sourceFile);
        int createTableIndex=DDLTransform.getCreateTableIndex(stringList);


        DDLTransform.replaceDdlDb(stringList,createTableIndex);//替换库名，并建立映射
        DDLTransform.replaceDdlTb(stringList,createTableIndex);//替换表名，并建立映射
        DDLTransform.dropDDLTableDrop(stringList); //删除原脚本自带的drop语句
        DDLTransform.addDropTable(stringList);//增加drop table if exists 语句
        DDLTransform.replaceddl(stringList);//替换文字，如字段类型的替换。

        stringList=AddLineUtils.removeSpace(stringList);
        DDLTransform.addLine(stringList);//增加一些字符串。如 row format delimited fields terminated by '\t'

        if(isPrint){
            StringUtils.printList(stringList);
        }
        FileUtils.writeFileList(targetPath+DDLTransform.getMapPath(sourceFile),stringList,false);
    }

}
