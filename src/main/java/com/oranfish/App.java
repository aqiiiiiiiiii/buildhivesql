package com.oranfish;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class App {

    public static final int ENV_MODEL = 1;
    public static final int ENV_DT_MODEL = 2;
    public static String LINE_SEPARATOR = System.getProperty("line.separator");

    public static void main( String[] args ) {
        convert("/Volumes/Data/sql转换/so_return_item.sql", "ads.so_return_item_inc", ENV_DT_MODEL);
    }

    public static void convert(String url, String hiveTableName, int partitionModel){
        try {
            String result = null;
            String selectFields = null;
            BufferedReader reader = new BufferedReader(new FileReader(url));
            StringBuilder sb = new StringBuilder();
            StringBuilder selectBuilder = new StringBuilder();
            sb.append("DROP TABLE IF EXISTS ");
            sb.append(hiveTableName);
            sb.append(";");
            sb.append(LINE_SEPARATOR);
            sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
            sb.append(hiveTableName);
            sb.append("(");
            sb.append(LINE_SEPARATOR);
            selectBuilder.append("columns#");
            String line = reader.readLine();
            if(line != null){
                while((line = reader.readLine()) != null){
                    String[] elements = line.split(" ");
                    if(elements.length <4){
                        break;
                    }
                    String field = elements[2];
                    String type = elements[3];
                    if(!field.startsWith("`")){
                        break;
                    }
                    if(field.startsWith("`del_")){
                        continue;
                    }
//                    String comment = null;
//                    int commentIndex = 0;
//                    for(String element : elements){
//                        commentIndex ++;
//                        if(element.equalsIgnoreCase("COMMENT")){
//                            break;
//                        }
//                    }
//                    if(commentIndex != 0){
//                        comment = elements[commentIndex];
//                    }
                    if(type.indexOf("decimal") == -1){
                        if(type.indexOf("bigint") != -1){
                            type = "bigint";
                        }else if(type.indexOf("int") != -1){
                            type = "int";
                        }else{
                            type = "string";
                        }
                    }
                    StringBuilder lineBuilder = new StringBuilder();
                    lineBuilder.append(field);
                    lineBuilder.append(" ");
                    lineBuilder.append(type);
//                    if(comment != null){
//                        lineBuilder.append(" COMMENT ");
//                        lineBuilder.append(comment);
//                    }
                    lineBuilder.append(",");
                    lineBuilder.append(LINE_SEPARATOR);
                    sb.append(lineBuilder.toString());
                    selectBuilder.append(field.replaceAll("`", ""));
                    selectBuilder.append(",");
                }
                result = sb.toString();
                selectFields = selectBuilder.toString();
                result = result.substring(0, result.lastIndexOf(",")) + result.substring(result.lastIndexOf(",")+1, result.length());
                selectFields = selectFields.substring(0, selectFields.lastIndexOf(",")) + selectFields.substring(selectFields.lastIndexOf(",")+1, selectFields.length());
                if(partitionModel == ENV_MODEL){
                    result = result + ")partitioned by (env string)" + LINE_SEPARATOR;
                }else if(partitionModel == ENV_DT_MODEL){
                    result = result + ")PARTITIONED BY (env string,dt string)" + LINE_SEPARATOR;
                }
                result = result + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' LINES TERMINATED BY '\\n' STORED AS textfile;";
            }
            System.out.println(LINE_SEPARATOR);
            System.out.println(result);
            System.out.println(LINE_SEPARATOR);
            System.out.println(selectFields);
            System.out.println(LINE_SEPARATOR);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e2){
            e2.printStackTrace();
        }
    }
}
