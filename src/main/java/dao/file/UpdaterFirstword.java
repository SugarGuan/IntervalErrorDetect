package dao.file;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class UpdaterFirstword {
    public static void main (String[] args) throws IOException {
        File file = new File("D:\\Project\\2020\\Offset_Errcode.ini");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        BufferedReader reader = new BufferedReader(new FileReader(file));
        List<String> list = new ArrayList<>();
//        list.add();
        String str;
        while (null != (str = reader.readLine()))
            list.add(str.toUpperCase());


        for (String strInput: list) {
            writer.newLine();
            writer.write(strInput);
        }
    }
}
