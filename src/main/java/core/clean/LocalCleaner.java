package core.clean;

import util.Config;

import java.io.File;

public class LocalCleaner {
    public boolean clean() {
        String filePath = Config.getFileDirPath();
        if (filePath == null){
            System.out.println("File Path Null");
            return true;
        }
        if (filePath.equals("")) {
            System.out.println("Filepath empty");
            return true;
        }

        File file = new File(filePath);

        if (!file.exists())
        {
            System.out.println("File does not exist.");
            return true;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            boolean result = true;
            for (File f :files) {
                result = ( result && f.delete() );
            }
            return result;
        }
        else {
            return file.delete();
        }
    }
}
