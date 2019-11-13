package core.clean;

import util.Config;

import java.io.File;

public class LocalCleaner {
    public boolean clean() {
        String filePath = Config.getFileDirPath();
        if (filePath == null){
            return true;
        }
        if (filePath.equals("")) {
            return true;
        }

        File file = new File(filePath);

        if (!file.exists())
        {
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
