package org.apache.hadoop.fs.xd;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.attribute.*;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.nio.file.Files;
import java.nio.file.Paths;

public class XdFileSystem extends FileSystem {
    private URI uri;
    private Path workingDir;
    static final URI NAME = URI.create("xidian:///");
    public static final Log LOG = LogFactory.getLog(XdFileSystem.class);
    private static final String Xd_URLPREFIX = "xidian:/";

    public XdFileSystem(){
                this.workingDir = getInitialWorkingDirectory();
            }

    private Path makeAbsolute(Path f) {
        if (f.isAbsolute()) {
            return f;
        } else {
            return new Path(workingDir, f);
        }
    }

    protected Path getInitialWorkingDirectory() {
        return this.makeQualified(new Path(System.getProperty("user.dir")));
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        LOG.info("*** Using Xd file system ***");
        super.initialize(name, conf);

        initURI(name, conf);
        setConf(conf);
    }

    private void initURI(URI name, Configuration conf) {
        if (name.getAuthority() != null) {
            String uriStr = name.getScheme() + "://" + name.getAuthority();
            this.uri = URI.create(uriStr);
        } else {
            this.uri = URI.create(name.getScheme() + ":///");
        }
    }

    @Override
    public void setOwner(Path f, String username, String groupname) throws IOException {
        if (username == null && groupname == null) {
            throw new IOException("username == null && groupname == null");
        }

        java.nio.file.Path absolutePath = Paths.get(f.toUri().getPath());
        FileOwnerAttributeView ownerAttributeView = Files.getFileAttributeView(absolutePath, FileOwnerAttributeView.class);
        if(username != null && groupname != null){
            UserPrincipal user = checkUser(username, f.toUri().getPath());
            UserPrincipal group = checkGroup(groupname, f.toUri().getPath());
            if(user == null || group == null){
                throw new IOException("Unknown user or group.");
            }
            StringTokenizer t = new StringTokenizer(user.getName(), "\\");;
            String userrefix = t.nextToken();
            StringTokenizer t2 = new StringTokenizer(group.getName(), "\\");
            String groupPrefix = t2.nextToken();

            if(userrefix.equals(groupPrefix)){
                ownerAttributeView.setOwner(user);
            }else{
                throw new IOException("Username and group does not match. Maybe it is a bug of nio. We will fix it in the future.");
            }
        }
        if(username == null && groupname != null){
            UserPrincipal group = checkGroup(groupname, f.toUri().getPath());
            if(group == null){
                throw new IOException("Unknown group.");
            }
            ownerAttributeView.setOwner(group);
        }

        if(username != null || groupname == null) {
            UserPrincipal user = checkUser(username, f.toUri().getPath());
            if(user == null) {
                throw new IOException("Unknown user.");
            }
            ownerAttributeView.setOwner(user);
        }


    }

    @Override
    public URI getUri() {
        return NAME;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        if(!exists(f)) {
            throw new FileNotFoundException(f.toString());
        }
        return new FSDataInputStream(new BufferedFSInputStream(
                new XdFSFileInputStream(f), bufferSize
        ));
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission fsPermission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return create(f, overwrite, true, bufferSize, replication, blockSize, progress, fsPermission);
    }

    private FSDataOutputStream create(Path f, boolean overwrite,
                                      boolean createParent, int bufferSize, short replication, long blockSize,
                                      Progressable progress, FsPermission permission) throws IOException {
        if (exists(f) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + f);
        }
        Path parent = f.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent.toString());
        }
        return new FSDataOutputStream(new BufferedOutputStream(
                new XdFSFileOutputStream(f, false, permission), bufferSize),
                statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        return null;
    }

    /*
     * DISABLED METHODS FOR READ-ONLY FILE SYSTEM
     */
    private String notSupportedMsg(String op) throws IOException {
        return op + " is not supported on a xidian File System";
    }
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        File srcFile = pathToFile(src);
        File dstFile = pathToFile(dst);
        if (srcFile.renameTo(dstFile)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean delete(Path p, boolean recursive) throws IOException {
        File f = pathToFile(p);
        if (!f.exists()) {
            //no path, return false "nothing to delete"
            return false;
        }
        // 是文件则直接删除
        if (f.isFile()) {
            return f.delete();
        } else if (!recursive && f.isDirectory() && // 非递归删除且目标是非空目录则不删除，抛出错误
                (FileUtil.listFiles(f).length != 0)) {
            throw new IOException("Directory " + f.toString() + " is not empty");
        }
        // 递归删除
        return FileUtil.fullyDelete(f);
    }

    /** Convert a path to a File. */
    public File pathToFile(Path path) {
        checkPath(path);
        if (!path.isAbsolute()) {
            path = new Path(getWorkingDirectory(), path);
        }
        return new File(path.toUri().getPath());
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        //
        File file = pathToFile(f);
        FileStatus[] results;

        if(!file.exists()){
            throw new FileNotFoundException("File " + f + " does not exist.");
        }

        if(file.isDirectory()){
            String[] names = file.list();
            if(names == null) {
                return null;
            }
            results = new FileStatus[names.length];
            for(int i = 0; i < names.length; i++) {
                results[i] = getFileStatus(new Path(f, names[i]));
            }
            return Arrays.copyOf(results, names.length);
        }else{
            throw new IOException(f + " is not a directory.");
        }
    }

    @Override
    public void setWorkingDirectory(Path path) {
        workingDir = makeAbsolute(path);
        checkPath(workingDir);
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDir;
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return mkdirs(path, null);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        if( path == null ){
            throw new IllegalArgumentException("mkdirs path arg is null");
        }

        Path parent = path.getParent();
        File file = pathToFile(path);
        File parentFile = null;
        // 父目录不存在
        if(parent != null) {
            parentFile = pathToFile(parent);
            if(parentFile != null && !parentFile.isDirectory()) {
                throw new ParentNotDirectoryException("Parent path is not a dictory: " + parent);
            }
        }
        // 目标File已存在且为文件
        if(file.exists() && !file.isDirectory()){
            throw new IOException("Destination exists and it is a file: " + path.toString());
        }

        if(parent != null && !parentFile.exists()){
            if(!mkdirs(parent)) return false;
        }

        if(file.isDirectory()){
            return true;
        }

        return file.mkdir();
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File file = pathToFile(path);
        if(!file.exists()) return null;
        return new XdFileStatus(path, pathToFile(path), getDefaultBlockSize(path), this);
    }

    public static UserPrincipal checkGroup(String name, String windowsPathString){
        java.nio.file.Path f = Paths.get(windowsPathString);
        java.nio.file.FileSystem fileSystem = f.getFileSystem();
        try{
            UserPrincipalLookupService service = fileSystem.getUserPrincipalLookupService();
            UserPrincipal up = service.lookupPrincipalByGroupName(name);
            return up;
        }catch (Exception e){
            return null;
        }
    }

    public static UserPrincipal checkUser(String name, String windowsPathString){
        java.nio.file.Path f = Paths.get(windowsPathString);
        java.nio.file.FileSystem fileSystem = f.getFileSystem();
        try{
            UserPrincipalLookupService service = fileSystem.getUserPrincipalLookupService();
            UserPrincipal up = service.lookupPrincipalByName(name);
            return up;
        }catch (Exception e){
            return null;
        }
    }

    static class XdFileStatus extends FileStatus {
        /* We can add extra fields here. It breaks at least CopyFiles.FilePair().
         * We recognize if the information is already loaded by check if
         * onwer.equals("").
         */
        private boolean isPermissionLoaded() {
            return !super.getOwner().isEmpty();
        }
        private Path path;
        XdFileStatus(Path path, File f, long defaultBlockSize, FileSystem fs) throws IOException {
            super(f.length(), f.isDirectory(), 1, defaultBlockSize,
                    f.lastModified(), new Path(f.getPath()).makeQualified(fs.getUri(),
                            fs.getWorkingDirectory()));
            this.path = path;
            loadPermissionInfo();
        }

        @Override
        public FsPermission getPermission() {
            if (!isPermissionLoaded()) {
                loadPermissionInfo();
            }
            return super.getPermission();
        }

        @Override
        public String getOwner() {
            if (!isPermissionLoaded()) {
                loadPermissionInfo();
            }
            return super.getOwner();
        }

        @Override
        public String getGroup() {
            if (!isPermissionLoaded()) {
                loadPermissionInfo();
            }
            return super.getGroup();
        }

        private void loadPermissionInfo() {
            IOException e = null;
            try {
                String owner = null;
                String group = null;
                java.nio.file.Path absolutePath = Paths.get(path.toUri().getPath());
                FileOwnerAttributeView ownerAttributeView = Files.getFileAttributeView(absolutePath, FileOwnerAttributeView.class);
                UserPrincipal up = ownerAttributeView.getOwner();
                StringTokenizer t = new StringTokenizer(up.getName(), "\\");
//                System.out.println("owner: " + owner.getName());
                group = t.nextToken();
                owner = t.nextToken();
                setOwner(owner);

                if(checkGroup(up.getName(), path.toUri().getPath()) != null){
                    setGroup(up.getName());
                } else {
                    setGroup(group + "\\None");
                }
            } catch (Shell.ExitCodeException ioe) {
                if (ioe.getExitCode() != 1) {
                    e = ioe;
                } else {
                    setPermission(null);
                    setOwner(null);
                    setGroup(null);
                }
            } catch (IOException ioe) {
                setOwner(null);
                setGroup(null);
            } finally {
                if (e != null) {
                    throw new RuntimeException("Error while running command to get " +
                            "file permissions : " +
                            StringUtils.stringifyException(e));
                }
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            if (!isPermissionLoaded()) {
                loadPermissionInfo();
            }
            super.write(out);
        }
    }
}
