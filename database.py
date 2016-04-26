
import sqlite3

class ManifestDb:
    def create_schema(self):
        print("Initializing/Updating manifest file")
        self.db.execute("""create table excludes (
            exclude text
            );""")
        self.db.execute("""create table config (
            param text, value text
            );""")
        self.db.execute("""create table files (
            filespec text,archive_id int,hash text,st_size int,atime int,mtime int,ctime int,st_mode int,st_uid int,st_gid int
            );""")
        self.db.execute("""create table backups (
            ctime int, folder text
            );""")
        self.db.execute("""create table backup_files (
            backup_id int, file_id int
            );""")
        self.db.execute("""create table archives (
            filename text, ctime int, backup_id int, hash text
            );""")
        self.db.execute("create index i1 ON files (filespec);")
        self.db.execute("create index i2 ON backup_files (file_id);")
        self.db.execute("create unique index i3 ON backup_files (backup_id,file_id);")
        self.db.execute("PRAGMA user_version=1;")

    def __init__(self, file):
        print("Opening manifest %s" % file)
        self.db = sqlite3.connect(file)
        version = self.db.execute('PRAGMA user_version;').fetchone()[0]
        if version < 1: self.create_schema()

    def get_config_or_default(self, param_name, def_value):
        result = self.db.execute("SELECT value FROM config WHERE param = ?", (param_name,)).fetchone()
        if result == None: return def_value
        return result[0]

    def get_config(self, param_name):
        result = self.db.execute("SELECT value FROM config WHERE param = ?", (param_name,)).fetchone()
        if result == None: raise Exception('Missing configuration option '+param_name)
        return result[0]

    def set_config(self, param_name, value):
        self.db.execute("DELETE FROM config WHERE param=?", (param_name,))
        self.db.execute("INSERT INTO config (param,value) values (?,?)", (param_name,value))
        self.db.commit()
        
    def get_ignores(self, ):
        return [row[0] for row in self.db.execute("SELECT * FROM excludes").fetchall()]

