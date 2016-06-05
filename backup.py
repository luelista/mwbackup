#!/usr/bin/env python3

import aescrypt
import argparse
import sys, os, stat
import tempfile
from subprocess import call, check_call, Popen, PIPE
import time
import csv

from webdavuploader import WebdavUploader
from database import ManifestDb

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger()
fh = logging.FileHandler('backup_warnings.log')
fh.setLevel(logging.WARN)
log.addHandler(fh)

def cmd_exclude():
    if arg.F:
        manifest.db.execute("DELETE FROM excludes ")
        manifest.db.commit()
        print("Flushed")
    if arg.a:
        manifest.db.executemany("INSERT INTO excludes (exclude) VALUES (?)", [ (x,) for x in arg.a ])
        manifest.db.commit()
    if arg.l:
        for fn in manifest.get_ignores():
            print(fn)

def addfile(filespec, backup_id):
    st = os.stat(filespec, follow_symlinks=False)
    if stat.S_ISREG(st.st_mode):
        hash = ""#aescrypt.md5_file(filespec)
    else:
        hash = ""
    print(hash,filespec,end="\t")
    c = manifest.db.execute("SELECT rowid FROM files WHERE filespec = ? AND mtime = ? AND st_size = ?", (filespec, st.st_mtime, st.st_size))
    if c.rowcount > 0:
        id = c.fetchone()[0]
        print("Unchanged",id)
    else:
        id = manifest.db.execute("INSERT INTO files (filespec, archive_id, hash, st_size, atime, mtime, ctime, st_mode, st_uid, st_gid) "+
                " VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                (filespec, hash, st.st_size, st.st_atime, st.st_mtime, st.st_ctime, st.st_mode, st.st_uid, st.st_gid)
                ).lastrowid
        print("New/Updated",id)
    manifest.db.execute("INSERT INTO backup_files (backup_id,file_id) VALUES (?,?)", (backup_id, id))

def cmd_createbackup():
    if arg.c:
        ignores = manifest.get_ignores()
        backup_id = manifest.db.execute("insert into backups (ctime,folder) values (?,?)", (int(time.time()), backup_dir,)).lastrowid
        for dirpath,dirnames,filenames in os.walk(backup_dir):
            addfile(dirpath, backup_id)
            dirnames[:] = [d for d in dirnames if d not in ignores]
            for file in filenames:
                if file in ignores: continue
                filespec = os.path.join(dirpath, file)
                try:
                  addfile(filespec, backup_id)
                except FileNotFoundError as ex:
                  log.warning("ERR with file: %s", filespec)
                  log.exception("%s", str(ex))
        manifest.db.commit()
    if arg.l:
        for b in manifest.db.execute("select rowid,* from backups").fetchall():
            print(b[0], b[1], b[2])
    if arg.s:
        c = manifest.db.execute("select rowid,* from backups where rowid=?", (arg.s,))
        backup = c.fetchone()
        for (col, val) in zip(c.description, backup):
            print(col[0], val)
        for file in manifest.db.execute("select files.rowid file_rowid,archive_id,filespec,hash,st_size,st_mode from files inner join backup_files ON file_rowid=file_id WHERE backup_id = ? ORDER BY filespec ", (arg.s,)):
            print(file)

def getfiles(backup_id, max_files, max_size):
    acc_size = 0
    files_count = 0
    for file in manifest.db.execute("SELECT files.rowid file_rowid,st_mode,filespec,st_size FROM files INNER JOIN backup_files ON file_rowid=file_id WHERE backup_id = ? AND archive_id IS NULL ORDER BY filespec LIMIT ?", (backup_id, max_files,)):
        regfile = False
        if stat.S_ISREG(file[1]):
            acc_size += file[3]
            files_count += 1
            regfile = True
        if acc_size > max_size and files_count > 1: break
        if arg.v >= 3: print('Yielding file ','#'+str(file[0]),file[2])
        yield { 'file_rowid': file[0], 'filespec': file[2], 'regular': regfile }


def getfilesize(backup_id, uploaded):
    if uploaded:
        cond = " NOT ( archive_id IS NULL ) "
    else:
        cond = " (archive_id IS NULL) "
    size = manifest.db.execute("""SELECT files.rowid file_rowid,SUM(st_size) FROM files
        INNER JOIN backup_files ON file_rowid=file_id
        WHERE backup_id = ? AND """ + cond, (backup_id,)).fetchone()[1]
    if not size: return 0
    return size


def makearchive(backup_id, uploader, enc_password, max_size):
    print("Progress: %0.02f MB up, %0.02f MB to go" % (getfilesize(backup_id, True)/(1024*1024), getfilesize(backup_id, False)/(1024*1024)))
    atime = int(time.time())
    aformatteddate = time.strftime('%Y-%m-%d-%H%M%S')
    aname = '%s.tar.gz.aes' % (aformatteddate)
    archive_id = manifest.db.execute("INSERT INTO archives (filename,ctime,backup_id) VALUES(?,?,?)", (aname, atime, backup_id,)).lastrowid
    if arg.v >= 2: print("Compiling file list "+aname, end=" "); sys.stdout.flush()

    strip_len = len(backup_dir)
    if strip_len != "/": strip_len += 1
    with tempfile.TemporaryDirectory() as dir, \
            open('/tmp/backup.log', 'a') as log:
        files_count = 0
        apathprefix = os.path.join(dir, aformatteddate)
        with open(apathprefix+'.lst', 'wb') as tmp, open(apathprefix+'.csv', 'w') as csvfile:
            csvout = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            for file in getfiles(backup_id, 2500, max_size):
                if arg.v >= 2: print (".", end=""); sys.stdout.flush()
                tmp.write(bytes(file['filespec'][strip_len:], 'UTF-8'))
                tmp.write(bytes([0]))
                hash = ""
                if file['regular']: hash = aescrypt.md5_file(file['filespec'])
                manifest.db.execute('UPDATE files SET archive_id=?,hash=? WHERE rowid=?',
                    (archive_id, hash, file['file_rowid'],))
                csvout.writerow([hash, file['filespec'], archive_id, aname])
                files_count += 1
        if arg.v >= 2: print ("\n",files_count," files")
        if files_count == 0: return False

        if arg.v >= 2: print("Encrypting file list")
        with open(apathprefix+'.csv', 'rb') as filelist, open(apathprefix+'.csv.aes', 'wb') as encfilelist:
            aescrypt.encrypt(filelist, encfilelist, enc_password)
        if arg.v >= 2: print("Uploading "+apathprefix+'.csv.aes'+"...")
        uploader.uploadfile(apathprefix+'.csv.aes')
        if arg.v >= 2: print("OK")

        if arg.v >= 2: print("Building archive from file list ",apathprefix+'.lst')

        apath = os.path.join(dir, aname)
        with open(apath, 'wb') as archive_file:
            tar = Popen(['tar', '--null', '-czf', '-', '-C', backup_dir, '-T', tmp.name, '--no-recursion'], stdout=PIPE, stderr=log)
            aescrypt.encrypt(tar.stdout, archive_file, enc_password)
        if arg.v >= 2: print("Uploading "+apath+"...")
        uploader.uploadfile(apath)

        hash = aescrypt.md5_file(apath)
        manifest.db.execute("UPDATE archives set hash=? where rowid=?", (hash,archive_id,))
        manifest.db.commit()
        if arg.v >= 2: print("OK")
        return True

def cmd_createarchives():
    uploader = WebdavUploader(manifest)
    enc_password = manifest.get_config("encryption.password")
    max_size = int(manifest.get_config_or_default("archive.max_source_size", 450*1024*1024))
    while makearchive(arg.b, uploader, enc_password, max_size):
        pass

def cmd_config():
    if arg.param_name:
        if arg.value:
            manifest.set_config(arg.param_name, arg.value)
        else:
            print(manifest.get_config(arg.param_name))
    else:
        for opt in manifest.db.execute("select param,value from config order by param"):
            print(opt[0], '=', opt[1])


parser = argparse.ArgumentParser(description='Backup stuff.')
parser.add_argument('-M', '--manifest', metavar='FILE', type=str,
                   help='name of the manifest file')
parser.add_argument('-v', action='count', default=0, help='Verbose')
subparsers = parser.add_subparsers(help='sub-command help')

parser_excludes = subparsers.add_parser('exclude')
parser_excludes.add_argument('-l', action='store_true', help='List excludes')
parser_excludes.add_argument('-a', action='append', type=str, help='Add an exclude')
parser_excludes.add_argument('-F', action='store_true', help='Flush excludes (clear list)')
parser_excludes.set_defaults(func=cmd_exclude)

parser_backup = subparsers.add_parser('backup')
parser_backup.add_argument('-c', action='store_true', help='Create new')
parser_backup.add_argument('-l', action='store_true', help='List')
parser_backup.add_argument('-s', type=int, help='Show files')
parser_backup.set_defaults(func=cmd_createbackup)

parser_archive = subparsers.add_parser('archive')
parser_archive.add_argument('-b', required=True, help='Backup id')
parser_archive.set_defaults(func=cmd_createarchives)

parser_config = subparsers.add_parser('config')
parser_config.add_argument('param_name', nargs='?', help='Config parameter name')
parser_config.add_argument('value', nargs='?', help='Set new config parameter value')
parser_config.set_defaults(func=cmd_config)

arg = parser.parse_args(sys.argv[1:])

manifest_file = os.path.join(os.curdir, '.backup_manifest')
if arg.manifest: manifest_file = arg.manifest

manifest = ManifestDb(manifest_file)
backup_dir = manifest.get_config_or_default("source", os.path.dirname(manifest_file))

if 'func' in arg: arg.func()
