
import urllib.request
from subprocess import check_call, CalledProcessError
import os

class WebdavUploader:
    def __init__(self, manifest):
        self.base_uri = manifest.get_config("webdav.base_url")
        self.max_size = int(manifest.get_config_or_default("webdav.max_size", 600*1024*1024))
        self.split_size = int(manifest.get_config_or_default("webdav.split_size", 450*1024*1024))

        self.headers = []
        for config in manifest.db.execute("select param,value from config where param like 'webdav.headers.%'"):
            self.headers.append(config[0][len('webdav.headers.'):] + ': ' + config[1])


    def uploadfile(self, filespec):
        if os.path.getsize(filespec) > self.max_size:
            prefix = filespec+'.split.'
            print('File '+filespec+' too big, splitting in chunks...')
            check_call(['split', '--bytes=' + str(self.split_size), filespec, prefix])
            dir = os.path.dirname(filespec)
            for file in os.listdir(dir):
                print('Uploading chunk '+file)
                if file.startswith(prefix):
                    self.run_curl(os.path.join(dir, file))
        else:
            self.run_curl(filespec)

    def run_curl(self, filespec):
        MAX_RETRY = 5
        for retries in range(5):
            if retries > 0: print("Retry %d of %d" % (retries, MAX_RETRY))
            try:
                check_call(['curl', '--fail', '-X', 'PUT', '--progress-bar'] + [w for header in self.headers for w in ['--header', header]]
                    + ['--data-binary', '@' + filespec, self.base_uri + os.path.basename(filespec)])
                break
            except CalledProcessError as ex:
                print("--------------------")
                print("ERROR: Uploading failed with exit code %d" % (ex.returncode))
                print(ex.output)
                print("--------------------")

