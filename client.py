

import codec_pb2
import requests
import time
import hashlib
import os
url = "http://127.0.0.1:8000/"
BLOCKSIZE = 1024 * 1024 * 4 # 4M
def send(path, name):
    start = time.time()
    
    filesize = os.path.getsize(path) 

    fileinfo = codec_pb2.FileInfo()
    fileinfo.BlockSize = BLOCKSIZE 
    fileinfo.BlockNum = filesize / fileinfo.BlockSize
    if filesize % fileinfo.BlockSize:
        fileinfo.BlockNum += 1

    fileinfo.name = name
    fileinfo.id = hashlib.md5( path.encode("utf8")).hexdigest()

    requests.post(url + "FileInfo", data = fileinfo.SerializeToString())

    index = 0
    with open(path, "rb") as f:
        while index < filesize:
            block = codec_pb2.Block()
            block.fileid = fileinfo.id
            if index + fileinfo.BlockSize < filesize:
                block.Seq = index // fileinfo.BlockSize
                block.Data = f.read(BLOCKSIZE) 
                block.Size = fileinfo.BlockSize
                index += fileinfo.BlockSize
            else:
                block.Seq = index // fileinfo.BlockSize
                block.Data = f.read() 
                block.Size = filesize - index
                index += block.Size

            requests.post(url + "Block" , data = block.SerializeToString())

    print( "Spend %f seconds" % (time.time() - start))
if __name__ == "__main__":

    send("ready.mkv", "hello.mkv")
