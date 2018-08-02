

import codec_pb2
import requests
import time
import hashlib
url = "http://127.0.0.1:8000/"
def send(path, name):
    start = time.time()
    with open(path, "rb") as f:
        byteData = f.read()
        filesize = len(byteData)

        fileinfo = codec_pb2.FileInfo()
        fileinfo.BlockSize = 1024 * 1024 * 4
        fileinfo.BlockNum = len(byteData) // fileinfo.BlockSize
        if filesize % fileinfo.BlockSize:
            fileinfo.BlockNum += 1

        fileinfo.name = name
        fileinfo.id = hashlib.md5( path.encode("utf8")).hexdigest()
        requests.post(url + "FileInfo", data = fileinfo.SerializeToString())

        index = 0
        while index < filesize:
            block = codec_pb2.Block()
            block.fileid = fileinfo.id
            if index + fileinfo.BlockSize < filesize:
                block.Seq = index // fileinfo.BlockSize
                block.Data = byteData[index : index + fileinfo.BlockSize]
                block.Size = fileinfo.BlockSize
                index += fileinfo.BlockSize
            else:
                block.Seq = index // fileinfo.BlockSize
                block.Data = byteData[index :]
                block.Size = filesize - index
                index += block.Size

            requests.post(url + "Block" , data = block.SerializeToString())

    print( "Spend %f seconds" % (time.time() - start))
if __name__ == "__main__":

    send("../test.pdf", "hello.pdf")
