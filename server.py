import tornado
import tornado.web
import codec_pb2
import os
FileInfos = {}

Blocks = {}

if not os.path.exists("temp"):
    os.mkdir("temp")

def check( id ):
    if not id in FileInfos:
        return False
    if not id in Blocks:
        return False 
    fileinfo = FileInfos[id]

    for i in range(fileinfo.BlockNum):
        if not i in Blocks[id]:
            return False
    return True

def writeToFile( id ):
    fileinfo = FileInfos[id]
    
    with open(fileinfo.name, "wb") as f:
        for i in range(fileinfo.BlockNum):
            blockpath = Blocks[id][i]
            with open(blockpath, "rb") as bf:
                block = codec_pb2.Block()
                block.ParseFromString(bf.read())
                f.write(block.Data[:block.Size])
            if os.path.exists(blockpath):
                os.remove(blockpath)
    del FileInfos[id]
    del Blocks[id]

class FileInfoHandle(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    def post(self):
        print( "Recv FileInfo")
        fileinfo = codec_pb2.FileInfo()
        fileinfo.ParseFromString(self.request.body)
        
        if fileinfo.IsInitialized():
            self.set_status(200)
            FileInfos[fileinfo.id] = fileinfo
        else:
            self.set_status(403)

        self.finish()


class BlockHandle(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        print( "Recv Block")
        block = codec_pb2.Block()
        block.ParseFromString(self.request.body)

        if block.IsInitialized():
            self.set_status(200)
            self.finish()
            print (block.Seq)
            if not block.fileid in Blocks:
                Blocks[block.fileid] = {}
            Blocks[block.fileid][block.Seq] = "temp/" + block.fileid + str(block.Seq)
            with open(Blocks[block.fileid][block.Seq], "wb") as f:
                f.write(block.SerializeToString())

            if check(block.fileid):
                writeToFile(block.fileid)

        else:
            self.set_status(403) 
            self.finish()


if __name__ == "__main__":
    app = tornado.web.Application({
        (r"/FileInfo", FileInfoHandle),
        (r"/Block", BlockHandle)
    })

    app.listen(8000)
    tornado.ioloop.IOLoop.current().start()


