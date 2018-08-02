import tornado
import tornado.web
import codec_pb2

FileInfos = {}

Blocks = {}
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
            block = Blocks[id][i]
            f.write(block.Data[:block.Size])

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
            if not block.fileid in Blocks:
                Blocks[block.fileid] = {}
            Blocks[block.fileid][block.Seq] = block

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


