import optparse
import os
import pickle
import sys

import server

optparser = optparse.OptionParser()
optparser.add_option("-a", "--address", dest="address", default="localhost", help="Server Address (default=localhost)")
optparser.add_option("-p", "--port", dest="port", default=7896, help="Server Port Number (default=7896)")
optparser.add_option("-d", "--directory", dest="directory", default=os.getcwd()+os.path.sep+"Server_Src", help="Server Directory to keep file created by user")
opts = optparser.parse_args()[0]


def main(argv):
    # ASSIGNING GLOBAL VARIABLE APPROPRIATE VALUES
    HOST, PORT, SERVER_DIR = arg_init(argv)

    # SERVER CONFIGURATION
    TCP_Server = server.Server((HOST, PORT), SERVER_DIR, server.ThreadedTCPRequestHandler)
    # TCP_Server.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    try:
        TCP_Server.serve_forever()
    except KeyboardInterrupt:
        # Ctrl-C to terminate
        TCP_Server.shutdown()
        TCP_Server.server_close()

        # Deleting useless files
        if os.path.isfile(SERVER_DIR+os.path.sep+'.txnids'):
            os.remove(SERVER_DIR+os.path.sep+'.txnids')

        filelist = [ f for f in os.listdir(SERVER_DIR) if f.endswith(".bak") ]
        for f in filelist:
            os.remove(SERVER_DIR+os.path.sep+f)

        sys.exit(0)



#####################################################################################
# INITIALIZATION OF GLOBAL VARIABLE FROM CMD AND CREATE DIR FOR SERVER DATA STORAGE #
#####################################################################################
def arg_init(argv):
    # GLOBAL VARIABLE
    HOST, PORT, SERVER_DIR = opts.address, int(opts.port), opts.directory

    # Create Directory for server storage if not exist
    if not os.path.exists(SERVER_DIR):
        os.makedirs(SERVER_DIR)

    if not os.path.isfile(SERVER_DIR+os.path.sep+'.txnids'):
        with open(SERVER_DIR+os.path.sep+'.txnids', 'wb') as f:
            transData={}
            pickle.dump(transData, f)

    return HOST, PORT, SERVER_DIR


if __name__ == "__main__":
    main(sys.argv)

