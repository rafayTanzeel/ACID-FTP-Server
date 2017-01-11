from __future__ import with_statement

import SocketServer
import os
import pickle
import random
import threading
import time
from shutil import copyfile

lockPickle=threading.Lock()


class ThreadedTCPRequestHandler(SocketServer.StreamRequestHandler):

    ERRORS = {201: "Invalid transaction ID", 202: "Invalid operation", 205: "File I/O error", 206: "File not found"}
    headerIndex={"METHOD":0, "TransactionID":1, "MsgSeq":2, "ContentSize":3, "Data":4}
    METHODS = ["READ", "NEW_TXN", "WRITE", "COMMIT", "ABORT", "ACK", "ASK_RESEND", "ERROR"]
    MSGLEN = 4096
    HIDDEN_TXN = ".txnids"

    MSG_SEQ="MsgSeq"
    MSG_SEQ_No="MsgSeqNo"
    FILE_NAME="fileName"
    FILE_CONTENT="fileContent"
    TXN_ID="txnid"
    TXN_COMMIT="commit"
    COMMIT_SEQ="commitSeq"
    TIMER="timer"

    COM_0 = "NotCommitted"
    COM_1 = "SoonCommitted"
    COM_2 = "Committed"


    def setup(self):
        print "\n=============================================="
        print "Connected to Client at %s Port: %s" % self.client_address
        return SocketServer.BaseRequestHandler.setup(self)


    def __hasData(self, method):
        if(method!="WRITE" and method!="NEW_TXN" and method!="READ"):
            return False
        return True


    def __recv_data(self):
        buff=[]
        method=None
        msglen=0
        oneTimeEx=True
        while True:
            ch = self.request.recv(self.MSGLEN)
            buff.append(ch)

            if ('\r' in ch and oneTimeEx):
                lst=''.join(buff).split()
                method=lst[0]
                msglen=int(lst[3])
                oneTimeEx=False
            if not oneTimeEx:
                seq=''.join(buff)
                if self.__hasData(method.upper()):
                    if '\r\n\r\n' in seq:
                        dat=seq.split('\r\n\r\n')[1]

                        # 'ge' because data may have an extra \n at the end
                        if (len(dat)>=msglen):
                            return seq.split('\r\n\r\n')[0] + "\r\n\r\n" + seq.split('\r\n\r\n')[1][:msglen]
                else:
                    if '\r\n\r\n\r\n' in seq:
                        return seq.split('\r\n\r\n\r\n')[0] + "\r\n\r\n\r\n"


    def __time_check(self):
        curTime = time.time()
        removeList = []
        for tid in self.server.pickleData.keys():
            txn = self.server.pickleData[tid]
            expTime = txn[self.TIMER]
            ft = float(expTime)

            if (ft + 600) < curTime:
                print "!!!!!! Transaction with ID: %s is expired !!!!!!"%tid
                removeList.append(tid)
        for rm in removeList:
            self.__remove_trans(rm)


    def __time_update(self, transid):
        curTime = time.time()
        self.server.pickleData[transid][self.TIMER] = curTime

        self.__update_trans_file()

    def __send_res(self, resData):
        print "\n-------------------------------------"

        print "Sent to client"
        print "====> begin message"
        print resData,
        print "====> end message"

        self.request.sendall(resData)


    def handle(self):
        try:
            # Check transaction expiry time
            self.__time_check()

            reqData=self.__recv_data()
            print "Received from client:"
            print reqData,

            resDataList = self.__processReq(reqData)
            resData=' '.join(resDataList)
            seqNos=None

            if resDataList[0]==self.METHODS[0]:
                resData=resDataList[5].strip()
            elif resDataList[0]==self.METHODS[6]:
                seqNos=resDataList[2].split(',')

            # case of ACK
            if resDataList[0] == self.METHODS[5]:

                # the dictionary could be empty after ABORT
                if self.server.pickleData:
                    txn = self.server.pickleData[resDataList[1]]
                    if txn[self.TXN_COMMIT] == self.COM_1:
                        allseq = True
                        for seq in xrange(txn[self.COMMIT_SEQ] + 1):
                            sseq = str(seq)
                            if sseq not in txn[self.MSG_SEQ].keys():
                                allseq = False

                        # ACK and in soon-committed state and has all seq no
                        if allseq:
                            self.__send_res(resData)

                            reqData = "COMMIT " + resDataList[1] + " " + str(txn[self.COMMIT_SEQ]) + " 0\r\n\r\n\r\n"
                            resDataList = self.__processReq(reqData)
                            resData=' '.join(resDataList)

                            self.__send_res(resData)

                        # ACK and in soon-committed state but missing extra seq no
                        else:
                            self.__send_res(resData)

                    # ACK but not in soon-committed state
                    else:
                        self.__send_res(resData)

                # ABORT is done and pickleData is empty
                else:
                    self.__send_res(resData)

            # case of ACK_RESEND
            elif seqNos:
                for seq in seqNos:
                    resDataList[2]=seq

                    resDataList[5] = "Message Seq No {} Missing\n".format(seq)
                    resDataList[4] = str(len(resDataList[5]))
                    resDataList[5] = "\r\n\r\n"+resDataList[5]+'\n'
                    resData=' '.join(resDataList)

                    self.__send_res(resData)

            # case of Etc. methods
            else:
                self.__send_res(resData)


            print "\nTotal Threads Running: ", threading.active_count()


        except Exception as ex:
            print "\nForce Termination Of Client Connection"


    def finish(self):
        print "End Client Connection at %s Port: %s" % self.client_address
        print "=============================================="
        return SocketServer.BaseRequestHandler.finish(self)


    def __parseHeader(self, data):
        # Msg Header Init
        header=data.strip().split()
        # print "HEADER", header

        # Msg Header Fields Init
        METHOD = header[self.headerIndex["METHOD"]].upper()
        TransactionID = header[self.headerIndex["TransactionID"]] if(len(header)>=self.headerIndex["TransactionID"]+1) else None
        MsgSeqNo = header[self.headerIndex["MsgSeq"]] if(len(header)>=self.headerIndex["MsgSeq"]+1) else None
        ContentSize = header[self.headerIndex["ContentSize"]] if(len(header)>=self.headerIndex["ContentSize"]+1) else None


        if len(header)==self.headerIndex["Data"]:
            Data=None
        elif len(header)>self.headerIndex["Data"]+1:
            Data=' '.join(header[self.headerIndex["Data"]:])
        else:
            Data=header[self.headerIndex["Data"]]

        return METHOD, TransactionID, MsgSeqNo, ContentSize, Data


    def __processReq(self, data):

        # Get the header fields
        METHOD, TransactionID, MsgSeqNo, ContentSize, Data = self.__parseHeader(data)
        # METHOD, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason
        if METHOD == "READ":
            METHOD, ErrorCode, ContentSize, Reason = self.__read_res(METHOD, ContentSize, Data)
        elif METHOD == "NEW_TXN":
            METHOD, TransactionID, ErrorCode, ContentSize, Reason = self.__new_txn_res(METHOD, ContentSize, Data)
        elif METHOD == "WRITE":
            METHOD, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason = self.__write_res(METHOD, TransactionID, MsgSeqNo, ContentSize, Data)
        elif METHOD == "COMMIT":
            METHOD, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason = self.__commit_res(METHOD, TransactionID, MsgSeqNo, ContentSize, Data)
        elif METHOD == "ABORT":
            METHOD, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason = self.__abort_res(METHOD, TransactionID, MsgSeqNo, ContentSize, Data)
        else:
            METHOD='ERROR'
            ErrorCode=202
            Reason = self.ERRORS[ErrorCode]
            ContentSize = len(Reason)
            # excpetions unknown method

        if Reason is None:
            Reason="\r\n\r\n\r\n"
        else:
            Reason="\r\n\r\n"+Reason+'\n'

        return [METHOD, str(TransactionID), str(MsgSeqNo), str(ErrorCode), str(ContentSize), Reason]


    def __abort_res(self, method, transid, msg_seq_no, fileSize, data):
        Method, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason = method, transid, msg_seq_no, 0, fileSize, None

        # no such transaction id exist
        if transid not in self.server.pickleData.keys():
            Method='ERROR'
            ErrorCode=201
            Reason = self.ERRORS[ErrorCode] + ' - Transaction ID doesn\'t exist'
            ContentSize = len(Reason)
        # transaction already commited
        elif self.server.pickleData[transid][self.TXN_COMMIT] == self.COM_2:
            Method='ERROR'
            ErrorCode=202
            Reason = "Transaction already commited"
            ContentSize = len(Reason)
        else:
            Method='ACK'
            ErrorCode=0
            ContentSize=0

            self.__remove_trans(transid)

        return Method, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason


    def __commit_res(self, method, transid, msg_seq_no, fileSize, data):
        Method, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason = method, transid, msg_seq_no, 0, fileSize, None

        # no such transaction id exist
        if transid not in self.server.pickleData.keys():
            Method='ERROR'
            ErrorCode=201
            Reason = self.ERRORS[ErrorCode] + ' - Transaction ID doesn\'t exist'
            ContentSize = len(Reason)
        # invalid msg_seq_no
        elif int(msg_seq_no) < 0:
            Method='ERROR'
            ErrorCode=202
            Reason = self.ERRORS[ErrorCode] + ' - Invalid message sequence number'
            ContentSize = len(Reason)
        # transaction already commited
        elif self.server.pickleData[transid][self.TXN_COMMIT] == self.COM_2:
            Method='ACK'
            ErrorCode=0
            Reason = "Transaction already committed"
            ContentSize = len(Reason)
        else:
            MsgSeqNos = self.server.pickleData[transid][self.MSG_SEQ].keys()
            IntMsgSeqNos = map(int, MsgSeqNos)
            IntMsgSeqNos.sort()
            MsgLastSeq = IntMsgSeqNos[-1]
            missingSeq = True


            MsgSeqNo=self.__getMissingMsgSeq(IntMsgSeqNos, int(msg_seq_no))

            # no missing msg_seq_no
            if not MsgSeqNo:
                missingSeq=False
                MsgSeqNo=msg_seq_no
            else:
                MsgSeqNo=','.join(map(str, MsgSeqNo))

            if  MsgLastSeq>int(msg_seq_no):
                Method='ERROR'
                ErrorCode=202
                MsgSeqNo=MsgLastSeq
                Reason = "Message Seq No is smaller than the total seq in transaction"
                ContentSize = len(Reason)
            elif missingSeq:
                Method='ASK_RESEND'
                ErrorCode=0
                Reason = "Message Seq No {} Missing".format(MsgSeqNo)
                ContentSize = len(Reason)

                self.server.pickleData[transid][self.TXN_COMMIT] = self.COM_1
                self.server.pickleData[transid][self.COMMIT_SEQ] = int(msg_seq_no)
                self.__update_trans_file()
            else:
                Method='ACK'
                dataToCommit = self.__sumData(transid, int(msg_seq_no))

                filename=self.server.pickleData[transid][self.FILE_NAME]
                filepath=self.server._server_dir+os.path.sep+filename

                filetemppath=self.server._server_dir+os.path.sep+'.'+os.path.splitext(filename)[0]+str(threading._get_ident())+'.bak'
                if os.path.isfile(filepath):
                    copyfile(filepath, filetemppath)

                ContentSize=len(dataToCommit)

                # Create file if not exist
                try:
                    with open(filetemppath, 'a+') as f:
                        f.write(dataToCommit)
                        f.flush()
                        os.fsync(f.fileno())
                except IOError:
                    METHOD='ERROR'
                    ErrorCode=205
                    Reason = self.ERRORS[ErrorCode]
                    ContentSize = len(Reason)

                try:
                    os.rename(filetemppath, filepath)
                except Exception:
                    os.remove(filepath)
                    os.rename(filetemppath, filepath)
                    # move(filetemppath, filepath)

                self.server.pickleData[transid][self.TXN_COMMIT] = self.COM_2
                self.__update_trans_file()

        return Method, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason

    def __sumData(self, transid, msg_seq_no):
        dataToCommit = ""
        msgDict = self.server.pickleData[transid][self.MSG_SEQ]

        for i in xrange(msg_seq_no+1):
            dataToCommit += msgDict[str(i)]

        return dataToCommit

    def __getMissingMsgSeq(self, MsgSeqNos, msg_seq_no):

        missingNo=[]

        for i in xrange(msg_seq_no+1):
            if i not in MsgSeqNos:
                missingNo.append(i)

        return missingNo


    def __write_res(self, method, transid, msg_seq_no, fileSize, data):
        Method, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason = method, transid, msg_seq_no, 0, fileSize, None

        # no such transaction id exist
        if transid not in self.server.pickleData.keys():
            Method='ERROR'
            ErrorCode=201
            Reason = self.ERRORS[ErrorCode] + ' - Transaction ID doesn\'t exist'
            ContentSize = len(Reason)
        # transaction already commited
        elif self.server.pickleData[transid][self.TXN_COMMIT] == self.COM_2:
            Method='ERROR'
            ErrorCode=202
            Reason = "Transaction already commited"
            ContentSize = len(Reason)

        elif msg_seq_no in self.server.pickleData[transid][self.MSG_SEQ].keys(): #if seq no already exist for this transid
            Method='ERROR'
            ErrorCode=202
            Reason = self.ERRORS[ErrorCode] + ' - Message Sequence No of this transaction already exists'
            ContentSize = len(Reason)
        else:
            Method='ACK'
            msg_seq_data = {MsgSeqNo : data}
            self.server.pickleData[transid][self.MSG_SEQ].update(msg_seq_data)

            self.__time_update(str(TransactionID))

        return Method, TransactionID, MsgSeqNo, ErrorCode, ContentSize, Reason


    def __new_txn_res(self, method, fileSize, filename):
        Method, TransactionID, ErrorCode, ContentSize, Reason = method, None, 0, fileSize, filename
        if(len(filename) != int(fileSize)):
            Method='ERROR'
            ErrorCode=202
            Reason = "Filename not the same length as the content size sent"
            ContentSize = len(Reason)
        else:
            TransactionID=self.__get_new_txnid(filename)
            Method='ACK'

        return Method, TransactionID, ErrorCode, ContentSize, Reason

    def __rand_num(self):
        rand = random.randint(0, 99999)
        ids = self.server.pickleData.keys()

        while rand in ids:
            rand = random.randint(0, 99999)

        return rand

    def __get_new_txnid(self, filename):
        newId = self.__rand_num()
        seq1 = {str(0) : ''}

        self.server.pickleData.update(
            {   str(newId) : {
                    self.TXN_COMMIT : self.COM_0,
                    self.COMMIT_SEQ : 0,
                    self.MSG_SEQ : seq1,
                    self.FILE_NAME : filename,
                    self.TIMER : time.time()
                }
            }
        )

        # Update pickle transaction file
        self.__update_trans_file()

        # ret new id
        return newId


    def __remove_trans(self, transid):
        self.server.pickleData.pop(transid, None)
        self.__update_trans_file()


    def __update_trans_file(self):
        with lockPickle:
            with open(self.server._server_dir+os.path.sep+self.HIDDEN_TXN, 'wb') as fw:
                    pickle.dump(self.server.pickleData, fw)

    def __read_res(self, method, fileNameLen, filename):

        METHOD, ErrorCode, Reason, ContentSize = method, 0, None, None
        filepath=self.server._server_dir+os.path.sep+filename

        if not os.path.isfile(filepath):
            METHOD='ERROR'
            ErrorCode=206
            Reason = self.ERRORS[ErrorCode]
            ContentSize = len(Reason)
        else:
            try:
                with open(filepath, 'rb') as f:
                    Reason=f.read()
                    ContentSize=len(Reason)
            except IOError:
                METHOD='ERROR'
                ErrorCode=205
                Reason = self.ERRORS[ErrorCode]
                ContentSize = len(Reason)

        return METHOD, ErrorCode, ContentSize, Reason


class Server(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    # Ctrl C kills all spawned threads and also kills all threads when main thread terminates
    daemon_threads = True
    # Faster rebinding
    allow_reuse_address = True
    request_queue_size = 5
    timeout = 5

    def __init__(self, server_addr, server_dir, RequestHandlerClass):
        self._server_dir=server_dir
        pickleData=None
        with open(server_dir+os.path.sep+".txnids","rb") as fr:
            pickleData=pickle.load(fr)

        self.pickleData=pickleData
        RequestHandlerClass.server_dir=server_dir
        SocketServer.TCPServer.__init__(self, server_addr, RequestHandlerClass)


    def server_activate(self):
        ip, port=self.server_address
        print "Server listening on {} Port {} with internal storage dir: {}".format(ip, port, self._server_dir)
        SocketServer.TCPServer.server_activate(self)
        return


    # def close_request(self, request_address):
    #     return SocketServer.TCPServer.close_request(self, request_address)
