Options:
  -h, --help            show this help message and exit
  -a ADDRESS, --address=ADDRESS
                        Server Address (default=localhost)
  -p PORT, --port=PORT  Server Port Number (default=7896)
  -d DIRECTORY, --directory=DIRECTORY
                        Server Directory to keep file created by user

						

E.G RUN:

python source.py -p 7896 -a localhost -d ./Server_Src


To Exit Gracefully, kill the server with Ctrl+C


Commands:

To send a NEW_TXN message, type:
	 new_txn
To send a WRITE message, type:
	 write <txn_num> <seq_num> <content_length> <data>
For example:
	 write 1 1 2 45
The program will generate some content for you
To send a COMMIT message, type:
	 commit <txn_num> <seq_num> <content_length> <data>
For example:
	 commit 1 1 1 0
To send an ABORT message, type:
	 abort <txn_num> <seq_num> <content_length> <data>
For example:
	 abort 1 1 1 0
To send a READ message, type:
	 read <any txn id> <any non-neg seq_num> <any content_length> <file_name>
For example:
	 read 1 1 1 file.txt
