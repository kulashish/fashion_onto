import ftplib
import sys
import csv
from StringIO import StringIO

quotedData = StringIO()

with open(sys.argv[1], 'r') as infile:
        with open(sys.argv[2], 'w') as outfile:
                reader = csv.reader(infile)
                writer = csv.writer(outfile, delimiter=',', quoting=csv.QUOTE_ALL)
                writer.writerows(reader)

#session = ftplib.FTP('server.address.com','USERNAME','PASSWORD')
#file = open(sys.argv[2],'rb')                  # file to send
#session.storbinary(sys.argv[3], file)     # send the file
#file.close()                                    # close file and FTP
#session.quit()

