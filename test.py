import pyarrow.plasma as plasma
import binascii
import pyarrow as pa
import sys

client = plasma.connect("/tmp/plasma", "", 0)

[buffers] = client.get_buffers([plasma.ObjectID(b"A" * 20)])

data = pa.BufferReader(buffers)

#print(data.read())
batch = pa.RecordBatchStreamReader(data)
